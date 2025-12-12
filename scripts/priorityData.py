#!/usr/bin/env python3
import io
import os
import sys
import re
import traceback
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from snowflake.connector.pandas_tools import write_pandas

sys.path.append('/u1/techteam/PFM_CUSTOM_SCRIPTS/PYTHON_MODULES')
from DbConns1 import *

PG_CONFIG = {'dbname': "apt_tool_db", 'user': "datateam", 'host': "zds-prod-pgdb01-01.bo3.e-dialog.com"}
PG_CONN_STR = 'postgresql+psycopg2://datateam:@zds-prod-pgdb01-01.bo3.e-dialog.com/apt_tool_db'


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def read_headerless_md5_file(path):
    try:
        df = pd.read_csv(path, header=None, dtype=str, names=['md5hash'], low_memory=False)
        df['md5hash'] = df['md5hash'].astype(str).str.strip().str.lower()
        df = df.drop_duplicates(subset=['md5hash']).reset_index(drop=True)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed reading MD5 file '{path}': {e}")


def df_to_pg_copy(df, table_name, PG_CONFIG):
    try:
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=True)
        buf.seek(0)
        with psycopg2.connect(**PG_CONFIG) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name};")
                cur.execute(f"CREATE TABLE {table_name} (md5hash TEXT);")
                cur.copy_expert(f"COPY {table_name}(md5hash) FROM STDIN WITH (FORMAT csv, HEADER TRUE);", buf, )
    except Exception as e:
        raise RuntimeError(f"Postgres COPY failed for table {table_name}: {e}")


def insert_df_to_snowflake(df, sf_conn, sf_cur, fq_table):
    try:
        df2 = df[['md5hash']].astype(str)
        ok, nchunks, nrows, _ = write_pandas(
            sf_conn, df2, table_name=fq_table, quote_identifiers=False
        )
        if not ok:
            raise Exception("write_pandas returned False")
        log(f"write_pandas loaded {nrows} rows into {fq_table} in {nchunks} chunks")
    except Exception as e:
        raise RuntimeError(f"Snowflake write_pandas failed for {fq_table}: {e}")


# ==================================================== #
# Main
# ==================================================== #
def main():
    try:
        if len(sys.argv) < 2:
            raise SystemExit("Usage: script.py <REQUEST_ID>")

        REQUEST_ID = sys.argv[1]
        engine = create_engine(PG_CONN_STR)
        log(f"Starting request_id={REQUEST_ID}")

        try:
            CLIENT_NAME = pd.read_sql(
                f"""SELECT upper(client_name) AS client_name FROM APT_CUSTOM_CLIENT_INFO_TABLE_DND a JOIN APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND b ON a.client_id=b.client_id WHERE b.request_id={REQUEST_ID} """,
                con=engine)['client_name'].iat[0]
        except Exception as e:
            raise RuntimeError(f"Error fetching client_name: {e}")
        PRIORITY_TABLE = f"APT_CUSTOM_{REQUEST_ID}_{CLIENT_NAME}_PRIORITY_DATA"
        PG_TABLE = PRIORITY_TABLE
        SF_TABLE = PRIORITY_TABLE
        log("PRIORITY_TABLE: " + SF_TABLE)
        rec = pd.read_sql(
            f"SELECT priority_file, query FROM APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND WHERE request_id={REQUEST_ID}",
            con=engine)
        priority_file = rec['priority_file'].iat[0]
        raw_query = rec['query'].iat[0]
        if not os.path.exists(priority_file):
            raise RuntimeError(f"priority_file does not exist: {priority_file}")
        df = read_headerless_md5_file(priority_file)
        unique_count = len(df)
        if unique_count == 0:
            raise RuntimeError("MD5 file has 0 valid rows. Aborting.")
        log(f"Unique md5 count = {unique_count}")
        df_to_pg_copy(df, PG_TABLE, PG_CONFIG)
        log(f"Loaded {unique_count} rows into Postgres table {PG_TABLE}")
        try:
            sf_conn, sf_cur = getSnowflakeDT()
        except:
            sf_conn, sf_cur = getSnowflake()

        try:
            sf_cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
            cur_db, cur_schema, cur_role = sf_cur.fetchone()
        except Exception as e:
            raise RuntimeError(f"Snowflake state check failed: {e}")
        fq_table = f"{cur_db}.DT_DATA.{SF_TABLE}"

        try:
            sf_cur.execute(f"DROP TABLE IF EXISTS {fq_table}")
            sf_cur.execute(f"CREATE TABLE {fq_table} (md5hash VARCHAR)")
        except Exception:
            fq_table = f"{cur_db}.{cur_schema}.{SF_TABLE}"
            sf_cur.execute(f"DROP TABLE IF EXISTS {fq_table}")
            sf_cur.execute(f"CREATE TABLE {fq_table} (md5hash VARCHAR)")
        log("Created Snowflake table: " + fq_table)
        insert_df_to_snowflake(df, sf_conn, sf_cur, fq_table)
        log(f"Loaded {unique_count} rows into Snowflake table {fq_table}")

        try:
            query_list = [q.strip() for q in raw_query.strip().split(";") if q.strip()]
            wrapped_queries = []
            for q in query_list:
                if "LEFT JOIN" in q and SF_TABLE in q:
                    wrapped_queries.append(q)
                else:
                    wrapped_queries.append(
                        f"""SELECT inner_q.*,CASE WHEN p.md5hash IS NOT NULL THEN 1 ELSE inner_q.priority END AS priority FROM ({q}) inner_q
                        LEFT JOIN {fq_table} p ON inner_q.md5hash = p.md5hash """)
            cleaned_queries = [" ".join(w.split()) for w in wrapped_queries]
            final_query = "; ".join(cleaned_queries) + ";"
        except Exception as e:
            raise RuntimeError(f"Query wrapping failed: {e}")
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    """UPDATE APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND SET query = :new_query WHERE request_id = :rid"""),
                             {"new_query": final_query, "rid": REQUEST_ID})
        except Exception as e:
            raise RuntimeError(f"Failed updating query in PG: {e}")
        log("Updated final wrapped query")
        extra = pd.read_sql(
            f"""SELECT li_flag, decile_wise_report_path, priority_file_per FROM APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND WHERE request_id={REQUEST_ID}""",
            con=engine)
        LI_flag = extra['li_flag'].iat[0]
        decile_path = extra['decile_wise_report_path'].iat[0]
        priority_pct = float(extra['priority_file_per'].iat[0])
        if LI_flag == 'Y':
            LI_unique_record_count = int(unique_count * (priority_pct / 100))
        elif LI_flag == 'N':
            df_dec = pd.read_csv(decile_path, header=None, sep='|', dtype=str)
            total = df_dec.iloc[:, 0].astype(float).sum()
            LI_unique_record_count = int(total * (priority_pct / 100))
        else:
            raise RuntimeError("Invalid li_flag — must be Y or N")
        log(f"Computed LI_unique_record_count = {LI_unique_record_count}")
        try:
            sf_cur.execute(f"ALTER TABLE {fq_table} SET COMMENT = 'LI_unique_record_count={LI_unique_record_count}'")
        except Exception as e:
            raise RuntimeError(f"Failed setting table comment: {e}")
        log("Stored LI_unique_record_count in Snowflake metadata")

    except Exception as e:
        log("ERROR OCCURRED")
        log(str(e))
        traceback.print_exc()
        sys.exit(1)

    finally:
        try:
            sf_cur.close()
        except:
            pass
        try:
            sf_conn.close()
        except:
            pass
        log("Completed successfully.")


if __name__ == "__main__":
    main()
