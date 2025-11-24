import pandas as pd
import os
from sqlalchemy import create_engine, inspect
import re
from datetime import datetime
from multiprocessing import Pool, cpu_count,Manager
import psycopg2
from pyhive import presto
import csv
import log_module
import sys
import time
from DB_conns import *


def init_worker(shared_event):
    global event
    event = shared_event

def status_up(desc):
    apt_tool_Db().execute(f"update APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND set request_status='E',request_desc='{desc}' where request_id={sys.argv[1]}")
    apt_tool_Db().close()

def main(args):
    client, trt_tb, qr, presto_config, pg_config,sup_l,n,deciles_,indx_val,indx_creation=args
    try:
        if event.is_set():
           return
        if deciles_=='True':
            de = qr.split(',')[4].strip().split(' ')[0]
            if not re.findall('where', qr):
                qr = f"{qr} where {de}='{client}'"
            else:
                qr = f"{qr} and {de}='{client}'"
        dstart_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"RLTP Data pulling started for decile {client} at : {dstart_time}")
        pconn= presto.connect(**presto_config)
        pcur=pconn.cursor()
        pcur.execute("SET SESSION query_max_run_time = '3h'")
        pcur.execute("SET SESSION query_max_execution_time = '3h'")
        pconn.commit()
        session_settings = pcur.fetchall()
        pcur.execute("SHOW SESSION LIKE 'query_max_execution_time%'")
        session_settings = pcur.fetchall()
        logger.info(f"Session settings:{session_settings}")
        logger.info(f"Execution query ::{qr}")
        pcur.execute(qr)
        #print(client)
        d_file = f'{path}/FILES/decile_{client}.csv'
        try:
            with open(d_file, 'w', newline='') as f:
                writer = csv.writer(f, delimiter='|')
                while True:
                    rows = pcur.fetchmany(size=3000000)
                    if not rows:
                        break
                    writer.writerows(rows)
            f.close()
        except Exception as e:
            logger.error(f"Unable to pull RLTP data for {client}: {e}", exc_info=True)
            logger.info(f"Retrying to pull RLTP data for {client} after 3 seconds")
            time.sleep(3)
            while n<=2:
                try:
                    os.remove(d_file)
                    n+=1
                    main(client, trt_tb, qr, presto_config, pg_config,sup_l,n,deciles_)
                    break
                except Exception as e:
                    status_up(f"Unable to pull data from RLTP{n}")

            status_up("Unable to pull data from RLTP")
            event.set()
            return
            sys.exit(1)

        # Create a new PostgreSQL connection and cursor for this process
        with psycopg2.connect(**pg_config) as pg_conn:
            pg_conn.autocommit = True
            with pg_conn.cursor() as cursor:
                with open(d_file, 'r') as f:
                    dtb=f"{trt_tb}_{client}".lower()
                    cursor.copy_expert(f"COPY {dtb} FROM STDIN WITH DELIMITER '|' CSV", f)
                    pg_conn.commit()
                    if indx_val==indx_creation:
                        cursor.execute(f"""SELECT indexname FROM pg_indexes WHERE tablename ='{dtb}'  AND indexname ='{dtb}_email_idx'""")
                        result = cursor.fetchone()
                        if not result:
                            try:
                                logger.info(f"Adding index on email level on {dtb}")
                                cursor.execute(f"create index {dtb}_email_idx on  {dtb} (email) ")
                            except Exception as e:
                                logger.info("Unable to create index on table")
                                status_up("Unable to create index on TRT")
                                event.set()
                                return
                                sys.exit(1)
                        if sup_l=='True':
                            cursor.execute(f"""SELECT indexname FROM pg_indexes WHERE tablename ='{dtb}'  AND indexname ='{dtb}_md5_idx'""")
                            mresult = cursor.fetchone()
                            if not mresult:
                                try:
                                    logger.info(f"Adding index on md5 level on {dtb}")
                                    cursor.execute(f"create index {dtb}_md5_idx on  {dtb} (md5hash) ")
                                except Exception as e:
                                    logger.info("Unable to create Md5 index on table")
                                    status_up("Unable to create md5 index on TRT")
                                    event.set()
                                    return
                                    sys.exit(1)

        os.remove(d_file)
        logger.info(f"{d_file} is removed")
        dend_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time1 = datetime.strptime(dstart_time, "%Y-%m-%d %H:%M:%S")
        time2 = datetime.strptime(dend_time, "%Y-%m-%d %H:%M:%S")
        total_ex = time2 - time1
        logger.info(f" Execution time for decile {client} : {total_ex}")
        pconn.close()
        pg_conn.close()
    except Exception as e:
        logger.error(f"Error processing {client}: {e}", exc_info=True)
        status_up('Unable to pull data from RLTP')
        event.set()
        return
        sys.exit(1)



if __name__ == "__main__":
    try:
        global logger,path,df
        n=1
        request_id=sys.argv[1]
        path='/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/'+request_id
        lpath=path+"/LOGS"
        logger = log_module.setup_logging(lpath)
        logger.info("Logs path: {}".format(lpath))
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("RLTP data pulling started at : " + start_time)
        # PostgreSQL connection config
        pg_config = {
            'dbname': "apt_tool_db",
            'user': "datateam",
            'host': "zds-prod-pgdb01-01.bo3.e-dialog.com"
        }
        # Presto connection config
        presto_config = {
            'host': 'zdl3-mn03.bo3.e-dialog.com',
            'port': 8081,
            'username': 'zx_tenant',
            'catalog': 'hive',
            'schema': 'zx_tenant_database',
        }

        # Create a base PostgreSQL connection for initial setup
        with psycopg2.connect(**pg_config) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                engine = create_engine('postgresql+psycopg2://datateam:@zds-prod-pgdb01-01.bo3.e-dialog.com/apt_tool_db')
                df = pd.read_sql("SELECT a.request_id,a.week,a.query,a.decile_wise_report_path,b.client_name,a.SUPP_PATH FROM APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND a,apt_custom_client_info_table_dnd b  WHERE request_id="+request_id+" and a.client_id=b.client_id", con=engine)
                trt_tb=("apt_custom_"+str(df['request_id'][0])+"_"+df['client_name'][0]+"_"+df['week'][0]+"_trt_table").lower()
                df3 = pd.read_csv(df['decile_wise_report_path'][0], sep='|', header=None, thousands=',')
                df3.columns = ['Delivered', 'Opens', 'clicks', 'unsubs', 'segment', 'sub_seg', 'decile','old_per']
                dcnt = len(df3['decile'].drop_duplicates())
                client = ''
                sup_l='True'
                sup_p=df['supp_path'][0]
                if not df['supp_path'][0]=='':
                    sup=pd.read_csv(sup_p,nrows=10,header=None)
                    if not sup[0].str.contains('@').any():
                        logger.info("Adding index on md5 level")
                        sup_l='True'
                # Create a base Presto connection for sampling
                econn= presto.connect(**presto_config)
                qry=df['query'][0].split(';')
                qry= [item for item in qry if re.search(r'apt_rltp_request_raw_', item)]
                indx_creation=len(qry)
                sqr = qry[0] + ' LIMIT 3'
                try:
                    samp = pd.read_sql(sqr, con=econn)
                except Exception as e:
                    logger.error("Unable to pull sample data from presto ::{}".format(e))
                    status_up("Unable to pull data from RLTP")
                    sys.exit(1)
                logger.info(samp)
                engine.dispose()
                econn.close()
                std_cols=['md5hash','email','segment','subseg','decile','priority']
                req_cols=['md5hash','email','segment','subseg','decile','priority']+list(samp.columns)[6::]
                colsn = ' varchar,'.join(req_cols) + ' varchar'
                inspector = inspect(engine)
                try:
                    if not inspector.has_table(trt_tb):
                        cursor.execute(f"CREATE TABLE {trt_tb} ({colsn}) PARTITION BY LIST(decile)")
                        for i in list(df3['decile'].drop_duplicates()):
                            cursor.execute(f"CREATE TABLE {trt_tb}_{i} PARTITION OF {trt_tb} FOR VALUES IN ('{i}')")
                        logger.info('Tables created')
                except Exception as e:
                    logger.error("Unable to create TRT tables")
                    status_up("Unable to create TRT tables")
                    sys.exit(1)
        indx_val=1
        for qr in qry:
            dec=str(qr.split(",")[4].strip().split()[0])
            deciles_='False'
            #decilel=list(df3['decile'].drop_duplicates())
            decilel=sorted(list(df3['decile'].drop_duplicates()),reverse=True)
            if decilel[0]==1 or dec =="'1'":
                decilel=[1]
            if dcnt != 1 and dec !="'1'":
                deciles_='True'
                decilel=sorted(list(df3['decile'].drop_duplicates()),reverse=True)
            logger.info('Executing single decile function')
            try:
                # Use multiprocessing with a pool of workers
                #pool_size = min(cpu_count(), dcnt)
                manager= Manager()
                shared_event=manager.Event()
                with Pool(processes=5,initializer=init_worker,initargs=(shared_event,)) as pool:
                    pool.map(main, [(client, trt_tb, qr, presto_config, pg_config,sup_l,n,deciles_,indx_val,indx_creation) for client in decilel])
                indx_val=indx_val+1
                logger.info("All threads for TRT are completed.")
            except Exception as e:
                logger.error("An error occurred: %s", str(e), exc_info=True)
                status_up("Failed to Launch Worker nodes for TRT")
                sys.exit(1)
            time.sleep(2)
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time1 = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        time2 = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        total_ex = time2 - time1
        pg1con=psycopg2.connect(**pg_config)
        pg1con.autocommit = True
        p2cur=pg1con.cursor()
        p2cur.execute(f"update APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND set request_desc='TRT Imported' where request_status<>'E' and request_id={sys.argv[1]}")
        p2cur.execute(f"select count(email) from {trt_tb}")
        cnt=list(p2cur.fetchone())[0]
        p2cur.execute(f" update apt_custom_postback_qa_table_dnd set RLTP_FILE_COUNT={cnt} where request_id={sys.argv[1]}")
        pg1con.close()
        logger.info(f"RLTP data pulling ended at : {total_ex}")
    except Exception as e:
        logger.error("Unable to pull data from RLTP ::{}".format(e))
        status_up("Unable to pull data from RLTP")
        pool.terminate()
        pool.join()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()
