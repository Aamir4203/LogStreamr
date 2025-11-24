import psycopg2
from pyhive import presto


def apt_tool_Db():
    pg_config = {
            'dbname': "apt_tool_db",
            'user': "datateam",
            'host': "zds-prod-pgdb01-01.bo3.e-dialog.com"
        }
    pg1con=psycopg2.connect(**pg_config)
    pg1con.autocommit = True
    p2cur=pg1con.cursor()
    return p2cur



def presto1():
    presto_config = {
            'host': 'zdl3-mn03.bo3.e-dialog.com',
            'port': 8081,
            'username': 'zx_tenant',
            'catalog': 'hive',
            'schema': 'zx_tenant_database',
        }
    pconn= presto.connect(**presto_config)
    pcur=pconn.cursor()

    return pcur

def attribution_db():
    pg_db3={
            'dbname': "attribution_db",
            'user': "datateam",
            'host': "zds-prod-pgdb03-01.bo3.e-dialog.com"
            }

    pconn= psycopg2.connect(**pg_db3)
    pcur=pconn.cursor()
    return pcur

