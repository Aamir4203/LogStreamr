import pandas as pd
from  sqlalchemy import *
import psycopg2
import sys


pg_config = {
            'dbname': "apt_tool_db",
            'user': "datateam",
            'host': "zds-prod-pgdb01-01.bo3.e-dialog.com"
        }

conn=psycopg2.connect(**pg_config)
conn.autocommit = True
cursor=conn.cursor()

def main(table_name,rpo,engine):
    dec=pd.read_sql_query(f"select count(email) Del,count(open_date) opens,count(click_date) clicks,count(unsub_date) unsubs,segment,subseg,decile from {table_name}  group by segment,subseg,decile ;",con=engine)
    rpo=rpo.sort_values(by=['segment','sub_seg','decile']).reset_index(drop=True)
    rpo['Tech_opens']=dec['opens']
    rpo['opens_diff'],rpo['Cpm_per'],rpo['Tech_per']=rpo['Opens']-rpo['Tech_opens'],(rpo['Opens']/rpo['Delivered'])*100,(rpo['Tech_opens']/rpo['Delivered'])*100
    rpo['per_diff']=rpo['Cpm_per']-rpo['Tech_per']
    ng=rpo[rpo['per_diff']==rpo['per_diff'].min()].reset_index(drop=True)
    gt=rpo[(rpo['segment']==ng['segment'][0]) & (rpo['sub_seg']==ng['sub_seg'][0])].reset_index(drop=True)
    gt=gt[gt['per_diff']==gt['per_diff'].max()].reset_index(drop=True)
    if round(abs(ng['per_diff'][0]))>1 or round(abs(gt['per_diff'][0]))>1:
        req_per=(abs(ng['per_diff'][0]))/abs(ng['per_diff'][0])*100
        req_cnt=int(abs(req_per*abs(ng['opens_diff'][0])/100))
        if req_cnt>gt['opens_diff'][0]:
            req_cnt=gt['opens_diff'][0]
        print(rpo)
        stats_up(req_cnt,ng,gt,engine,table_name,cpm_rpt_path)
    else:
        print("execution completed")
        print(rpo)
        cursor.close()
        engine.dispose()
        conn.close()
        sys.exit()

def stats_up(cnt,ng,gt,engine,table_name,cpm_rpt_path):
    stats=pd.read_sql_query(f'''select count(email), del_date,subject,creative,segment,offerid,subseg from {table_name} where open_date is not null and decile='{ng['decile'][0]}' and segment='{ng['segment'][0]}' and subseg='{ng['sub_seg'][0]}' and  del_date=open_date and unsub_date is null and click_date is null group by 2,3,4,5,6,7 order by 2,3,4,5,6,7''',con=engine)
    stats['subject']=stats['subject'].str.replace("\'","''")
    stats['counts_up']=(stats['count']*(abs(cnt)/stats['count'].sum())).round().astype('int')
    print("updating opens")
    for index,i in stats.iterrows():
        query = f"""SELECT COUNT(id) FROM {table_name} WHERE decile = %s AND open_date IS NULL AND del_date = %s AND subject = %s AND creative = %s AND offerid = %s AND segment = %s AND flag IS NULL AND subseg = %s"""
        params = ( gt['decile'][0],i['del_date'],i['subject'], i['creative'], i['offerid'], i['segment'], i['subseg'])
        available_cnt=pd.read_sql_query(query, con=engine, params=params)
        if available_cnt['count'][0]!=0:
            up_cnt=i['counts_up']
            if up_cnt>available_cnt['count'][0]:
               up_cnt =available_cnt['count'][0]
            cursor.execute(f"update {table_name} set open_date=del_date where id in ( select id from {table_name} where decile='{gt['decile'][0]}' and open_date is null and del_date='{i['del_date']}' and subject='{i['subject']}' and creative='{i['creative']}' and offerid='{i['offerid']}' and segment='{i['segment']}' and flag is null and subseg='{i['subseg']}' order by status desc,random() limit {up_cnt})")
            cursor.execute(f"update {table_name} set open_date=null where id in ( select id from {table_name} where decile='{ng['decile'][0]}' and open_date is not null and click_date is null and unsub_date is null and del_date='{i['del_date']}' and subject='{i['subject']}' and creative='{i['creative']}' and offerid='{i['offerid']}' and segment='{i['segment']}' and subseg='{i['subseg']}' order by status,random() limit {up_cnt})")


if __name__=="__main__":
    request_id=sys.argv[1]
    engine=create_engine('postgresql+psycopg2://datateam:@zds-prod-pgdb01-01.bo3.e-dialog.com/apt_tool_db')
    tb_info = pd.read_sql("SELECT a.request_id,a.week,a.query,a.decile_wise_report_path,b.client_name,a.SUPP_PATH FROM APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND a,apt_custom_client_info_table_dnd b  WHERE request_id="+request_id+" and a.client_id=b.client_id", con=engine)
    cpm_rpt_path=tb_info['decile_wise_report_path'][0]
    rpo=pd.read_csv(cpm_rpt_path,sep='|',header=None,names=['Delivered','Opens','clicks','unsubs','segment','sub_seg','decile','old_delivered_per'],dtype={'decile':'str'})
    table_name=f"APT_CUSTOM_{tb_info['request_id'][0]}_{tb_info['client_name'][0]}_{tb_info['week'][0]}_POSTBACK_TABLE"
    if rpo['decile'].drop_duplicates().count()>1:
        for i in range(0,rpo['Delivered'].count()+5):
            main(table_name,rpo,engine)
    cursor.close()
