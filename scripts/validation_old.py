import pandas as pd
import os
from  sqlalchemy import *
import sys
sys.path.append('/u1/techteam/PFM_CUSTOM_SCRIPTS/PYTHON_MODULES')
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyhive import presto
import re
import psycopg2
from DbConns import *
import warnings
warnings.filterwarnings("ignore", category=UserWarning)






validation_level=[]
validation_status=[]

#,dtype={'Delivered':int,'Clicks':int,'Unique Opens':int,'Unsubs':int,'sb':int,'hb':int}
def is_valid_date(date_str):
    try:
        pd.to_datetime(date_str, format='%Y-%m-%d', errors='raise')
        return True
    except ValueError:
        return False


def validate_dtype(x):
    return isinstance(x, (int,float))

def style_cell(value):
    if value == 'Failed':
        color = 'background-color: #EC7063; font-weight: bold'
    elif value == 'Pass':
        color='background-color:#82E0AA'
    else:
         color=''
    return color
def style_first_row(row):
    return 'font-weight: bold' if row !='' else ''

def sendEmailNotification(html_table,client_name):
        sender_email = 'attributionalerts@zds-db3-02.bo3.e-dialog.com'
        receiver_email = 'vmarni@zetaglobal.com,datateam@aptroid.com'
        subject = 'APT REQUEST VALIDATION :: '+client_name
        # Create a multipart message
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        html_content = f'''
        <html>
        <body>
        <p>Hi Team,</p><br>
        <p>Please find the below details.</p><br>
        {html_table}
        <p><b>Regards,<br>DataTeam</b></p>
        </body>
        </html>
        '''
        # Attach the message to the MIMEMultipart object
        msg.attach(MIMEText(html_content, 'html'))
        server = smtplib.SMTP('localhost')
        try:
        # Send email
            server.sendmail(sender_email,receiver_email.split(','), msg.as_string())
            print("Mail sent successfully ....!")
        except Exception as e:
            print(e)


####cpm report validatons#########


try:
    presto_config = {
            'host': 'zdl3-mn03.bo3.e-dialog.com',
            'port': 8081,
            'username': 'zx_tenant',
            'catalog': 'hive',
            'schema': 'zx_tenant_database',
        }


    pg_config = {
            'dbname': "apt_tool_db",
            'user': "datateam",
            'host': "zds-prod-pgdb01-01.bo3.e-dialog.com"
        }
    engine=create_engine('postgresql+psycopg2://datateam:@zds-prod-pgdb01-01.bo3.e-dialog.com/apt_tool_db')
    conn=psycopg2.connect(**pg_config)
    conn.autocommit = True
    cur=engine.connect()
    cur=conn.cursor()
    cur.execute("update APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND set request_validation='V' where request_id="+sys.argv[1]+"")
    df=pd.read_sql("select * from APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND where request_id="+sys.argv[1]+"",con=engine)
    df6=pd.read_sql("select * from APT_CUSTOM_CLIENT_INFO_TABLE_DND where client_id="+str(df['client_id'][0])+"",con=engine)
    validation_level.append('Request ID')
    validation_status.append(sys.argv[1])
    if os.path.isfile(df['cpm_report_path'][0]):
        df2=pd.read_csv(df['cpm_report_path'][0],sep='|',header=None,thousands=',')
        if len(df2.columns)==14:
            df2.columns=['Campaign','Date','Delivered','Unique Opens','Clicks','Unsubs','sb','hb','Subject Line','Creative','Creative ID','Offer ID','segment','sub_seg']
            df2 = df2.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            if not df2['Subject Line'].str.contains("''").any():
                df2['Subject Line']=df2['Subject Line'].str.replace("\'","''")
            if not df2['Date'].apply(is_valid_date).all():
                validation_level.append('Report Date Format')
                validation_status.append('Failed')
            else:
                #print('All dates are valid.')
                validation_level.append('Report Date Format')
                validation_status.append('Pass')

            if not df2['Delivered'].apply(validate_dtype).all() & df2['Unique Opens'].apply(validate_dtype).all() & df2['Clicks'].apply(validate_dtype).all() & df2['Unsubs'].apply(validate_dtype).all() & df2['sb'].apply(validate_dtype).all() &df2['hb'].apply(validate_dtype).all() :
                #print('At least one delivered count is not valid.')
                validation_level.append('Report Number Format')
                validation_status.append('Failed')
            else:
                #print('All Delivered_Count are valid.')
                df2['Delivered']=df2['Delivered'].astype(int)
                df2['Unique Opens']=df2['Unique Opens'].astype(int)
                df2['Clicks']=df2['Clicks'].astype(int)
                df2['Unsubs']=df2['Unsubs'].astype(int)
                df2['sb']=df2['sb'].astype(int)
                df2['hb']=df2['hb'].astype(int)
                validation_level.append('Report Number Format')
                validation_status.append('Pass')

            columns_to_count = ['Date','segment','sub_seg','Subject Line', 'Creative','Offer ID']  # List of columns to count
            column_counts = df2[columns_to_count].apply(tuple, axis=1).value_counts()
            column_counts = column_counts.reset_index()
            column_counts.columns = ['Row', 'Count']
            if (column_counts['Count']>=2).any():
                print(column_counts)
                validation_level.append('Duplicate Rows')
                validation_status.append('Failed')
        else:
            #print("file columns doesnot exits")
            validation_level.append('Report Column Count')
            validation_status.append('Failed')
    else:
        #print("CPM file doesnot exits")
        validation_level.append('CPM Report')
        validation_status.append('Failed')



#===== SUPPRESSION FILE TEST CASE ===#

    if df['supp_path'].str.contains('/').all():
        if os.path.isfile(df['supp_path'][0]):
            #print('Suppression file exists')
            validation_level.append('Suppression File')
            validation_status.append('Pass')
        else:
            #print('Suppression file does not exists')
            validation_level.append('Suppression File')
            validation_status.append('Failed')

#====== TIME STAMP TEST CASE ===#

    if df['timestamp_append'][0]=='Y':
        if df['timestamp_report_path'].str.contains('/').all():
            if os.path.isfile(df['timestamp_report_path'][0]):
                validation_level.append('Timestamp Report File')
                validation_status.append('Pass')
                df4=pd.read_csv(df['timestamp_report_path'][0],sep='|',header=0)
                if ((pd.to_datetime(df4.iloc[:, 0]) == pd.to_datetime(df4.iloc[:, 1]).dt.date) & (pd.to_datetime(df4.iloc[:, 1]).dt.date == pd.to_datetime(df4.iloc[:, 2]).dt.date)).all():
                    #print('Time staps dates are matching')
                    validation_level.append('Timestamp Report Dates')
                    validation_status.append('Pass')
                else:
                    #print('Time staps dates are not matching')
                    validation_level.append('Timestamp Report Dates')
                    validation_status.append('Failed')
            else:
                #print('Time stamp file doesnot exists')
                validation_level.append('Timestamp Report File')
                validation_status.append('Failed')


#===================================================================================================================================#


    if os.path.isfile(df['decile_wise_report_path'][0]):
        df3=pd.read_csv(df['decile_wise_report_path'][0],sep='|',header=None,thousands=',')
        if len(df3.columns)==8:
            df3.columns=['Delivered','Opens','clicks','unsubs','segment','sub_seg','decile','old_delivered_per']
            if os.path.isfile(df['cpm_report_path'][0]):
        ###need to check with verizon
                try:
                    if (df2['segment'].drop_duplicates().sort_values().reset_index(drop =True)==df3['segment'].drop_duplicates().sort_values().reset_index(drop =True)).all() and (df2['sub_seg'].drop_duplicates().sort_values().reset_index(drop =True)==df3['sub_seg'].drop_duplicates().sort_values().reset_index(drop =True)).all():
                        #print('True')
                        validation_level.append('Segments Matching')
                        validation_status.append('Pass')
                    else:
                        #print('False')
                        validation_level.append('Segments Matching')
                        validation_status.append('Failed')
                    #=======OLD================#
                    if df3['old_delivered_per'].drop_duplicates().sort_values().reset_index(drop =True).all()>0 and df3['old_delivered_per'].drop_duplicates().sort_values().reset_index(drop =True).notna().all():
                        validation_level.append('OLD_DELIVERED_PER')
                        validation_status.append('Pass')
                    else:
                        validation_level.append('old_delivered_per')
                        validation_status.append('Failed')
                except Exception as e:
                    #print('False')
                    validation_level.append("Segments Matching")
                    validation_status.append('Failed')



#=== RESIDUAL DATE TEST CASE ===#

                if (pd.to_datetime(df['residual_date'])>=pd.to_datetime(df2['Date'].max())).any():
                    #print('residual_date is valid')
                    validation_level.append('Residual Date')
                    validation_status.append('Pass')
                else:
                    #print('residual_date not valid')
                    validation_level.append('Residual Date')
                    validation_status.append('Failed')

            if df3['Delivered'].apply(validate_dtype).all() & df3['Opens'].apply(validate_dtype).all() & df3['clicks'].apply(validate_dtype).all() & df3['unsubs'].apply(validate_dtype).all():
                #print('All valid decile')
                validation_level.append("Decile Report Number Format")
                validation_status.append('Pass')
            else:
                #print('not valid decile')
                validation_level.append("Decile Report Number Format")
                validation_status.append('Failed')
            #=== CPM REPORT - DECILE COUNTS VALIDATION ====#


            #print("Entering into testcase")
            cpm_report_result=df2.groupby(['segment', 'sub_seg'])['Delivered'].sum().rename_axis(index=['segment', 'subseg']).reset_index().sort_values(by=['segment', 'subseg'])
            #print("Test2")
            del_report_result=df3.groupby(['segment', 'sub_seg'])['Delivered'].sum().rename_axis(index=['segment', 'subseg']).reset_index().sort_values(by=['segment', 'subseg'])
            #print("Test3")
            comparison=pd.merge(cpm_report_result,del_report_result,on=['segment', 'subseg'],how='outer',suffixes=('_cpm', '_decile'))
            comparison['difference'] = comparison['Delivered_cpm'] - comparison['Delivered_decile']
            mismatches = comparison[comparison['difference'] != 0]
            #print("checking mismatch")
            if len(mismatches)== 0:
                validation_level.append('CPM AND DECILE REPORT COMPARISON')
                validation_status.append('Pass')
            else:
                validation_level.append('CPM AND DECILE REPORT COMPARISON')
                validation_status.append('Failed')
        else:
            validation_level.append('Decile Report Column Count')
            validation_status.append('Failed')
    else:
        #print('decile wise columns are mismatched')
        validation_level.append('Decile File')
        validation_status.append('Failed')


#==== CLIENT INFORMATION TEST CASE ===#


    validation_level.insert(1,'Client ID')
    validation_status.insert(1,df6['client_id'][0])
    validation_level.insert(2,'Client Name')
    validation_status.insert(2,df6['client_name'][0])
    inspector = inspect(engine)
    if inspector.has_table(df6['prev_week_pb_table'][0].lower()):
        #print('Table exists')
        validation_level.append(df6['prev_week_pb_table'][0])
        validation_status.append('Pass')
    else:
        #print("Table doesn't exits")
        validation_level.append(df6['prev_week_pb_table'][0])
        validation_status.append('Failed')
    if inspector.has_table(df6['total_delivered_table'][0].lower()):
        #print('Table exists')
        validation_level.append(df6['total_delivered_table'][0])
        validation_status.append('Pass')
    else:
        #print("Table doesn't exits")
        validation_level.append(df6['total_delivered_table'][0])
        validation_status.append('Failed')
    if inspector.has_table(df6['posted_unsub_hards_table'][0].lower()):
        #print('Table exists')
        validation_level.append(df6['posted_unsub_hards_table'][0])
        validation_status.append('Pass')
    else:
        #print("Table doesn't exits")
        validation_level.append(df6['posted_unsub_hards_table'][0])
        validation_status.append('Failed')


#=================Unique decile ==========================
    if df['type'][0]==2:
        if os.path.isfile(df['unique_decile_report_path'][0]):
            df4=pd.read_csv(df['unique_decile_report_path'][0],sep='|',header=None,thousands=',')
            if len(df4.columns)==8:
                df4.columns=['Delivered','Opens','clicks','unsubs','segment','sub_seg','decile','old_delivered_per']
                try:
                    if (df2['segment'].drop_duplicates().sort_values().reset_index(drop =True)==df4['segment'].drop_duplicates().sort_values().reset_index(drop =True)).all() and (df2['sub_seg'].drop_duplicates().sort_values().reset_index(drop =True)==df4['sub_seg'].drop_duplicates().sort_values().reset_index(drop =True)).all():
                        #print('True')
                        validation_level.append('Unique decile Segments Matching')
                        validation_status.append('Pass')
                    else:
                        #print('False')
                        validation_level.append('Unique decile Segments Matching')
                        validation_status.append('Failed')
                    if df4['old_delivered_per'].drop_duplicates().sort_values().reset_index(drop =True).all()>0 and df4['old_delivered_per'].drop_duplicates().sort_values().reset_index(drop =True).notna().all():
                        validation_level.append('old_delivered_per')
                        validation_status.append('Pass')
                    else:
                        validation_level.append('old_delivered_per')
                        validation_status.append('Failed')
                except Exception as e:
                    #print('False')
                    validation_level.append("Unique decile Segments Matching")
                    validation_status.append('Failed')


                if df4['Delivered'].apply(validate_dtype).all() & df4['Opens'].apply(validate_dtype).all() & df4['clicks'].apply(validate_dtype).all() & df4['unsubs'].apply(validate_dtype).all():
                    #print('All valid decile')
                    validation_level.append("Unique Decile Report Number Format")
                    validation_status.append('Pass')
                else:
                    #print('not valid decile')
                    validation_level.append("Unique Decile Report Number Format")
                    validation_status.append('Failed')

            else:
                validation_level.append('Unique decile file columns count')
                validation_status.append('Failed')
        else:
            validation_level.append('Unique decile file')
            validation_status.append('Failed')

#######queries validation
    
    qry=df['query'][0].split(';')
    col_l = qry[0] + ' LIMIT 3'
    
    if int(re.findall('apt_rltp_request_raw_(\d+)_postback_file',col_l)[0])>28388:
        econn,pcur=getSnowflake()
    else:
        econn= presto.connect(**presto_config)
    
    try:
        samp_col = pd.read_sql(col_l, con=econn)
        econn.close()
        len_cols=len(samp_col.columns)
        for query in qry:
            if re.findall('apt_rltp_request_raw_',query):
                if int(re.findall('apt_rltp_request_raw_(\d+)_postback_file',query)[0])>28388:
                    econn,pcur=getSnowflake()
                else:
                    econn= presto.connect(**presto_config)
                    
                sqr = query+ ' LIMIT 3'
                    
                try:
                    samp = pd.read_sql(sqr, con=econn)
                    if len(samp.columns)!=len_cols:
                        validation_level.append('Query')
                        validation_status.append(f''' "columns count" : {query}''')
                except Exception as e:
                    validation_level.append('Query')
                    validation_status.append(query)
        econn.close()            
    except Exception as e:
        validation_level.append('Query')
        validation_status.append(qry[0])


#######added by

    validation_level.append('Added by')
    validation_status.append(df['added_by'][0])




except Exception as e:
    error='Other Errors:: '+str(e)
    validation_level.append(error)
    validation_status.append('Failed')
    validation_level.append('Added by')
    validation_status.append(df['added_by'][0])


dictr={'Validation Case':validation_level,'Validation Status':validation_status}



dfd=pd.DataFrame(dictr)
Qs='False'
if 'Query' in validation_level:
    check=dfd[dfd['Validation Case']=='Query']
    Qs=list(check['Validation Status']!='Pass')[0]

if 'Failed' in validation_status or Qs==True :

    print(dfd)
    cur.execute("update APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND set request_validation='N' where request_id="+sys.argv[1]+"")
    styles = [
    {'selector': 'th, td', 'props': [('white-space', 'nowrap'),('border', '1px solid black')]},
    {'selector': 'th', 'props': [('background-color', 'lightblue'), ('color', 'black'),('white-space', 'nowrap')]},
    {'selector': 'td', 'props': [('border', '1px solid black'),('color','black'),('text-align','center')]},
]
    html_table= dfd.style.applymap(style_cell,subset=['Validation Status']).applymap(style_first_row,subset=['Validation Case']).set_table_styles(styles).hide().to_html(index=False)
    #html_table=df.to_html(index=False,justify='center')
    sendEmailNotification(html_table,df6['client_name'][0])
else:
    df2.to_csv(df['cpm_report_path'][0],sep='|',header=False,index=False)
    df3.to_csv(df['decile_wise_report_path'][0],sep='|',header=False,index=False)
    cur.execute("update APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND set request_validation='Y',from_date="+"\'"+str(df2['Date'].min())+"\'"",end_date="+"\'"+str(df2['Date'].max())+"\'"" where request_id="+sys.argv[1]+"")



conn.close()
cur.close()
engine.dispose()
validation_level.clear()
validation_status.clear()
