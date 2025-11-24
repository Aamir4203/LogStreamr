source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE5 Start Time: `date`"




#==== ERROR FUNCTION ===#

error_fun()
{


    
    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1 ,request_end_time=now(), REQUEST_DESC='$2' where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set execution_time = TO_CHAR(AGE(request_end_time,request_start_time), 'HH24:MI:SS') where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,execution_time EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT,LAST_WK_DEL_INSERT_CNT,LAST_WK_UNSUB_INSERT_CNT,UNIQUE_DELIVERED_COUNT,NEW_RECORD_CNT,NEW_ADDED_IP_CNT,ADDED_BY from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
    ( '<b>RequestID</b>', REQUEST_ID::text ),
    ( '<b>ClientName</b>', CLIENT_NAME::text ),
    ( '<b>TRTFileCount</b>', RLTP_FILE_COUNT::text ),
    ( '<b>RequestStatus</b>', REQUEST_STATUS::text ),
    ( '<b>RequestDescription</b>', REQUEST_DESC::text ),
    ( '<b>StartTime</b>', REQUEST_START_TIME::text ),
    ( '<b>TotalExecutionTime</b>', EXECUTION_TIME::text ),
    ( '<b>UnsubHardsSuppressionCount</b>', POSTED_UNSUB_HARDS_SUPP_COUNT::text ),
    ( '<b>OfferIDSuppressionCount</b>', OFFERID_SUPPRESSED_COUNT::text ),
    ( '<b>ClientSuppressionCount</b>', CLIENT_SUPPRESSION_COUNT::text ),
    ( '<b>MaxTouchCount</b>', MAX_TOUCH_COUNT::text ),
    ( '<b>LastWeekDeliveredInsertedCount</b>', LAST_WK_DEL_INSERT_CNT::text ),
    ( '<b>LastWeekUnsubInsertedCount</b>', LAST_WK_UNSUB_INSERT_CNT::text ),
    ( '<b>UniqueDeliveredCount</b>', UNIQUE_DELIVERED_COUNT::text ),
    ( '<b>NewlyAddedRecordsCount</b>', NEW_RECORD_CNT::text ),
    ( '<b>NewlyAddedIPCount</b>', NEW_ADDED_IP_CNT::text ),
    ( '<b>ADDED_BY</b>', ADDED_BY::text)) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv

    sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"

}



$CONNECTION_STRING  -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Delivered Script Initiated' WHERE REQUEST_ID=$REQUEST_ID "

posted_unsub_table=`$CONNECTION_STRING -qtAX -c "select upper(POSTED_UNSUB_HARDS_TABLE) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.CLIENT_ID=b.CLIENT_ID where REQUEST_ID=$REQUEST_ID"`

client_name=`$CONNECTION_STRING -qtAX -c "select upper(CLIENT_NAME) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.client_id=b.client_id where request_id=$REQUEST_ID"`


$CONNECTION_STRING  -vv -c "create table $PB_TABLE (like $SRC_TABLE including all) "


if [[ $? -ne 0 ]]
then

        error_fun "5" "Unable to create delivered table"
		exit

fi

p_key=`echo $PB_TABLE | tr '[:upper:]' '[:lower:]' | sed 's/$/_pkey/g'`
email_key=`echo $PB_TABLE | tr '[:upper:]' '[:lower:]' | sed 's/$/_email_key/g'`

$CONNECTION_STRING  -vv -c "alter table $PB_TABLE drop constraint $email_key " 

$CONNECTION_STRING  -vv -c "alter table $PB_TABLE drop constraint $p_key " 

email_key=`echo $PB_TABLE | tr '[:upper:]' '[:lower:]' | sed 's/$/_email_del_date_key/g'`

$CONNECTION_STRING  -vv -c "alter table $PB_TABLE drop constraint $email_key "

email_key=`echo $PB_TABLE | tr '[:upper:]' '[:lower:]' | sed 's/$/_email_del_date_segment_key/g'`

$CONNECTION_STRING  -vv -c "alter table $PB_TABLE drop constraint $email_key "



$CONNECTION_STRING  -vv -c "alter table $PB_TABLE alter column id drop default " 


$CONNECTION_STRING  -vv -c "alter table $PB_TABLE alter column id drop not null "


pb_id=pb_$REQUEST_ID\_idx

$CONNECTION_STRING  -vv -c "create index $pb_id on $PB_TABLE (id) " 


$CONNECTION_STRING  -vv -c "alter table $PB_TABLE add campaign varchar,add subject varchar,add creative varchar,add open_date varchar,add click_date varchar,add unsub_date varchar,add diff int default 0,add offerid varchar,add ip varchar,add timestamp varchar " 

if [[ $? -ne 0 ]]
then

        error_fun "5" "Unable to alter delivered table"
		exit

fi


report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

if [[ $? -ne 0 ]]
then

        error_fun "5" "Unable to fetch report path"
		exit

fi


#=== PRODUCER DELIVERED SCRIPT  ===#

echo " PRODUCER EXECUTION StartTime:: `date` "

filename=consumerDeliveredScript_$REQUEST_ID.sh

while read dates
do

        date_=`echo $dates | sed 's/-//g'`
        date_report=report_$date_
        date_table=$PB_TABLE\_$date_


        grep "$dates" $report_path > $SPOOLPATH/$date_report

        sleep 3s

        sh -x $SCRIPTPATH/$filename $REQUEST_ID $SPOOLPATH/$date_report $date_table $date_ >> $LOGPATH/delivered_$date_.log 2>> $LOGPATH/delivered_$date_.log &

done <$SPOOLPATH/uniq_deldates




for ((;;))
do
		
		
        if [ "`ps -ef |grep $filename |wc -l `" -lt 2 ]
        then
                        sleep 20
                        break
        else
                        sleep 20
        fi

done

$CONNECTION_STRING  -vv -c "vacuum analyze $PB_TABLE"

get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and ERROR_CODE=5 and REQUEST_ID=$REQUEST_ID "`

if [[ $get_status == 'E' ]]
then

	error_fun "5" "Delivered consumer script is failed"
	exit
	
fi

$CONNECTION_STRING  -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Adjusting OR/CR' WHERE REQUEST_ID=$REQUEST_ID "
/usr/bin/python3 $SCRIPTPATH/openClickAdjustment.py "$REQUEST_ID"



#psql -U datateam -h zds-prod-pgdb01-01.bo3.e-dialog.com -d apt_tool_db -qtAX  -c "select email,md5hash,decile,segment,new_date from APT_ADHOC_CUST_TRT_MATCHES_SLING_ANUSHA_UNIQUE where new_date is not null order by new_date " > new_cust


touch=`$CONNECTION_STRING -qtAX -c " select MAX_TOUCH_COUNT from $QA_TABLE where REQUEST_ID=$REQUEST_ID "`

if [[ $touch -gt 1 ]]
then
	if [[ $client_name == 'VERIZON' ]]
	then
		$CONNECTION_STRING  -vv -c "alter table $PB_TABLE add unique(email,del_date,segment)"
		if [[ $? -ne 0 ]]
		then
			error_fun "5" "Unable to add unique key on email and del_date to delivered table"
		fi

	else
		 $CONNECTION_STRING  -vv -c "alter table $PB_TABLE add unique(email,del_date)"

		if [[ $? -ne 0 ]]
		then
	
			error_fun "5" "Unable to add unique key on email and del_date to delivered table"
	
		fi
	fi

else

	$CONNECTION_STRING  -vv -c "alter table $PB_TABLE add unique(email)"
	
	if [[ $? -ne 0 ]]
	then
	
			error_fun "5" "Unable to add unique key on email to delivered table"
	
	fi	
fi

$CONNECTION_STRING  -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Delivered Script Completed' WHERE REQUEST_ID=$REQUEST_ID "


echo " PRODUCER EXECUTION EndTime:: `date` "


#==== TIME STAMP APPENDING ===#

timestamp_append=`$CONNECTION_STRING -qtAX -c "select upper(TIMESTAMP_APPEND) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


ip_append=`$CONNECTION_STRING -qtAX -c "select upper(IP_APPEND) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


if [[ $timestamp_append == 'Y' ]]
then

	sh -x $SCRIPTPATH/timestampAppending.sh  $REQUEST_ID >>$LOGPATH/$REQUEST_ID.log 2>>$LOGPATH/$REQUEST_ID.log

	get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`

	if [[ $get_status == 'E' ]]
	then
	
		error_fun "5" "Erroed in timestamp appeding"
		exit
		
	fi
	
fi


#==== IP APPENDING ===#


if [[ $ip_append == 'Y' ]]
then

	sh -x $SCRIPTPATH/ipAppending.sh  $REQUEST_ID >>$LOGPATH/$REQUEST_ID.log 2>>$LOGPATH/$REQUEST_ID.log
	
	get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`

	if [[ $get_status == 'E' ]]
	then
	
		error_fun "5" "Erroed in ip appeding"
		exit
		
	fi	

fi
$CONNECTION_STRING  -vv -c "vacuum analyze $PB_TABLE"

$CONNECTION_STRING -vv -c "drop table $PARTITION_SRC"
$CONNECTION_STRING -vv -c "drop table $UNIQ_SRC_TABLE"
$CONNECTION_STRING -vv -c "drop table $REPLACE_IP_TABLE"
$CONNECTION_STRING -vv -c "drop table $REPLACE_TIMESTAMP_TABLE"
$CONNECTION_STRING -vv -c "drop table $SRC_TABLE"
$CONNECTION_STRING -vv -c "drop table $REPORT_TABLE"
$CONNECTION_STRING -vv -c "drop table $GREEN_TOTAL_UNSUBS_TEMP"
$PGDB2_CONN_STRING -vv -c "drop table if exists $ARCA_GENUINE_DEL_TEMP"


client_id=`$CONNECTION_STRING -qtAX -c "select CLIENT_ID from $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID "`

$CONNECTION_STRING -vv -c "update $CLIENT_TABLE set bkp_prev_pb_table=prev_week_pb_table where client_id='$client_id' "

$CONNECTION_STRING -vv -c "update $CLIENT_TABLE set prev_week_pb_table='$PB_TABLE' where client_id='$client_id' "



GenCount=`$CONNECTION_STRING -qtAX -c " select count(email) from $UNIQ_GEN_TABLE "`

PbReportedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $PB_TABLE b on a.email=b.email"`

TRTMatchedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $TRT_TABLE b on a.email=b.email"`


OpensGenCount=`$CONNECTION_STRING -qtAX -c " select count(email) from $UNIQ_GEN_TABLE where status=2"`

OpensPbReportedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $PB_TABLE b on a.email=b.email and a.status=2 "`

OpensTRTMatchedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $TRT_TABLE b on a.email=b.email and status=2 "`

OpensToOpensPbReportedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $PB_TABLE b on a.email=b.email and a.status=2 and b.open_date is not null"`


ClicksGenCount=`$CONNECTION_STRING -qtAX -c " select count(email) from $UNIQ_GEN_TABLE where status=3"`

ClicksPbReportedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $PB_TABLE b on a.email=b.email and a.status=3 "`

ClicksTRTMatchedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $TRT_TABLE b on a.email=b.email and status=3 "`

ClicksToClicksPbReportedGenCount=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $UNIQ_GEN_TABLE  a join $PB_TABLE b on a.email=b.email and a.status=3  and b.open_date is not null"`

total_del_cnt=`$CONNECTION_STRING -qtAX -c " select count(email) from $PB_TABLE "`


$CONNECTION_STRING -vv -c " update $QA_TABLE set actualopenscount='$OpensGenCount' , openstrtmatchcount='$OpensTRTMatchedGenCount' , openspbreportedcount='$OpensPbReportedGenCount' ,actualclickscount='$ClicksGenCount' ,clickstrtmatchcount='$ClicksTRTMatchedGenCount' , clickspbreportedcount='$ClicksPbReportedGenCount'  , OpensToOpensPbReportedGenCount='$OpensToOpensPbReportedGenCount' , ClicksToClicksPbReportedGenCount='$ClicksToClicksPbReportedGenCount' ,ActualLogsCount='$GenCount', ActualLogsTRTMatchCount='$TRTMatchedGenCount', ActualLogsPbReportedCount='$PbReportedGenCount' , TotalDeliveredCount='$total_del_cnt' where REQUEST_ID=$REQUEST_ID "


#$CONNECTION_STRING -vv -c " update $QA_TABLE set ActualLogsCount='$GenCount', ActualLogsTRTMatchCount='$TRTMatchedGenCount', ActualLogsPbReportedCount='$PbReportedGenCount'  where REQUEST_ID=$REQUEST_ID "



#=== MAIL ALERT ===#

    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='C' , REQUEST_DESC='Request Completed :) ' where REQUEST_ID=$REQUEST_ID"


#=== INSERT LAST WEEK POSTBACK UNSUBS AND HARDS ===#

$CONNECTION_STRING -qtAX -c "update $PB_TABLE set unsub=1,unsub_date=null where unsub_date is not null"

$CONNECTION_STRING -qtAX -c "update $PB_TABLE set unsub_date=open_date where click_date is null and open_date is not null and unsub=1"


#=== UNSUBS INSERT ===#

inserted_unsub_cnt=`$CONNECTION_STRING -qtAX -c " with cte as (select email,segment,del_date,unsub_date from $PB_TABLE where unsub_date is not null ) , rows as (insert into $posted_unsub_table(email,segment,del_date,unsub_date) select * from cte on conflict do nothing returning 1) select count(*) from rows"`

if [[ $? -ne 0 ]]
then

error_fun "5" "Unable to insert last week unsubs into posted unsub table"
exit

fi


$CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET LAST_WK_UNSUB_INSERT_CNT=$inserted_unsub_cnt WHERE REQUEST_ID=$REQUEST_ID "



#== HARDS INSERTS ===#

$CONNECTION_STRING -vv -c " with cte as (select email,segment,del_date,flag from $PB_TABLE where flag='B') insert into $posted_unsub_table(email,segment,del_date,flag) select * from cte on conflict do nothing"

if [[ $? -ne 0 ]]
then

error_fun "5" "Unable to insert last week hards into posted unsub table"
exit

fi


   # $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,age(now()::timestamp(0),REQUEST_START_TIME) EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT,LAST_WK_DEL_INSERT_CNT,LAST_WK_UNSUB_INSERT_CNT,UNIQUE_DELIVERED_COUNT,NEW_RECORD_CNT,NEW_ADDED_IP_CNT,TOTAL_RUNNING_UNIQ_CNT,PREV_WEEK_PB_TABLE,ActualLogsCount,ActualLogsTRTMatchCount,ActualLogsPbReportedCount from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
   # ( '<b>RequestID</b>', REQUEST_ID::text ),
   # ( '<b>ClientName</b>', CLIENT_NAME::text ),
   # ( '<b>TRTFileCount</b>', RLTP_FILE_COUNT::text ),
   # ( '<b>RequestStatus</b>', REQUEST_STATUS::text ),
   # ( '<b>RequestDescription</b>', REQUEST_DESC::text ),
   # ( '<b>StartTime</b>', REQUEST_START_TIME::text ),
   # ( '<b>TotalExecutionTime</b>', EXECUTION_TIME::text ),
   # ( '<b>UnsubHardsSuppressionCount</b>', POSTED_UNSUB_HARDS_SUPP_COUNT::text ),
   # ( '<b>OfferIDSuppressionCount</b>', OFFERID_SUPPRESSED_COUNT::text ),
   # ( '<b>ClientSuppressionCount</b>', CLIENT_SUPPRESSION_COUNT::text ),
   # ( '<b>MaxTouchCount</b>', MAX_TOUCH_COUNT::text ),
   # ( '<b>LastWeekDeliveredInsertedCount</b>', LAST_WK_DEL_INSERT_CNT::text ),
   # ( '<b>UnsubInsertedCount</b>', LAST_WK_UNSUB_INSERT_CNT::text ),
   # ( '<b>UniqueDeliveredCount</b>', UNIQUE_DELIVERED_COUNT::text ),
   # ( '<b>NewlyAddedRecordsCount</b>', NEW_RECORD_CNT::text ),
   # ( '<b>NewlyAddedIPCount</b>', NEW_ADDED_IP_CNT::text ),
   # ( '<b>TotalRunningUniqueCount</b>', TOTAL_RUNNING_UNIQ_CNT::text ),
   # ( '<b>DeliveredTable</b>', PREV_WEEK_PB_TABLE::text ),
   # ( '<b>ActualLogsCount</b>', ActualLogsCount::text ),
   # ( '<b>ActualLogsTRTMatchCount</b>', ActualLogsTRTMatchCount::text ),
   # ( '<b>ActualLogsPbReportedCount</b>', ActualLogsPbReportedCount::text )) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv

    #sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"

	$CONNECTION_STRING -vv -c "update $REQUEST_TABLE set request_end_time=now() where REQUEST_ID=$REQUEST_ID"
	$CONNECTION_STRING -vv -c "update $REQUEST_TABLE set execution_time = TO_CHAR(AGE(request_end_time,request_start_time), 'HH24:MI:SS') where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,execution_time EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT,LAST_WK_DEL_INSERT_CNT,LAST_WK_UNSUB_INSERT_CNT,UNIQUE_DELIVERED_COUNT,TotalDeliveredCount,NEW_RECORD_CNT,NEW_ADDED_IP_CNT,TOTAL_RUNNING_UNIQ_CNT,PREV_WEEK_PB_TABLE,ActualLogsCount,ActualLogsTRTMatchCount,ActualLogsPbReportedCount,Added_By from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
    ( '<b>RequestID</b>', REQUEST_ID::text ),
    ( '<b>ClientName</b>', CLIENT_NAME::text ),
    ( '<b>TRTFileCount</b>', RLTP_FILE_COUNT::text ),
    ( '<b>RequestStatus</b>', REQUEST_STATUS::text ),
    ( '<b>RequestDescription</b>', REQUEST_DESC::text ),
    ( '<b>StartTime</b>', REQUEST_START_TIME::text ),
    ( '<b>TotalExecutionTime</b>', EXECUTION_TIME::text ),
    ( '<b>UnsubHardsSuppressionCount</b>', POSTED_UNSUB_HARDS_SUPP_COUNT::text ),
    ( '<b>OfferIDSuppressionCount</b>', OFFERID_SUPPRESSED_COUNT::text ),
    ( '<b>ClientSuppressionCount</b>', CLIENT_SUPPRESSION_COUNT::text ),
    ( '<b>MaxTouchCount</b>', MAX_TOUCH_COUNT::text ),
    ( '<b>LastWeekDeliveredInsertedCount</b>', LAST_WK_DEL_INSERT_CNT::text ),
    ( '<b>UnsubInsertedCount</b>', LAST_WK_UNSUB_INSERT_CNT::text ),
    ( '<b>UniqueDeliveredCount</b>', UNIQUE_DELIVERED_COUNT::text ),
    ( '<b>TotalDeliveredCount</b>', TotalDeliveredCount::text ),
    ( '<b>NewlyAddedRecordsCount</b>', NEW_RECORD_CNT::text ),
    ( '<b>NewlyAddedIPCount</b>', NEW_ADDED_IP_CNT::text ),
    ( '<b>TotalRunningUniqueCount</b>', TOTAL_RUNNING_UNIQ_CNT::text ),
    ( '<b>DeliveredTable</b>', PREV_WEEK_PB_TABLE::text ),
    ( '<b>AddedBy</b>', Added_By::text)) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv

    $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select actuallogscount,actuallogstrtmatchcount,actuallogspbreportedcount,actualopenscount,openstrtmatchcount,openspbreportedcount,actualclickscount,clickstrtmatchcount,clickspbreportedcount,openstoopenspbreportedgencount,clickstoclickspbreportedgencount from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
    ( '<b>ActuaLogsCount</b>', actuallogscount::text ),
    ( '<b>ActualLogsTRTmatchCount</b>', actuallogstrtmatchcount::text ),
    ( '<b>ActualLogsPBreportedCount</b>', actuallogspbreportedcount::text ),
    ( '<b>ActualOpensCount</b>', actualopenscount::text ),
    ( '<b>OpensTRTmatchCount</b>', openstrtmatchcount::text ),
    ( '<b>OpensPBreportedCount</b>', openspbreportedcount::text ),
    ( '<b>ActualClicksCount</b>', actualclickscount::text ),
    ( '<b>ClicksTRTmatchCount</b>', clickstrtmatchcount::text ),
    ( '<b>ClicksPBreportedCount</b>', clickspbreportedcount::text ),
    ( '<b>OpensToOpensPBreportedGenCount</b>', openstoopenspbreportedgencount::text ),
    ( '<b>ClicksToClicksPBreportedGenCount</b>', clickstoclickspbreportedgencount::text )) x(Header, Value)" >$SPOOLPATH/fetchLogsDetails.csv

	sh $SCRIPTPATH/sendMail2.sh "$REQUEST_ID"

	echo "MODULE5 Start Time: `date`"
