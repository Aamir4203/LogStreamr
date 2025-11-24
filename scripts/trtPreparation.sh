#/bin/bash

source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE1 Start Time: `date`"


$CONNECTION_STRING -vv -c "update $REQUEST_TABLE SET REQUEST_START_TIME=now() :: timestamp(0) , REQUEST_STATUS='R' where REQUEST_ID=$REQUEST_ID"

$CONNECTION_STRING -vv -c "insert into $QA_TABLE(REQUEST_ID) SELECT $REQUEST_ID on conflict do nothing"


#==== ERROR FUNCTION ===#

error_fun()
{


        $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1 ,request_end_time=now(),  REQUEST_DESC=concat(REQUEST_DESC,'- ','$2') where REQUEST_ID=$REQUEST_ID"

        $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set execution_time = TO_CHAR(AGE(request_end_time,request_start_time), 'HH24:MI:SS') where REQUEST_ID=$REQUEST_ID"

        $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,execution_time EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT,ADDED_BY from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
    ( '<b>REQUEST_ID</b>', REQUEST_ID::text ),
    ( '<b>CLIENT_NAME</b>', CLIENT_NAME::text ),
    ( '<b>RLTP_FILE_COUNT</b>', RLTP_FILE_COUNT::text ),
    ( '<b>REQUEST_STATUS</b>', REQUEST_STATUS::text ),
    ( '<b>REQUEST_DESC</b>', REQUEST_DESC::text ),
    ( '<b>REQUEST_START_TIME</b>', REQUEST_START_TIME::text ),
    ( '<b>EXECUTION_TIME</b>', EXECUTION_TIME::text ),
    ( '<b>POSTED_UNSUB_HARDS_SUPP_COUNT</b>', POSTED_UNSUB_HARDS_SUPP_COUNT::text ),
    ( '<b>OFFERID_SUPPRESSED_COUNT</b>', OFFERID_SUPPRESSED_COUNT::text ),
    ( '<b>CLIENT_SUPPRESSION_COUNT</b>', CLIENT_SUPPRESSION_COUNT::text ),
    ( '<b>MAX_TOUCH_COUNT</b>', MAX_TOUCH_COUNT::text ),
    ( '<b>ADDED_BY</b>', ADDED_BY::text)) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv

        kill -9 $resp_script_pid

        sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"

}

$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Preparing TRT' WHERE REQUEST_ID=$REQUEST_ID "


#=== CREATE REPORT TABLE ===#


subseg=`$CONNECTION_STRING -qtAX -c "select UPPER(SUB_SEG) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


#$CONNECTION_STRING -vv -c "create table $REPORT_TABLE(CAMPAIGN VARCHAR , DEL_DATE VARCHAR , DEL_COUNT int , OPEN_COUNT int , CLICK_COUNT int ,UNSUB_COUNT int , SOFT_COUNT int , HARD_COUNT int , SUBJECT VARCHAR , CREATIVE VARCHAR ,CREATIVEID VARCHAR , OFFERID VARCHAR , SEGMENT VARCHAR ,SUB_SEG VARCHAR)"
$CONNECTION_STRING -vv -c "drop table if exists $REPORT_TABLE"

$CONNECTION_STRING -vv -c "create table IF NOT EXISTS $REPORT_TABLE(CAMPAIGN VARCHAR , DEL_DATE VARCHAR , DEL_COUNT int , OPEN_COUNT int , CLICK_COUNT int ,UNSUB_COUNT int , SOFT_COUNT int , HARD_COUNT int , SUBJECT VARCHAR , CREATIVE VARCHAR ,CREATIVEID VARCHAR , OFFERID VARCHAR , SEGMENT VARCHAR ,SUB_SEG VARCHAR)"


if [[ $subseg == 'Y' ]]
then

        $CONNECTION_STRING -vv -c "\copy $REPORT_TABLE from '$report_path' with delimiter '|'"

        if [[ $? -ne 0 ]]
        then

                error_fun "1" "Unable to load data into report table"
                                exit

        fi

else

        $CONNECTION_STRING -vv -c "\copy $REPORT_TABLE(CAMPAIGN,DEL_DATE,DEL_COUNT,OPEN_COUNT,CLICK_COUNT,UNSUB_COUNT,SOFT_COUNT,HARD_COUNT,SUBJECT,CREATIVE,CREATIVEID,OFFERID,SEGMENT) from '$report_path' with delimiter '|'"

        if [[ $? -ne 0 ]]
        then
                error_fun "1" "Unable to load data into report table"
                                exit

        fi

fi





#=== EXECUTING RESPONDERS SCRIPT ===#

sh -x $SCRIPTPATH/respondersPulling_$REQUEST_ID.sh  $REQUEST_ID >>$HOMEPATH/LOGS/Resp_$REQUEST_ID.log 2>>$HOMEPATH/LOGS/Resp_$REQUEST_ID.log &

resp_script_pid=$!

#==EXECUTING RLTP SCRIPT ==#

echo "Presto Start time:`date`"

/usr/bin/python3 $SCRIPTPATH/rltpDataPulling.py "$REQUEST_ID"

if [[ $? -ne 0 ]]
then

        error_fun "1" "RLTP Failure"
        exit
fi

echo "Presto End time:`date`"


#=== CHECK RESPONDERS BACKGROUND RUN ===#

resp_file=respondersPulling_$REQUEST_ID.sh

for ((;;))
do


        if [ "`ps -ef |grep $resp_file |wc -l `" -lt 2 ]
        then
                        sleep 20
                                                break
        else
                        sleep 20
        fi

done


get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where upper(REQUEST_STATUS)='E' and REQUEST_ID=$REQUEST_ID "`

if [[ $get_status == 'E' ]]
then

        $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
    ( '<b>REQUEST_ID</b>', REQUEST_ID::text ),
    ( '<b>CLIENT_NAME</b>', CLIENT_NAME::text ),
    ( '<b>RLTP_FILE_COUNT</b>', RLTP_FILE_COUNT::text ),
    ( '<b>REQUEST_STATUS</b>', REQUEST_STATUS::text ),
    ( '<b>REQUEST_DESC</b>', REQUEST_DESC::text ),
    ( '<b>REQUEST_START_TIME</b>', REQUEST_START_TIME::text ),
    ( '<b>EXECUTION_TIME</b>', EXECUTION_TIME::text ),
    ( '<b>POSTED_UNSUB_HARDS_SUPP_COUNT</b>', POSTED_UNSUB_HARDS_SUPP_COUNT::text ),
    ( '<b>OFFERID_SUPPRESSED_COUNT</b>', OFFERID_SUPPRESSED_COUNT::text ),
    ( '<b>CLIENT_SUPPRESSION_COUNT</b>', CLIENT_SUPPRESSION_COUNT::text ),
    ( '<b>MAX_TOUCH_COUNT</b>', MAX_TOUCH_COUNT::text )) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv
    echo "SendingMail"
    sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"
        exit

fi

#==== GENUINE OFFER ID UNSUB SUPPRESSION ====#



offerid_unsub_supp_cnt=`$CONNECTION_STRING -qtAX -c "select upper(OFFERID_UNSUB_SUPP) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


if [[ $offerid_unsub_supp_cnt == "Y" ]]
then


        mindate=`$CONNECTION_STRING -qtAX -c "select min(del_date)  from $REPORT_TABLE  " `

        echo " OFFERID SUPPRESSION START TIME: `date`"
	#SET enable_seqscan TO off;
        offerid_unsubs_supp_cnt=`$CONNECTION_STRING -qtAX -c "with cte as (delete from $TRT_TABLE a using $GREEN_TOTAL_UNSUBS_TEMP b where a.email=b.email  returning 1 ) select count(*) from cte"`

        if [[ $? -ne 0 ]]
        then

                error_fun "1" "Unable to perform offer id unsub suppression to the TRT"
                exit
        else

                $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_DESC='OfferId Suppressions Completed' where REQUEST_ID=$REQUEST_ID  "

                $CONNECTION_STRING -vv -c "update $QA_TABLE set  OFFERID_UNSUB_SUPP_COUNT=$offerid_unsubs_supp_cnt where REQUEST_ID=$REQUEST_ID"

        fi
        echo " OFFERID SUPPRESSION END TIME: `date`"

fi


$CONNECTION_STRING -vv -c "vacuum analyze $TRT_TABLE"

sh -x $SCRIPTPATH/suppressionList.sh  $REQUEST_ID >>$HOMEPATH/LOGS/$REQUEST_ID.log 2>>$HOMEPATH/LOGS/$REQUEST_ID.log


