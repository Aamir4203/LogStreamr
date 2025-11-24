#/bin/bash

source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/SCRIPTS/config.properties



new_request_id=$1

sendmail_fun()
{

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

}

new_request_status=`$CONNECTION_STRING -qtAX -c "select upper(request_status) from $REQUEST_TABLE where request_id=$new_request_id "`

subseg=`$CONNECTION_STRING -qtAX -c "select UPPER(SUB_SEG) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

if [[ $new_request_status == 'W' ]]
then


        mkdir $HOMEPATH
        mkdir $HOMEPATH/FILES/
        mkdir $HOMEPATH/SPOOL/
        mkdir $HOMEPATH/LOGS/
        mkdir $HOMEPATH/ETC/
        mkdir $SCRIPTPATH/


        cp $MAIN_SCRIPTS/config.properties $HOMEPATH/ETC/

        cp $MAIN_SCRIPTS/*.sh $SCRIPTPATH/

        cp $MAIN_SCRIPTS/*.py $SCRIPTPATH/

        cp $MAIN_SCRIPTS/*.jar $SCRIPTPATH/

        cp $MAIN_SCRIPTS/consumerDeliveredScript.sh $SCRIPTPATH/consumerDeliveredScript_$new_request_id.sh

        cp $MAIN_SCRIPTS/respondersPulling.sh $SCRIPTPATH/respondersPulling_$new_request_id.sh

        sh -x $SCRIPTPATH/trtPreparation.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log


elif [[ $new_request_status == 'RW' ]]
then

                if [ -d "$HOMEPATH" ]
                then

                        pb_table=`$CONNECTION_STRING -qtAX -c "select prev_week_pb_table from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  where  a.REQUEST_ID=$REQUEST_ID"`
                        $CONNECTION_STRING -vv -c "delete from  $POSTED_UNSUB_HARDS_TABLE a using $pb_table b where b.unsub_date is not null and a.email=b.email"

                        if [[ $? -ne 0 ]]
                        then

                                echo -e " Hi Team, \n Unable to delete from client unsub table for re-work. \n\n Thanks, \n SysAdmin" | mail -s "APT REQUEST DETAILS :: $CLIENT_NAME "
                                $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set request_status='E',request_desc='Unable to delete unsubs from client table' where request_id=$REQUEST_ID "
				exit
                        fi

                        rm -rf $HOMEPATH

            $CONNECTION_STRING -vv -c "drop table if exists $TRT_TABLE , $ARCA_GENUINE_DEL_TEMP , $GREEN_DELIVERED_TEMP, $GREEN_OPENS_TEMP , $GREEN_CLICKS_TEMP , $GREEN_UNSUBS_TEMP , $GREEN_FINAL_TEMP , $ORANGE_GENUNIE_DELIVERED ,$ORANGE_DEPLOY_IDS_TABLE , $ORANGE_GENUINE_DEL_TEMP , $GREEN_TOTAL_UNSUBS_TEMP , $SUPP_TABLE,$PARTITION_SRC ,  $SRC_TABLE, $UNIQ_SRC_TABLE , $PB_TABLE , $REPORT_TABLE , $DECILE_TABLE , $UNIQ_GEN_TABLE , $REPLACE_IP_TABLE , $REPLACE_TIMESTAMP_TABLE"

                        $PGDB2_CONN_STRING -vv -c "drop table if exists $ARCA_GENUINE_DEL_TEMP"

			

                        $CONNECTION_STRING -vv -c "update $CLIENT_TABLE set prev_week_pb_table=bkp_prev_pb_table where client_name='$CLIENT_NAME' "

                        if [[ $? -ne 0 ]]
                        then

                                echo -e " Hi Team, \n Unable to update client table for re-work. \n\n Thanks, \n SysAdmin" | mail -s "APT REQUEST DETAILS :: $CLIENT_NAME "
                                exit

                        fi

                        mkdir $HOMEPATH
                        mkdir $HOMEPATH/FILES/
                        mkdir $HOMEPATH/SPOOL/
                        mkdir $HOMEPATH/LOGS/
                        mkdir $HOMEPATH/ETC/
                        mkdir $SCRIPTPATH/


                        cp $MAIN_SCRIPTS/config.properties $HOMEPATH/ETC/

                        cp $MAIN_SCRIPTS/*.sh $SCRIPTPATH/

                        cp $MAIN_SCRIPTS/*.py $SCRIPTPATH/

                        cp $MAIN_SCRIPTS/*.jar $SCRIPTPATH/

                        cp $MAIN_SCRIPTS/consumerDeliveredScript.sh $SCRIPTPATH/consumerDeliveredScript_$new_request_id.sh

                        cp $MAIN_SCRIPTS/respondersPulling.sh $SCRIPTPATH/respondersPulling_$new_request_id.sh

                        sh -x $SCRIPTPATH/trtPreparation.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

                fi


elif [[ $new_request_status == 'RE' ]]
then

        request_error_code=`$CONNECTION_STRING -qtAX -c "select error_code from $REQUEST_TABLE where request_id=$new_request_id "`

        request_status=`$CONNECTION_STRING -qtAX -c "select upper(request_status) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

        report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


        $CONNECTION_STRING -vv -c "update $REQUEST_TABLE SET REQUEST_START_TIME=now() :: timestamp(0) , REQUEST_STATUS='R' where REQUEST_ID=$REQUEST_ID"


        $CONNECTION_STRING -vv -c "drop table if exists $REPORT_TABLE"

        $CONNECTION_STRING -vv -c "create table IF NOT EXISTS $REPORT_TABLE(CAMPAIGN VARCHAR , DEL_DATE VARCHAR , DEL_COUNT int , OPEN_COUNT int , CLICK_COUNT int ,UNSUB_COUNT int , SOFT_COUNT int , HARD_COUNT int , SUBJECT VARCHAR , CREATIVE VARCHAR ,CREATIVEID VARCHAR , OFFERID VARCHAR , SEGMENT VARCHAR ,SUB_SEG VARCHAR)"

        if [[ $subseg == 'Y' ]]
        then

                        $CONNECTION_STRING -vv -c "\copy $REPORT_TABLE from '$report_path' with delimiter '|'"

        fi



        if [[ $request_error_code == '1' ]]
        then

                $CONNECTION_STRING -vv -c "drop table if exists $TRT_TABLE , $ARCA_GENUINE_DEL_TEMP , $GREEN_DELIVERED_TEMP, $GREEN_OPENS_TEMP , $GREEN_CLICKS_TEMP , $GREEN_UNSUBS_TEMP , $GREEN_FINAL_TEMP , $ORANGE_GENUNIE_DELIVERED ,$ORANGE_DEPLOY_IDS_TABLE , $ORANGE_GENUINE_DEL_TEMP , $GREEN_TOTAL_UNSUBS_TEMP , $SUPP_TABLE,$PARTITION_SRC ,  $SRC_TABLE, $UNIQ_SRC_TABLE , $PB_TABLE , $REPORT_TABLE , $DECILE_TABLE , $UNIQ_GEN_TABLE , $REPLACE_IP_TABLE , $REPLACE_TIMESTAMP_TABLE"

                $PGDB2_CONN_STRING -vv -c "drop table if exists $ARCA_GENUINE_DEL_TEMP"

                >$HOMEPATH/LOGS/$new_request_id.log
                >$HOMEPATH/LOGS/TRT_error.log
                >$HOMEPATH/LOGS/TRT_sucess.log
                >$HOMEPATH/LOGS/Resp_$new_request_id.log

                sh -x $SCRIPTPATH/trtPreparation.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

        fi





        if [[ $request_error_code == '2' ]]
        then

                $CONNECTION_STRING -vv -c " drop table if exists $UNIQ_GEN_TABLE , $ARCA_GENUINE_DEL_TEMP , $GREEN_DELIVERED_TEMP, $GREEN_OPENS_TEMP , $GREEN_CLICKS_TEMP , $GREEN_UNSUBS_TEMP , $GREEN_FINAL_TEMP , $ORANGE_GENUNIE_DELIVERED ,$ORANGE_DEPLOY_IDS_TABLE , $ORANGE_GENUINE_DEL_TEMP , $GREEN_TOTAL_UNSUBS_TEMP,$SUPP_TABLE,$PARTITION_SRC ,  $SRC_TABLE, $UNIQ_SRC_TABLE , $PB_TABLE "

                $PGDB2_CONN_STRING -vv -c "drop table if exists $ARCA_GENUINE_DEL_TEMP"

                >$HOMEPATH/LOGS/$new_request_id.log

                sh -x $SCRIPTPATH/respondersPulling.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log
                sh -x $SCRIPTPATH/suppressionList.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

        fi



        if [[ $request_error_code == '3' ]]
        then

                $CONNECTION_STRING -vv -c "drop table if exists $SUPP_TABLE,$PARTITION_SRC ,  $SRC_TABLE, $UNIQ_SRC_TABLE , $PB_TABLE"
                >$HOMEPATH/LOGS/$new_request_id.log

                sh -x $SCRIPTPATH/suppressionList.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

        fi

        if [[ $request_error_code == '4' ]]
        then

                >$HOMEPATH/LOGS/$new_request_id.log

                $CONNECTION_STRING -vv -c "drop table if exists $SRC_TABLE  , $PARTITION_SRC , $UNIQ_SRC_TABLE , $PB_TABLE,$REPLACE_IP_TABLE,$REPLACE_TIMESTAMP_TABLE "

                sh -x $SCRIPTPATH/srcPreparation.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

        fi


        if [[ $request_error_code == '5' ]]
        then

                $CONNECTION_STRING -vv -c "drop table if exists $PB_TABLE,$PARTITION_SRC,$REPLACE_IP_TABLE,$REPLACE_TIMESTAMP_TABLE "

                sh -x $SCRIPTPATH/partitioningSrc.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

                get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`

                if [[ $get_status == 'E' ]]
                then
                                        exit

                fi


                sh -x $SCRIPTPATH/deliveredScript.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

                get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`
                if [[ $get_status == 'E' ]]
                then
					exit

                fi

        fi

        if [[ $request_error_code == '6' ]]
        then

                $CONNECTION_STRING -vv -c "drop table if exists $REPLACE_TIMESTAMP_TABLE "
                $CONNECTION_STRING -vv -c "update $PB_TABLE set timestamp=null "


                sh -x $SCRIPTPATH/timestampAppending.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log

                get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`

                if [[ $get_status == 'E' ]]
                then
					exit

                fi
				
				ip_append=`$CONNECTION_STRING -qtAX -c "select upper(IP_APPEND) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

				if [[ $ip_append == 'Y' ]]
				then
				
					sh -x $SCRIPTPATH/ipAppending.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log
					get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`
	
					if [[ $get_status == 'E' ]]
					then
						exit
	
					fi

				fi
				
                $CONNECTION_STRING -vv -c "drop table if exists $PARTITION_SRC"
                $CONNECTION_STRING -vv -c "drop table if exists $UNIQ_SRC_TABLE"
                $CONNECTION_STRING -vv -c "drop table if exists $REPLACE_IP_TABLE"
                $CONNECTION_STRING -vv -c "drop table if exists $REPLACE_TIMESTAMP_TABLE"
				
				sendmail_fun



        fi

        if [[ $request_error_code == '7' ]]
        then

                $CONNECTION_STRING -vv -c "drop table if exists $REPLACE_IP_TABLE "
                $CONNECTION_STRING -vv -c "update $PB_TABLE set ip=null where ip is not null "

                sh -x $SCRIPTPATH/ipAppending.sh  $new_request_id >>$HOMEPATH/LOGS/$new_request_id.log 2>>$HOMEPATH/LOGS/$new_request_id.log
				
                get_status=`$CONNECTION_STRING -qtAX -c "select upper(REQUEST_STATUS) from $REQUEST_TABLE where REQUEST_STATUS='E' and REQUEST_ID=$REQUEST_ID "`

                if [[ $get_status == 'E' ]]
                then
					exit

                fi

                $CONNECTION_STRING -vv -c "drop table if exists $PARTITION_SRC"
                $CONNECTION_STRING -vv -c "drop table if exists $UNIQ_SRC_TABLE"
                $CONNECTION_STRING -vv -c "drop table if exists $REPLACE_IP_TABLE"
                $CONNECTION_STRING -vv -c "drop table if exists $REPLACE_TIMESTAMP_TABLE"
				
		sendmail_fun


        fi



fi

