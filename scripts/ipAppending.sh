source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE7 Start Time: `date`"



#==== ERROR FUNCTION ===#

error_fun()
{


    echo "ERROR: $2" >> $LOGPATH
    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1  ,request_end_time=now(), REQUEST_DESC='$2' where REQUEST_ID=$REQUEST_ID"
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
	exit
}

$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Updating IPs',REQUEST_STATUS='R' WHERE REQUEST_ID=$REQUEST_ID "


#==== update ips matching with used ip table ====#

$CONNECTION_STRING -qtAX -c " update $PB_TABLE a set ip=b.ip from $OLD_IP_TABLE b where a.email=b.email and open_date is not null "

if [[ $? -ne 0 ]]
then

        error_fun "7" "Unable to update old ips to delivered table"
        exit

fi


$CONNECTION_STRING -vv -c "vacuum analyze $OLD_IP_TABLE "
#====  GET NEW IPS ====#


$CONNECTION_STRING -qtAX -c "select distinct  email from $PB_TABLE where open_date is not null and ip is null " > $SPOOLPATH/RequiredIpEmails

if [[ $? -ne 0 ]]
then

        error_fun "7" "Unable to write emails for opens with no ips"
        exit

fi


req_ip_count=`wc -l $SPOOLPATH/RequiredIpEmails | awk -F' ' '{ print $1 }'`

if [[ $req_ip_count -gt 0 ]]
then



	new_ip_cnt=`$CONNECTION_STRING -qtAX -c "select count(a.ip) from $NEW_IP_TABLE a left join $OLD_IP_TABLE b on a.ip=b.ip where b.ip is null " `
	
	if [[ $new_ip_cnt -gt $req_ip_count ]]
	then
	
		$CONNECTION_STRING -qtAX -c "select ip from $NEW_IP_TABLE order by random() limit $req_ip_count " > $SPOOLPATH/RequiredIps
	
	
		if [[ $? -ne 0 ]]
		then
		
				error_fun "7" "Unable to write ips from new_ip table"
				exit
		
		fi
		
		paste -d'|' $SPOOLPATH/RequiredIpEmails $SPOOLPATH/RequiredIps > $SPOOLPATH/Final_Ips
		
		$CONNECTION_STRING -vv -c "create table $REPLACE_IP_TABLE(email varchar unique,ip varchar unique)"
		
		if [[ $? -ne 0 ]]
		then
		
				error_fun "7" "Unable to create replace ip table"
				exit
		
		fi		
		
		$CONNECTION_STRING -qtAX -c "\copy $REPLACE_IP_TABLE from '$SPOOLPATH/Final_Ips' with delimiter '|' "

		if [[ $? -ne 0 ]]
		then
		
				error_fun "7" "Unable to copy data to replace ip table"
				exit
		
		fi	

		#==== update ips matching with new ip table ====#

		$CONNECTION_STRING -qtAX -c " update $PB_TABLE a set ip=b.ip from $REPLACE_IP_TABLE b where a.email=b.email and open_date is not null "
		
		if [[ $? -ne 0 ]]
		then
		
				error_fun "7" "Unable to update new ips to the delivered table"
				exit
		
		fi
		
        #=== UPDATE NEWLY ADDED IP COUNT TO THE QA TABLE ====#

        $CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET NEW_ADDED_IP_CNT=$req_ip_count WHERE REQUEST_ID=$REQUEST_ID "		
		
		
	else
	
		error_fun "7" "New IP table doesnt have sufficiet ips."
	
	fi

fi

$CONNECTION_STRING  -vv -c "vacuum analyze $PB_TABLE"

#==== INSERT INTO USED IP TABLE FOR THE NEW IPS ====#


$CONNECTION_STRING -vv -c "insert into $OLD_IP_TABLE (email,ip) select email,ip from $PB_TABLE where open_date is not null on conflict do nothing "

if [[ $? -ne 0 ]]
then

		error_fun "7" "Unable to insert ips into used IP table"
		exit

fi



$CONNECTION_STRING -vv -c "delete from $NEW_IP_TABLE a using $OLD_IP_TABLE b where a.ip=b.ip "



