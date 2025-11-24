source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE6 Start Time: `date`"



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


$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Updating DeliveredTimestamps',REQUEST_STATUS='R' WHERE REQUEST_ID=$REQUEST_ID "

#====

timestamp_input=`$CONNECTION_STRING -qtAX -c "select TIMESTAMP_REPORT_PATH from $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID "`

if [[ $? -ne 0 ]]
then

        error_fun "6" "Unable to get delivered timestamps absolute file path"
        exit

fi

if [[ -f "$timestamp_input" ]]
then


	> $SPOOLPATH/combine_timestamps
	
	while read timestamp
	do
	
		t_date=`echo $timestamp | cut -d'|' -f1`
		t_start=`echo $timestamp | cut -d'|' -f2`
		t_end=`echo $timestamp | cut -d'|' -f3`
		
		t_date_1=`echo $t_date | awk -F' ' '{ print $1 }'  `
		
		cnt=`$CONNECTION_STRING -qtAX -c "select count(id) from $PB_TABLE where del_date='$t_date_1' "`
		
	    if [[ $? -ne 0 ]]
	    then
	    
	    		error_fun "6" "unable to get count for delivered date"
	    		exit
	    
	    fi	
		
		total_secs=`$CONNECTION_STRING -qtAX -c "select EXTRACT(EPOCH FROM (cast('$t_end' as timestamp) - cast('$t_start' as timestamp))) "`
		
	    if [[ $? -ne 0 ]]
	    then
	    
	    		error_fun "6" "unable to get total seconds for delivered timestamps"
	    		exit
	    
	    fi		
		
		
		per_sec_val=`expr $cnt/$total_secs | bc`
		
		l_limit=`expr $per_sec_val - 5 | bc`
		u_limit=`expr $per_sec_val + 8 | bc`
		
		java -classpath "$SCRIPTPATH/TimeStampGenerator.jar" TimeStampGenerator "$t_start" "$l_limit" "$u_limit" "$cnt" "$SPOOLPATH/"
		
	    if [[ $? -ne 0 ]]
	    then
	    
	    		error_fun "6" "Execution error in delivered timestamps java script"
	    		exit
	    
	    fi		
		
		$CONNECTION_STRING -qtAX -c "select id from $PB_TABLE where del_date='$t_date_1' order by random() " > $SPOOLPATH/id_$t_date_1
		
		if [[ $? -ne 0 ]]
		then
		
				error_fun "6" "Unable to pull delivered emails from postback table"
				exit
		
		fi		
		
		paste -d'|' $SPOOLPATH/TimeStamps.txt $SPOOLPATH/id_$t_date_1 >> $SPOOLPATH/combine_timestamps
		
		
		rm $SPOOLPATH/TimeStamps.txt
	
	
	done <$timestamp_input
	
	
	
	#==== LOADING TIMESTAMPS TO TABLE ===#
	
	
	$CONNECTION_STRING -vv -c "create table $REPLACE_TIMESTAMP_TABLE(timestamp varchar,id bigint) "

	if [[ $? -ne 0 ]]
	then
	
			error_fun "6" "Unable to create replacing timestamp table"
			exit
	
	fi
		
	$CONNECTION_STRING -vv -c "\copy  $REPLACE_TIMESTAMP_TABLE from '$SPOOLPATH/combine_timestamps' with delimiter '|' "

	if [[ $? -ne 0 ]]
	then
	
			error_fun "6" "Unable to load data into replacing timestamp table"
			exit
	
	fi	
	
	
	#==== update ips matching with used ip table ====#
	
	$CONNECTION_STRING -qtAX -c " update $PB_TABLE a set timestamp=b.timestamp from $REPLACE_TIMESTAMP_TABLE b where a.id=b.id"
	
	if [[ $? -ne 0 ]]
	then
	
			error_fun "6" "Unable to update delivered timestamps"
			exit
	
	fi


else

	error_fun "6" "delivered timestamp file doesnt exist in the path"
	exit

fi


	$CONNECTION_STRING  -vv -c "vacuum analyze $PB_TABLE"


