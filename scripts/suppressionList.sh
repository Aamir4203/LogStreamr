
#/bin/bash

source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE3 Start Time: `date`"



#==== ERROR FUNCTION ===#

error_fun()
{


    echo "ERROR: $2" >> $LOGPATH
    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1  ,request_end_time=now(), REQUEST_DESC='$2' where REQUEST_ID=$REQUEST_ID"

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

    sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"
	exit
    
}


request_status=`$CONNECTION_STRING -qtAX -c "select upper(request_status) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`





$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Preparing suppresssion data' WHERE REQUEST_ID=$REQUEST_ID "

supp_path=`$CONNECTION_STRING -qtAX -c "select SUPP_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


if [[ $supp_path != '' ]]
then

dos2unix $supp_path

email_cnt=`head -5 $supp_path | grep '@' | wc -l`


if [[ $email_cnt -gt 0 ]]
then


        $CONNECTION_STRING -vv -c "create table $SUPP_TABLE(EMAIL VARCHAR)"
        $CONNECTION_STRING -vv -c "\copy $SUPP_TABLE from '$supp_path'"
        if [[ $? -ne 0 ]]
        then

				error_fun "3" "Unable to copy suppression data file to the table"
                exit


        else

                $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set  REQUEST_DESC='SuppressionTable Created' where REQUEST_ID=$REQUEST_ID "

	fi
	
	supp_index="supp_email_$REQUEST_ID"
	$CONNECTION_STRING -vv -c "create index $supp_index on $SUPP_TABLE(EMAIL)"

        #suppressed_cnt=`$CONNECTION_STRING -qtAX -c "with cte as (delete from $TRT_TABLE a using $SUPP_TABLE b where a.email=b.email returning 1) select count(*) from cte"`
        
	query="delete from $TRT_TABLE a using $SUPP_TABLE b where a.email=b.email "
	suppressed_cnt=$(python3 $SCRIPTPATH/delete_partitions.py "$query")
	
	
	if [[ $? -ne 0 ]]
        then

			error_fun "3" "Unable to perform suppression to the TRT"
            exit
        else

                $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set   ERROR_CODE=0,REQUEST_STATUS='R',REQUEST_DESC='Client Suppression Performed' where REQUEST_ID=$REQUEST_ID "
                $CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET SUPPRESSION_COUNT='$suppressed_cnt' WHERE REQUEST_ID=$REQUEST_ID "

        fi

else


        $CONNECTION_STRING -vv -c "create table $SUPP_TABLE(MD5HASH VARCHAR)"
        $CONNECTION_STRING -vv -c "\copy $SUPP_TABLE from '$supp_path'"
        if [[ $? -ne 0 ]]
        then
				
			error_fun "3" "Unable to copy suppression data to TRT on Md5hash"
            exit


        else

                $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set  REQUEST_DESC='SuppressionTable Created' where REQUEST_ID=$REQUEST_ID"

        fi
	supp_index="supp_md5_$REQUEST_ID"
	$CONNECTION_STRING -vv -c "create index $supp_index on $SUPP_TABLE(MD5HASH)"

	echo "MODULE3: SUPPRESSION START TIME: `date`"
        #suppressed_cnt=`$CONNECTION_STRING -qtAX -c "SET enable_seqscan TO off;with cte as (delete from $TRT_TABLE a using $SUPP_TABLE b where a.md5hash=b.md5hash returning 1) select count(*) from cte"`
	
	query="delete from $TRT_TABLE a using $SUPP_TABLE b where a.md5hash=b.md5hash "
	suppressed_cnt=$(python3 $SCRIPTPATH/delete_partitions.py "$query")

		
        if [[ $? -ne 0 ]]
        then

			error_fun "3" "Unable to perform suppression to the TRT on Md5hash"
            exit


        else

                $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set   ERROR_CODE=0,REQUEST_STATUS='R',REQUEST_DESC='Client Suppressions Performed.' where REQUEST_ID=$REQUEST_ID "
				
                $CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET SUPPRESSION_COUNT=$suppressed_cnt WHERE REQUEST_ID=$REQUEST_ID "

        fi

	echo "MODULE3: SUPPRESSION END TIME: `date`"

fi

fi

sh -x $SCRIPTPATH/srcPreparation.sh  $REQUEST_ID >>$HOMEPATH/LOGS/$REQUEST_ID.log 2>>$HOMEPATH/LOGS/$REQUEST_ID.log


