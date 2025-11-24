source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE5 Start Time: `date`"



#==== ERROR FUNCTION ===#

error_fun()
{

    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1  ,request_end_time=now(), REQUEST_DESC='$2' where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set execution_time = TO_CHAR(AGE(request_end_time,request_start_time), 'HH24:MI:SS') where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,execution_time EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT,LAST_WK_DEL_INSERT_CNT,LAST_WK_UNSUB_INSERT_CNT,UNIQUE_DELIVERED_COUNT,NEW_RECORD_CNT from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
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
    ( '<b>LAST_WK_DEL_INSERT_CNT</b>', LAST_WK_DEL_INSERT_CNT::text ),
    ( '<b>LAST_WK_UNSUB_INSERT_CNT</b>', LAST_WK_UNSUB_INSERT_CNT::text ),
    ( '<b>UNIQUE_DELIVERED_COUNT</b>', UNIQUE_DELIVERED_COUNT::text ),
    ( '<b>NEW_RECORD_CNT</b>', NEW_RECORD_CNT::text )) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv

    sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"
        exit

}




report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


subseg=`$CONNECTION_STRING -qtAX -c "select UPPER(SUB_SEG) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

request_status=`$CONNECTION_STRING -qtAX -c "select upper(request_status) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

if [[ $request_status == 'RE' ]]
then

$CONNECTION_STRING -vv -c "create table $REPORT_TABLE(CAMPAIGN VARCHAR , DEL_DATE VARCHAR , DEL_COUNT int , OPEN_COUNT int , CLICK_COUNT int ,UNSUB_COUNT int , SOFT_COUNT int , HARD_COUNT int , SUBJECT VARCHAR , CREATIVE VARCHAR ,CREATIVEID VARCHAR , OFFERID VARCHAR , SEGMENT VARCHAR ,SUB_SEG VARCHAR)"

if [[ $subseg == 'Y' ]]
then

        $CONNECTION_STRING -vv -c "\copy $REPORT_TABLE from '$report_path' with delimiter '|'"

        if [[ $? -ne 0 ]]
        then

                error_fun "5" "Unable to load data into report table"
                                exit

        fi

else

        $CONNECTION_STRING -vv -c "\copy $REPORT_TABLE(CAMPAIGN,DEL_DATE,DEL_COUNT,OPEN_COUNT,CLICK_COUNT,UNSUB_COUNT,SOFT_COUNT,HARD_COUNT,SUBJECT,CREATIVE,CREATIVEID,OFFERID,SEGMENT) from '$report_path' with delimiter '|'"

        if [[ $? -ne 0 ]]
        then
                error_fun "5" "Unable to load data into report table"
                                exit

        fi

fi

fi

#=== PARTITIONING SOURCE TABLE ON DELIVERED DATE ===#

                  

                  trt_header_1=`$CONNECTION_STRING  --pset footer  -qAX -c "select * from $TRT_TABLE limit 1" | head -1 | sed 's/|/ varchar,/g'`


                  $CONNECTION_STRING -vv -c "create table $PARTITION_SRC ($trt_header_1 varchar,status int default 0,unsub int default 0,freq int default 1,flag varchar,del_date varchar,id bigint,touch int default 1) PARTITION BY LIST(del_date)"

                  if [[ $? -ne 0 ]]
                  then

                          error_fun "5" "Unable to create source partition table"
                          exit
                  fi

                  $CONNECTION_STRING -qtAX -c "select distinct del_date from $REPORT_TABLE "  > $SPOOLPATH/uniq_deldates


                  while read date_
                  do
                          date1=`echo $date_ | sed 's/-//g'`

                          part_src_table=$PARTITION_SRC\_$date1

                          $CONNECTION_STRING -vv -c "create table $part_src_table PARTITION OF $PARTITION_SRC FOR VALUES IN ('$date_')"

                          if [[ $? -ne 0 ]]
                          then

                                  error_fun "5" "Unable to perform partitioning on SRC table"
                                  exit

                          fi

                  done <$SPOOLPATH/uniq_deldates


		                  #+== INSERT SRC INTO PARTITION SOURCE TABLE ===#

                  $CONNECTION_STRING -vv -c "insert into $PARTITION_SRC select * from $SRC_TABLE "

                  if [[ $? -ne 0 ]]
                  then

                          error_fun "5" "Unable to insert source into Partitioned source table"
                          exit

                  fi



                  #==== INDEXING ON PARTITIONED TABLES ====#

while read dates
do

        date_=`echo $dates | sed 's/-//g'`
        date_table=$PB_TABLE\_$date_

		$CONNECTION_STRING -vv -c "drop table if exists $date_table "
		
		if [[ $? -ne 0 ]]
		then

			error_fun "5" "Unable to drop  partitioned delivered table"
			exit

		fi		
		
		
done <$SPOOLPATH/uniq_deldates

               while read date_
                 do

                         date1=`echo $date_ | sed 's/-//g'`
                         part_src_table=$PARTITION_SRC\_$date1
                         index_name=$part_src_table\_id
                         index_name2=$part_src_table\_combine

                         $CONNECTION_STRING -vv -c "create index $index_name on $part_src_table(id)"

                         if [[ $? -ne 0 ]]
                         then

                                 error_fun "5" "Unable to create index on ID to partitioned source table"
                                 exit

                         fi

				if [[ $subseg == 'Y' ]]
				then
				
					seg_var=" segment,subseg "
				
				else
				
					seg_var=" segment  "
	
	
				fi
				$CONNECTION_STRING -vv -c "create index $index_name2 on $part_src_table($seg_var,status,unsub,flag)"
	
				if [[ $? -ne 0 ]]
				then
	
					error_fun "5" "Unable to create combine index  to partitioned source table"
					exit
	
				fi

			done <$SPOOLPATH/uniq_deldates
				
				
				
