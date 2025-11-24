source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE4 Start Time: `date`"



#==== ERROR FUNCTION ===#

error_fun()
{
    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1  ,request_end_time=now(), REQUEST_DESC='$2' where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set execution_time = TO_CHAR(AGE(request_end_time,request_start_time), 'HH24:MI:SS') where REQUEST_ID=$REQUEST_ID"

    $CONNECTION_STRING  --no-align --field-separator '|'  --pset footer -qtAX -c "with cte as (select a.REQUEST_ID,CLIENT_NAME,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,REQUEST_START_TIME,execution_time EXECUTION_TIME,POSTED_UNSUB_HARDS_SUPP_COUNT,OFFERID_UNSUB_SUPP_COUNT OFFERID_SUPPRESSED_COUNT,SUPPRESSION_COUNT CLIENT_SUPPRESSION_COUNT,MAX_TOUCH_COUNT,LAST_WK_DEL_INSERT_CNT,LAST_WK_UNSUB_INSERT_CNT,UNIQUE_DELIVERED_COUNT,NEW_RECORD_CNT,ADDED_BY from $REQUEST_TABLE a join $CLIENT_TABLE b on a.CLIENT_ID=b.CLIENT_ID  join $QA_TABLE c on a.REQUEST_ID=c.REQUEST_ID where  a.REQUEST_ID=$REQUEST_ID) select x.* from cte cross join lateral (values
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
    ( '<b>NEW_RECORD_CNT</b>', NEW_RECORD_CNT::text ),
    ( '<b>ADDED_BY</b>', ADDED_BY::text)) x(Header, Value)" >$SPOOLPATH/fetchRequestDetails.csv

    sh $SCRIPTPATH/sendMail.sh "$REQUEST_ID"
        exit

}



$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE SET REQUEST_DESC='Preparing Source' WHERE REQUEST_ID=$REQUEST_ID "

trt_header=`$CONNECTION_STRING  --pset footer  -qAX -c "select * from $TRT_TABLE limit 1" | head -1 | sed 's/|/,/g'`


OLD_DATA=`$CONNECTION_STRING -qtAX -c "select upper(TOTAL_DELIVERED_TABLE) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.client_id=b.client_id where request_id=$REQUEST_ID"`

LAST_WK_PB_TABLE=`$CONNECTION_STRING -qtAX -c "select upper(PREV_WEEK_PB_TABLE) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.client_id=b.client_id where request_id=$REQUEST_ID"`



subseg=`$CONNECTION_STRING -qtAX -c "select UPPER(SUB_SEG) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

priority=`$CONNECTION_STRING -qtAX -c "select UPPER(DATA_PRIORITY) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


on_sent=`$CONNECTION_STRING -qtAX -c "select upper(ON_SENT) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

bounce_as_delivered=`$CONNECTION_STRING -qtAX -c "select upper(INCLUDE_BOUNCE_AS_DELIVERED) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


request_status=`$CONNECTION_STRING -qtAX -c "select upper(request_status) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

report_path=`$CONNECTION_STRING -qtAX -c "select CPM_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

decile_file=`$CONNECTION_STRING -qtAX -c "select DECILE_WISE_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


if [[ $REQUEST_TYPE == 1 ]]
then

unique_decile_file=`$CONNECTION_STRING -qtAX -c "select DECILE_WISE_REPORT_PATH from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

elif [[ $REQUEST_TYPE == 2 ]]
then

unique_decile_file=`$CONNECTION_STRING -qtAX -c "select unique_decile_report_path from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


fi




#=== FOR VERIZON ===#


ip_append=`$CONNECTION_STRING -qtAX -c "select upper(IP_APPEND) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

posted_unsub_table=`$CONNECTION_STRING -qtAX -c "select upper(POSTED_UNSUB_HARDS_TABLE) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.CLIENT_ID=b.CLIENT_ID where REQUEST_ID=$REQUEST_ID"`

OLD_DATA=`$CONNECTION_STRING -qtAX -c "select upper(TOTAL_DELIVERED_TABLE) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.client_id=b.client_id where request_id=$REQUEST_ID"`

LAST_WK_PB_TABLE=`$CONNECTION_STRING -qtAX -c "select upper(PREV_WEEK_PB_TABLE) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.client_id=b.client_id where request_id=$REQUEST_ID"`

client_name=`$CONNECTION_STRING -qtAX -c "select upper(CLIENT_NAME) from $CLIENT_TABLE a join $REQUEST_TABLE b on a.client_id=b.client_id where request_id=$REQUEST_ID"`

if [[ $client_name == 'VERIZON' ]]
then
	uniq_key="email,del_date,segment"
	select_ver=" distinct "
        #$CONNECTION_STRING -vv -c "truncate table $LAST_WK_PB_TABLE"

        $CONNECTION_STRING -vv -c "truncate table $OLD_DATA"

        $CONNECTION_STRING -vv -c "truncate table $posted_unsub_table"

else

	select_ver=" "
	uniq_key="email,del_date"

fi



#=== INSERT LAST WEEK UNSUBS ===#


#=== UNSUBS INSERT ===#

inserted_unsub_cnt=`$CONNECTION_STRING -qtAX -c " with cte as (select email,segment,del_date,unsub_date from $LAST_WK_PB_TABLE where unsub_date is not null ) , rows as (insert into $posted_unsub_table(email,segment,del_date,unsub_date) select * from cte on conflict do nothing returning 1) select count(*) from rows"`

if [[ $? -ne 0 ]]
then

error_fun "4" "Unable to insert last week unsubs into posted unsub table"
exit

fi


$CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET LAST_WK_UNSUB_INSERT_CNT=$inserted_unsub_cnt WHERE REQUEST_ID=$REQUEST_ID "



#== HARDS INSERTS ===#

$CONNECTION_STRING -vv -c " with cte as (select email,segment,del_date,flag from $LAST_WK_PB_TABLE where flag='B') insert into $posted_unsub_table(email,segment,del_date,flag) select * from cte on conflict do nothing"

if [[ $? -ne 0 ]]
then

error_fun "4" "Unable to insert last week hards into posted unsub table"
exit

fi





#==== POSTED UNSUBS AND HARDS SUPPRESSION ====#

echo "POSTED UNSUB SUPP START TIME: `date`"

#unsubs_supp_cnt=`$CONNECTION_STRING -qtAX -c "SET enable_seqscan TO off;with cte as (delete from $TRT_TABLE a using $posted_unsub_table b where a.email=b.email returning 1 ) select count(*) from cte"`
query="delete from $TRT_TABLE a using $posted_unsub_table b where a.email=b.email "
unsubs_supp_cnt=$(python3 $SCRIPTPATH/delete_partitions.py "$query")

if [[ $? -ne 0 ]]
then

        error_fun "4" "Unable to perform posted unsubs/hards suppression to the TRT"
        exit
else

        $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_DESC='Mailer Unsubs/Hards Suppressed.' where REQUEST_ID=$REQUEST_ID  "

        $CONNECTION_STRING -vv -c "update $QA_TABLE set  POSTED_UNSUB_HARDS_SUPP_COUNT=$unsubs_supp_cnt where REQUEST_ID=$REQUEST_ID"

fi

echo "POSTED UNSUB SUPP END TIME: `date`"

if [[ $client_name == 'VERIZON' ]]
then

        #$CONNECTION_STRING -vv -c "truncate table $LAST_WK_PB_TABLE"

        $CONNECTION_STRING -vv -c "truncate table $OLD_DATA"

        $CONNECTION_STRING -vv -c "truncate table $posted_unsub_table"

fi



#===== CHECK FOR BOUNCE AS DELIVERED INCLUSION ====#

if [[ $bounce_as_delivered == 'Y' ]]
then

        include_hards_status=" where "

else

        include_hards_status=" left join $HARDS_TABLE h on a.email=h.email where h.email is null and  "

fi




#=== INSERT LAST WEEK DEIVERED DATA TO TOTAL DELIVERED TABLE ===#

inserted_del_count=`$CONNECTION_STRING -qtAX -c " with cte as (select distinct a.email,a.segment from $LAST_WK_PB_TABLE a left join $OLD_DATA b on a.email=b.email where b.email is null ) , rows as ( insert into $OLD_DATA(email,segment) select distinct email,segment from cte on conflict do nothing returning 1) select count(*) from rows"`

if [[ $? -ne 0 ]]
then

        error_fun "4" "Unable to insert last week delivered into total delivered table"
        exit

fi

$CONNECTION_STRING -vv -c "vacuum analyze $OLD_DATA"
$CONNECTION_STRING -vv -c "vacuum analyze $TRT_TABLE"

$CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET LAST_WK_DEL_INSERT_CNT=$inserted_del_count WHERE REQUEST_ID=$REQUEST_ID "


$CONNECTION_STRING -vv -c "create table $SRC_TABLE (like $TRT_TABLE)"

if [[ $client_name == 'VERIZON' ]]
then
	$CONNECTION_STRING -vv -c "alter table $SRC_TABLE add status int default 0,add unsub int default 0,add freq int default 1,add flag varchar,add del_date varchar,add id serial primary key,add touch int default 1,add unique(email,segment); comment on column $SRC_TABLE.status is '-1 - Bounce, 0 - Random, 1 - Genuine, 2 - Open, 3 - Click' "

	src_idx_key_ver=src_email_idx_$REQUEST_ID
	$CONNECTION_STRING -vv -c "create index $src_idx_key_ver on $SRC_TABLE(email)"

else

	$CONNECTION_STRING -vv -c "alter table $SRC_TABLE add status int default 0,add unsub int default 0,add freq int default 1,add flag varchar,add del_date varchar,add id serial primary key,add touch int default 1,add unique(email); comment on column $SRC_TABLE.status is '-1 - Bounce, 0 - Random, 1 - Genuine, 2 - Open, 3 - Click' "
fi


#== GENUINE DELIVERED INSERT===#

$CONNECTION_STRING -vv -c " with cte as ( select a.*,b.status,b.unsub,del_date from $TRT_TABLE a join $UNIQ_GEN_TABLE b on a.email=b.email ) insert into $SRC_TABLE($trt_header,status,unsub,del_date) select * from cte on conflict do nothing"

if [[ $? -ne 0 ]]
then

        error_fun "4" "Unable to insert genuine delivered data."
        exit

fi


$CONNECTION_STRING -vv -c "update $SRC_TABLE c set del_date=null where id in (select a.id from $SRC_TABLE a left join $REPORT_TABLE b on a.del_date=b.del_date and a.segment=b.segment and a.subseg=b.sub_seg where b.del_date is null and b.segment is null and b.sub_seg is null)  "

#=== INSERT GENUINE BOUNCES INTO HARDS TABLE ===#

#$CONNECTION_STRING -vv -c "  insert into $HARDS_TABLE(email) select email from $UNIQ_GEN_TABLE where status=-1 on conflict do nothing "



#==== UPDATE FREQUENCY WITH OLD DELIVERED DATA ===#

$CONNECTION_STRING -vv -c " with cte as ( select a.id from $SRC_TABLE a join $OLD_DATA b on a.email=b.email ) update $SRC_TABLE x set freq=0 from cte y where x.id=y.id "

if [[ $? -ne 0 ]]
then

        error_fun "4" "Unable to update frequency to SRC table."
        exit

fi

#=== PULL SOURCE INITIAL STATS AND INSERT DEFICITS AS PER PRIORITY ===#

if [[ $priority == 'Y' ]]
then

        priority_order=" a.priority,"

else

        priority_order=" "

fi


$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE set REQUEST_STATUS='R',REQUEST_DESC='Preparing Source.' where REQUEST_ID=$REQUEST_ID "

#============SOURCE PREPARATION SEGMENT-SUBSEGMENT WISE=======================================#



        if [[ $on_sent == 'Y' ]]
        then

                #====== HARDS SETUP ======#


                $CONNECTION_STRING -qtAX -c "select sum(HARD_COUNT) cnt,segment,sub_seg from $REPORT_TABLE group by 2,3 order by 2,3 "> $SPOOLPATH/total_hards

                if [[ $? -ne 0 ]]
                then

                        error_fun "4" "Unable to pull stats for hards from report table."
                        exit

                fi

                $CONNECTION_STRING -vv -c " update $SRC_TABLE set del_date=null,flag='B' where status=-1"


                while read hards
                do

                        hard_cnt=`echo $hards | cut -d'|' -f1`
                        hard_seg=`echo $hards | cut -d'|' -f2`
                        hard_subseg=`echo $hards | cut -d'|' -f3`


                        avl_hard_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE WHERE status=-1 and segment='$hard_seg' and subseg='$hard_subseg' "`

                        req_cnt=`echo $hard_cnt-$avl_hard_cnt | bc`

                        if [[ $req_cnt -gt 0 ]]
                        then

                                $CONNECTION_STRING -vv -c "with cte as ( select a.*,(case when c.email is not null then 0 else 1 end) trt_freq,-1,'B' from $TRT_TABLE a left join $SRC_TABLE b on a.email=b.email left join $OLD_DATA c on a.email=c.email join  $HARDS_TABLE d on a.email=d.email where c.email is null and b.email is null and a.segment='$hard_seg' and a.subseg='$hard_subseg' order by random() ) insert into $SRC_TABLE($trt_header,freq,status,flag) select  * from cte limit $req_cnt on conflict do nothing"

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to insert hards matching with hards table"
                                        exit

                                fi

                        else

                                req_cnt1=`echo $req_cnt | sed 's/-//g'`

                                $CONNECTION_STRING -vv -c "with cte as ( select id from $SRC_TABLE where status=-1 and segment='$hard_seg' and subseg='$hard_subseg' order by random() limit $req_cnt1) delete from $SRC_TABLE a using cte b where a.id=b.id"

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to delete excess hards."
                                        exit

                                fi

                        fi

                        #=== FILLING STILL DEFICIT ===#

                        avl_hard_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE WHERE status=-1 and segment='$hard_seg' and subseg='$hard_subseg' "`

                        req_cnt=`echo $hard_cnt-$avl_hard_cnt | bc`

                        if [[ $req_cnt -gt 0 ]]
                        then

                                $CONNECTION_STRING -vv -c "with cte as ( select a.*,(case when c.email is not null then 0 else 1 end) trt_freq,-1,'B' from $TRT_TABLE a left join $SRC_TABLE b on a.email=b.email left join $OLD_DATA c on a.email=c.email  where  b.email is null and a.segment='$hard_seg' and a.subseg='$hard_subseg' order by random() ) insert into $SRC_TABLE($trt_header,freq,status,flag) select * from cte order by trt_freq desc limit $req_cnt on conflict do nothing"

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to insert hards from TRT"
                                        exit

                                fi

                        fi
                done <$SPOOLPATH/total_hards



                #=== UPDATE DELDATE TO  HARD BOUNCE DATA  ===#

                $CONNECTION_STRING -qtAX -c "select sum(HARD_COUNT) cnt,sum(soft_count) softs,segment,sub_seg,del_date from $REPORT_TABLE group by 3,4,5 order by 3,4,5 "> $SPOOLPATH/total_hards_del_date


                while read hard_date
                do

                        hard_cnt=`echo $hard_date | cut -d'|' -f1`
                        soft_cnt=`echo $hard_date | cut -d'|' -f2`
                        hard_seg=`echo $hard_date | cut -d'|' -f3`
                        hard_subseg=`echo $hard_date | cut -d'|' -f4`
                        hard_deldate=`echo $hard_date | cut -d'|' -f5`


                        #==== HARDS UPDATE ===#

                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where status=-1 and flag='B' and del_date is null and segment='$hard_seg' and subseg='$hard_subseg' order by random() limit $hard_cnt) update $SRC_TABLE a set del_date='$hard_deldate' from cte b where a.id=b.id"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to update hards delivered date"
                                exit

                        fi




                done <$SPOOLPATH/total_hards_del_date


        else

                $CONNECTION_STRING -vv -c "delete from $SRC_TABLE where status=-1"

        fi



        #==== FREQUENCY WISE DEFICIT INSERTS =====#

        #old_per=`$CONNECTION_STRING -qtAX -c " select OLD_DELIVERED_PER from $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


        while read decile_stats
        do
                cpm_sent=`echo $decile_stats | cut -d'|' -f1`
                cpm_seg=`echo $decile_stats | cut -d'|' -f5`
                cpm_subseg=`echo $decile_stats | cut -d'|' -f6`
                cpm_decile=`echo $decile_stats | cut -d'|' -f7`
		old_per=`echo $decile_stats | cut -d'|' -f8`

                total_old=`echo $old_per*$cpm_sent/100 | bc`

                avl_old=`$CONNECTION_STRING -qtAX -c " select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and freq=0"`

                if [[ $total_old -gt $avl_old ]]
                then

                        req_old=`echo $total_old-$avl_old | bc`

                        #$CONNECTION_STRING -vv -c " with cte as ( select a.*,(case when c.email is not null then 0 else 1 end) trt_freq from $TRT_TABLE a left join $SRC_TABLE b on a.email=b.email left join $OLD_DATA c on a.email=c.email $include_hards_status b.email is null and a.segment='$cpm_seg' and a.subseg='$cpm_subseg' and a.decile='$cpm_decile'  order by trt_freq , $priority_order random() ) insert into $SRC_TABLE($trt_header,freq) select $select_ver * from cte limit $req_old"
			$CONNECTION_STRING -vv -c "WITH candidates AS (SELECT a.*, CASE WHEN c.email IS NOT NULL THEN 0 ELSE 1 END AS trt_freq FROM $TRT_TABLE a LEFT JOIN $OLD_DATA c ON a.email = c.email $include_hards_status NOT EXISTS (SELECT 1 FROM $SRC_TABLE b WHERE b.email = a.email) AND a.segment = '$cpm_seg' AND a.subseg = '$cpm_subseg' AND a.decile = '$cpm_decile'), ranked AS (SELECT * FROM candidates a ORDER BY trt_freq , $priority_order RANDOM()) INSERT INTO $SRC_TABLE($trt_header, freq) SELECT $select_ver * FROM ranked LIMIT $req_old;"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to insert freq wise deficit inserts."
                                exit

                        fi

                        total_avl=`$CONNECTION_STRING -qtAX -c " select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile'"`

                        req_new=`echo $cpm_sent-$total_avl | bc`

                        if [[ $req_new -gt 0 ]]
                        then

                                echo "SRC Decile Wise, Frequency wise insert Start time: `date`"

                                #$CONNECTION_STRING -vv -c " with cte as ( select a.*,(case when c.email is not null then 0 else 1 end) trt_freq from $TRT_TABLE a left join $SRC_TABLE b on a.email=b.email  left join $OLD_DATA c on a.email=c.email $include_hards_status b.email is null and a.segment='$cpm_seg' and a.subseg='$cpm_subseg' and a.decile='$cpm_decile' order by trt_freq desc , $priority_order random() ) insert into $SRC_TABLE($trt_header,freq) select $select_ver * from cte limit $req_new "
				$CONNECTION_STRING -vv -c "WITH candidates AS (SELECT a.*, CASE WHEN c.email IS NOT NULL THEN 0 ELSE 1 END AS trt_freq FROM $TRT_TABLE a LEFT JOIN $OLD_DATA c ON a.email = c.email $include_hards_status NOT EXISTS (SELECT 1 FROM $SRC_TABLE b WHERE b.email = a.email) AND a.segment = '$cpm_seg' AND a.subseg = '$cpm_subseg' AND a.decile = '$cpm_decile'), ranked AS (SELECT * FROM candidates a ORDER BY trt_freq DESC, $priority_order RANDOM()) INSERT INTO $SRC_TABLE($trt_header, freq) SELECT $select_ver * FROM ranked LIMIT $req_new;"
                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to insert freq wise deficit inserts."
                                exit

                        fi

                                echo "SRC Decile Wise, Frequency wise insert End time: `date`"

                        elif [[ $req_new -lt 0 ]]
                        then

                                req_new_1=`echo $req_new | sed 's/-//g'`

                                $CONNECTION_STRING -vv -c " with cte as ( select id from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and status in (0,1,2,3) and flag is null order by freq desc,status,random() limit $req_new_1 ) delete from $SRC_TABLE a using cte b where a.id=b.id "

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to insert freq wise deficit inserts."
                                        exit

                                fi

                        fi

                elif [[ $total_old -lt $avl_old ]]
                then

                                                extra_gen=`expr $avl_old - $total_old | bc `

                         $CONNECTION_STRING -vv -c " with cte as ( select id from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and status in (0,1,2,3) and flag is null order by status,freq , random() limit $extra_gen ) delete from $SRC_TABLE a using cte b where a.id=b.id "

                         if [[ $? -ne 0 ]]
                         then

                                 error_fun "4" "Unable to delete extra old records from source"
                                 exit

                         fi


                        total_avl=`$CONNECTION_STRING -qtAX -c " select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile'"`

                        req_cnt=`echo $cpm_sent-$total_avl | bc `

                        if [[ $req_cnt -gt 0 ]]
                        then

                                #$CONNECTION_STRING -vv -c " with cte as ( select a.*,(case when c.email is not null then 0 else 1 end) trt_freq from $TRT_TABLE a left join $SRC_TABLE b on a.email=b.email left join $OLD_DATA c on a.email=c.email $include_hards_status b.email is null and a.segment='$cpm_seg' and a.subseg='$cpm_subseg' and a.decile='$cpm_decile'  order by trt_freq desc, $priority_order random() ) insert into $SRC_TABLE($trt_header,freq) select $select_ver * from cte limit $req_cnt"
				$CONNECTION_STRING -vv -c "WITH candidates AS (SELECT a.*, CASE WHEN c.email IS NOT NULL THEN 0 ELSE 1 END AS trt_freq FROM $TRT_TABLE a LEFT JOIN $OLD_DATA c ON a.email = c.email $include_hards_status NOT EXISTS (SELECT 1 FROM $SRC_TABLE b WHERE b.email = a.email) AND a.segment = '$cpm_seg' AND a.subseg = '$cpm_subseg' AND a.decile = '$cpm_decile'), ranked AS (SELECT * FROM candidates a ORDER BY trt_freq DESC, $priority_order RANDOM()) INSERT INTO $SRC_TABLE($trt_header, freq) SELECT $select_ver * FROM ranked LIMIT $req_cnt;"
                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to insert freq wise deficit inserts."
                                        exit

                                fi


                        else

                                req_cnt_1=`echo $req_cnt | sed 's/-//g'`

                                $CONNECTION_STRING -vv -c " with cte as ( select id from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and status in (0,1,2,3) and flag is null order by freq desc,status, random() limit $req_cnt_1 ) delete from $SRC_TABLE a using cte b where a.id=b.id "

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to insert freq wise deficit inserts."
                                        exit

                                fi

                        fi
                fi

		$CONNECTION_STRING -vv -c "vacuum analyze $SRC_TABLE"
        done <$unique_decile_file


        $CONNECTION_STRING -qtAX -c "select sum(DEL_COUNT) cnt,sum(UNSUB_COUNT) unsub_cnt,sum(SOFT_COUNT) softs,segment,sub_seg,del_date from $REPORT_TABLE group by 4,5,6 order by 4,5,6"> $SPOOLPATH/deldate_counts



        #=== UPDATE DEL_DATES TO NULL FOR EXTRA DATA ====#


        while read read_del_cnt
        do

                del_cnt=`echo $read_del_cnt | cut -d'|' -f1`
                unsub_cnt=`echo $read_del_cnt | cut -d'|' -f2`
                soft_cnt=`echo $read_del_cnt | cut -d'|' -f3`
                del_seg=`echo $read_del_cnt | cut -d'|' -f4`
                del_subseg=`echo $read_del_cnt | cut -d'|' -f5`
                del_deldate=`echo $read_del_cnt | cut -d'|' -f6`

                avl_del_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' " `

                req_del_cnt=`echo $del_cnt-$avl_del_cnt | bc`

                if [[ $req_del_cnt -lt 0 ]]
                then

                    req_del_cnt_1=`echo $req_del_cnt | sed 's/-//g'`

                    $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and status in (0,1,2,3)  and flag is null order by status,random() limit $req_del_cnt_1) update $SRC_TABLE a set del_date=null from cte b where a.id=b.id"

                    if [[ $? -ne 0 ]]
                    then

                            error_fun "4" "Unable to update deldate"
                            exit

                    fi

                fi

        done <$SPOOLPATH/deldate_counts

        #=== UPDATE DELIVERED DATES ,SOFTS & UNSUBS ====#

        while read read_del_cnt
        do

                del_cnt=`echo $read_del_cnt | cut -d'|' -f1`
                unsub_cnt=`echo $read_del_cnt | cut -d'|' -f2`
                soft_cnt=`echo $read_del_cnt | cut -d'|' -f3`
                del_seg=`echo $read_del_cnt | cut -d'|' -f4`
                del_subseg=`echo $read_del_cnt | cut -d'|' -f5`
                del_deldate=`echo $read_del_cnt | cut -d'|' -f6`


                 #=== DELDATE UPDATE ====#


                avl_del_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' " `

                req_del_cnt=`echo $del_cnt-$avl_del_cnt | bc`

                if [[ $req_del_cnt -gt 0 ]]
                then

                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where del_date is null and segment='$del_seg' and subseg='$del_subseg' order by random() limit $req_del_cnt) update $SRC_TABLE a set del_date='$del_deldate' from cte b where a.id=b.id"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to update deldate"
                                exit

                        fi


                else

                        req_del_cnt_1=`echo $req_del_cnt | sed 's/-//g'`

                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and status in (0,1,2,3)  and flag is null order by status,random() limit $req_del_cnt_1) update $SRC_TABLE a set del_date=null from cte b where a.id=b.id"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to update deldate"
                                exit

                        fi

                fi


                        #=== UNSUB UPDATE ====#


                avl_unsub_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=1 " `

                req_unsub_cnt=`echo $unsub_cnt-$avl_unsub_cnt | bc`

                if [[ $req_unsub_cnt -gt 0 ]]
                then

                        $CONNECTION_STRING -vv -c "with cte as (select x.id from $SRC_TABLE x join $UNSUBS_TABLE y on x.email=y.email  where x.del_date='$del_deldate' and x.segment='$del_seg' and x.subseg='$del_subseg' and x.unsub=0 and x.flag is null order by x.status,random() limit $req_unsub_cnt) update $SRC_TABLE a set status=2,unsub=1 from cte b where a.id=b.id"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to update unsubs"
                                exit

                        fi

                else

                        req_unsub_cnt_1=`echo $req_unsub_cnt | sed 's/-//g'`

                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=1 order by random() limit $req_unsub_cnt_1) update $SRC_TABLE a set unsub=0,status=2 from cte b where a.id=b.id"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to update unsubs"
                                exit

                        fi

                fi

                avl_unsub_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=1 " `

                req_unsub_cnt=`echo $unsub_cnt-$avl_unsub_cnt | bc`

                if [[ $req_unsub_cnt -gt 0 ]]
                then

                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where  flag is null and del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=0 order by status,random() limit $req_unsub_cnt) update $SRC_TABLE a set status=2,unsub=1 from cte b where a.id=b.id"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to update unsubs"
                                exit

                        fi


                fi



                #=== SOFTS UPDATE ====#

                if [[ $on_sent == 'Y' ]]
                then

                        if [[ $soft_cnt -gt 0 ]]
                        then

                                $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where  flag is null and del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=0 order by status,random() limit $soft_cnt) update $SRC_TABLE a set flag='S' from cte b where a.id=b.id"

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to update softs"
                                        exit

                                fi


                        fi


                fi
		$CONNECTION_STRING -vv -c "vacuum analyze $SRC_TABLE"


        done <$SPOOLPATH/deldate_counts

                        $CONNECTION_STRING -vv -c "update $SRC_TABLE set status=0 where  flag ='S' "


                #================================TOUCH 2 CHECK ==================#


                $CONNECTION_STRING -qtAX -c "select sum(DEL_COUNT) cnt,segment,sub_seg from $REPORT_TABLE group by 2,3 order by 2,3"> $SPOOLPATH/segment_wise_cnts

                touch="1"

                while read check_touch
                do
					cpm_sent=`echo $check_touch | cut -d'|' -f1`
					cpm_seg=`echo $check_touch | cut -d'|' -f5`
					cpm_subseg=`echo $check_touch | cut -d'|' -f6`
					cpm_decile=`echo $check_touch | cut -d'|' -f7`

                     avl_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' " `

                     if [[ $cpm_sent -gt $avl_cnt ]]
                     then

                              (( result=(cpm_sent+avl_cnt-1)/avl_cnt ));

                              if [[ $result -gt $touch ]]
                              then
                                     touch=$result

                              fi


                     fi


                done <$decile_file

                #=== UPDATE TOUCH TO THE QA TABLE ====#

                $CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET MAX_TOUCH_COUNT=$touch WHERE REQUEST_ID=$REQUEST_ID "

                #================= NON-UNIQUE SOURCE BUILDING======================#

                touch_counter="2"

                while [[ $touch -gt 1 && $touch_counter -le $touch ]]
                do


                        if [[ $touch_counter -eq 2 ]]
                        then

                                UNIQ_SRC_TABLE=$SRC_TABLE\_UNIQ

                                $CONNECTION_STRING -vv -c " alter table $SRC_TABLE rename to $UNIQ_SRC_TABLE"

                                $CONNECTION_STRING -vv -c "create table $SRC_TABLE (like $TRT_TABLE)"

                                $CONNECTION_STRING -vv -c "alter table $SRC_TABLE add status int default 0,add unsub int default 0,add freq int default 1,add flag varchar,add del_date varchar,add id serial primary key,add touch int default 1; comment on column $SRC_TABLE.status is '-1 - Bounce, 0 - Random, 1 - Genuine, 2 - Open, 3 - Click' "
				if [[ $client_name == 'VERIZON' ]]
				then
                                	$CONNECTION_STRING -vv -c " alter table $SRC_TABLE add unique(email,del_date,segment) "

				else
					$CONNECTION_STRING -vv -c " alter table $SRC_TABLE add unique(email,del_date) "
				fi

                                $CONNECTION_STRING -vv -c " insert into  $SRC_TABLE($trt_header,status,unsub,freq,flag,del_date,touch) select $trt_header,status,unsub,freq,flag,del_date,touch from $UNIQ_SRC_TABLE "

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to insert into src table from unique src"
                                        exit

                                fi

                        #=== ADDDING INDEX TO SRC ====#
                        src_index_2=src_index_2_$REQUEST_ID

                        $CONNECTION_STRING -vv -c " create index $src_index_2 on $SRC_TABLE (del_date,segment,subseg)"


                        fi

                        prev_touch=`expr $touch_counter - 1 |bc`

                        $CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE set REQUEST_STATUS='R',REQUEST_DESC='Preparing NonUnique Source.' where REQUEST_ID=$REQUEST_ID "
                        #=== TOUCH DEFICIT INSERTS ===#


                        while read decile_stats
                        do
                                cpm_sent=`echo $decile_stats | cut -d'|' -f1`
                                cpm_seg=`echo $decile_stats | cut -d'|' -f5`
                                cpm_subseg=`echo $decile_stats | cut -d'|' -f6`
                                cpm_decile=`echo $decile_stats | cut -d'|' -f7`

                                avl_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg'  and decile='$cpm_decile' " `

                                if [[ $cpm_sent -gt $avl_cnt ]]
                                then

                                        req_cnt=`echo $cpm_sent-$avl_cnt | bc`

                                        if [[ $req_cnt -gt 0 ]]
                                        then

                                                $CONNECTION_STRING -vv -c " with cte as ( select $trt_header,0 status,unsub,freq from  $SRC_TABLE  where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and (flag is null or flag='S') and unsub=0 and touch=$prev_touch order by del_date,random()) insert into $SRC_TABLE($trt_header,status,unsub,freq,touch) select $trt_header,status,unsub,freq,$touch_counter from cte limit $req_cnt"

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to insert deficits into non-unique src table "
                                                        exit

                                                fi

                                        fi

                                fi
				$CONNECTION_STRING -vv -c "vacuum analyze $SRC_TABLE"



                        done <$decile_file

                        #==== TOUCH DEL_DATE,SOFTS & UNSUBS SETUP ===#


                        while read read_del_cnt
                        do

                                del_cnt=`echo $read_del_cnt | cut -d'|' -f1`
                                unsub_cnt=`echo $read_del_cnt | cut -d'|' -f2`
                                soft_cnt=`echo $read_del_cnt | cut -d'|' -f3`
                                del_seg=`echo $read_del_cnt | cut -d'|' -f4`
                                del_subseg=`echo $read_del_cnt | cut -d'|' -f5`
                                del_deldate=`echo $read_del_cnt | cut -d'|' -f6`


                                #=== DELDATE UPDATE ====#


                                avl_del_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' " `

                                req_del_cnt=`echo $del_cnt-$avl_del_cnt | bc`

                                if [[ $req_del_cnt -gt 0 ]]
                                then

                                        #$CONNECTION_STRING -vv -c "with cte as (select x.id from $SRC_TABLE x  left join (select email from $SRC_TABLE  where del_date='$del_deldate' ) y on x.email=y.email where y.email is null and x.del_date is null and x.segment='$del_seg' and x.subseg='$del_subseg' order by random() limit $req_del_cnt) update $SRC_TABLE a set del_date='$del_deldate' from cte b where a.id=b.id"
					$CONNECTION_STRING -vv -c " WITH cte AS (  SELECT x.id FROM $SRC_TABLE x WHERE NOT EXISTS ( SELECT 1 FROM $SRC_TABLE y  WHERE y.email = x.email AND y.del_date = '$del_deldate' ) AND x.del_date IS NULL AND x.segment = '$del_seg' AND x.subseg = '$del_subseg' ORDER BY random() LIMIT $req_del_cnt) UPDATE $SRC_TABLE a SET del_date = '$del_deldate' FROM cte b WHERE a.id = b.id "

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to update deldates to non-unique src table "
                                                        exit

                                                fi

                                fi

                                #=== UNSUB UPDATE ====#


                                avl_unsub_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=1 " `

                                req_unsub_cnt=`echo $unsub_cnt-$avl_unsub_cnt | bc`

                                if [[ $req_unsub_cnt -gt 0 ]]
                                then

                                        $CONNECTION_STRING -vv -c "with cte as (select x.id from $SRC_TABLE x join $UNSUBS_TABLE y on x.email=y.email  where x.del_date='$del_deldate' and x.segment='$del_seg' and x.subseg='$del_subseg' and x.unsub=0 order by x.status,random() limit $req_unsub_cnt) update $SRC_TABLE a set status=2,unsub=1 from cte b where a.id=b.id"

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to update unsubs to non-unique src table "
                                                        exit

                                                fi

                                else

                                        req_unsub_cnt_1=`echo $req_unsub_cnt | sed 's/-//g'`

                                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=1 order by random() limit $req_unsub_cnt_1) update $SRC_TABLE a set unsub=0,status=2 from cte b where a.id=b.id"

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to update unsubs to non-unique src table "
                                                        exit

                                                fi


                                fi

                                avl_unsub_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=1 " `

                                req_unsub_cnt=`echo $unsub_cnt-$avl_unsub_cnt | bc`

                                if [[ $req_unsub_cnt -gt 0 ]]
                                then

                                        $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where  del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=0 order by status,random() limit $req_unsub_cnt) update $SRC_TABLE a set status=2,unsub=1 from cte b where a.id=b.id"

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to update unsubs to non-unique src table "
                                                        exit

                                                fi


                                fi



                                #=== SOFTS UPDATE ====#

                                if [[ $on_sent == 'Y' ]]
                                then
                                                                                avl_soft_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and flag='S' " `

                                                                                req_soft_cnt=`echo $soft_cnt-$avl_soft_cnt | bc`

                                        if [[ $req_soft_cnt -gt 0 ]]
                                        then

                                                $CONNECTION_STRING -vv -c "with cte as (select id from $SRC_TABLE where  flag is null and del_date='$del_deldate' and segment='$del_seg' and subseg='$del_subseg' and unsub=0 order by status,random() limit $req_soft_cnt) update $SRC_TABLE a set flag='S' from cte b where a.id=b.id"

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to update softs to non-unique src table "
                                                        exit

                                                fi

                                        fi


                                fi
			$CONNECTION_STRING -vv -c "vacuum analyze $SRC_TABLE"

                        done <$SPOOLPATH/deldate_counts



                        touch_counter=`expr $touch_counter + 1 |bc`

                done

                                $CONNECTION_STRING -vv -c " update $SRC_TABLE set status=0 where flag ='S' "




#=============OPENS PRIORITIZATION FOR IPS ====#

                                                ip_append=`$CONNECTION_STRING -qtAX -c "select upper(IP_APPEND) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

                        if [[ $ip_append == 'Y' ]]
                        then

                                ip_var=" left join $OLD_IP_TABLE b  on a.email=b.email"
                                select_ip_var=",(case when b.email is not null then 1 else 0 end) ip_match"
                                ip_order=" , ip_match desc"

                        else

                                ip_var=" "
                                select_ip_var=""
                                ip_order=""

                        fi


                #==== DECILE WISE OPENS/CLICKS SETUP ====#

                        while read decile_stats
                        do
                                cpm_sent=`echo $decile_stats | cut -d'|' -f1`
                                cpm_open=`echo $decile_stats | cut -d'|' -f2`
                                cpm_click=`echo $decile_stats | cut -d'|' -f3`
                                cpm_unsub=`echo $decile_stats | cut -d'|' -f4`
                                cpm_seg=`echo $decile_stats | cut -d'|' -f5`
                                cpm_subseg=`echo $decile_stats | cut -d'|' -f6`
                                cpm_decile=`echo $decile_stats | cut -d'|' -f7`


                                #=== CLICKS ===#

                                req_click=`expr 90*$cpm_click/100 | bc`


                                avl_click_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg'  and decile='$cpm_decile'  and status=3" `

                                if [[ $req_click -gt $avl_click_cnt ]]
                                then

                                        req_cnt=`echo $req_click-$avl_click_cnt | bc`

                                        $CONNECTION_STRING -vv -c " with cte as ( select a.id $select_ip_var from  $SRC_TABLE a $ip_var where a.segment='$cpm_seg' and a.subseg='$cpm_subseg' and a.decile='$cpm_decile' and a.flag is null and a.unsub=0 and a.status in (0,1,2) order by status desc $ip_order ,random() limit $req_cnt) update $SRC_TABLE a set status=3 from cte b where a.id=b.id "

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to adjust clickers to src table "
                                                        exit

                                                fi


                                elif [[ $req_click -lt $avl_click_cnt ]]
                                then

                                        excess_clicks=`expr $avl_click_cnt - $req_click |bc`

                                        $CONNECTION_STRING -vv -c " with cte as ( select id from  $SRC_TABLE  where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and flag is null and unsub=0 and status=3 order by random() limit $excess_clicks) update $SRC_TABLE a set status=2 from cte b where a.id=b.id "

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to adjust clickers to src table "
                                                        exit

                                                fi

                                fi

                                #=== OPENS ===#

                                req_open=`expr 90*$cpm_open/100 | bc`


                                avl_open_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg'  and decile='$cpm_decile'  and status in (2,3)" `



                                if [[ $req_open -gt $avl_open_cnt ]]
                                then

                                        req_open_cnt=`echo $req_open-$avl_open_cnt | bc`

                                        $CONNECTION_STRING -vv -c " with cte as ( select a.id $select_ip_var from  $SRC_TABLE a $ip_var where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and flag is null and unsub=0 and status in (0,1) order by status desc $ip_order ,random() limit $req_open_cnt) update $SRC_TABLE a set status=2 from cte b where a.id=b.id "

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to adjust opens to src table "
                                                        exit

                                                fi


                                elif [[ $req_open -lt $avl_open_cnt ]]
                                then

                                        excess_opens=`expr $avl_open_cnt - $req_open |bc`

                                        $CONNECTION_STRING -vv -c " with cte as ( select id from  $SRC_TABLE  where segment='$cpm_seg' and subseg='$cpm_subseg' and decile='$cpm_decile' and flag is null and unsub=0 and status=2 order by random() limit $excess_opens) update $SRC_TABLE a set status=1 from cte b where a.id=b.id "

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to adjust opens to src table "
                                                        exit

                                                fi


                                fi
				$CONNECTION_STRING -vv -c "vacuum analyze $SRC_TABLE"


                        done <$decile_file



                $CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE set REQUEST_STATUS='R',REQUEST_DESC='Adjusting DecileWise Opens-Clicks.' where REQUEST_ID=$REQUEST_ID "

                #==== DELDATE WISE OPENS/CLICKS/UNSUBS SETUP ====#

                        $CONNECTION_STRING -qtAX -c "select sum(DEL_COUNT) cnt,sum(OPEN_COUNT) opens,sum(CLICK_COUNT) clics,sum(UNSUB_COUNT) unsubs,segment,sub_seg,del_date from $REPORT_TABLE group by 5,6,7 order by 5,6,7"> $SPOOLPATH/deldate_wise_counts

                        while read del_stats
                        do
                                cpm_sent=`echo $del_stats | cut -d'|' -f1`
                                cpm_open=`echo $del_stats | cut -d'|' -f2`
                                cpm_click=`echo $del_stats | cut -d'|' -f3`
                                cpm_unsub=`echo $del_stats | cut -d'|' -f4`
                                cpm_seg=`echo $del_stats | cut -d'|' -f5`
                                cpm_subseg=`echo $del_stats | cut -d'|' -f6`
                                cpm_deldate=`echo $del_stats | cut -d'|' -f7`


                                #=== CLICKS ===#


                                avl_click_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg'  and del_date='$cpm_deldate'  and status=3" `



                                if [[ $avl_click_cnt -gt $cpm_click ]]
                                then

                                        excess_clicks=`expr $avl_click_cnt - $cpm_click |bc`

                                        $CONNECTION_STRING -vv -c " with cte as ( select id from  $SRC_TABLE  where segment='$cpm_seg' and subseg='$cpm_subseg' and del_date='$cpm_deldate' and flag is null and unsub=0 and status=3 order by random() limit $excess_clicks) update $SRC_TABLE a set status=2 from cte b where a.id=b.id "

                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to adjust clickers del_date wise to src table "
                                                        exit

                                                fi

                                fi

                                #=== OPENS ===#

                                avl_open_cnt=`$CONNECTION_STRING -qtAX -c "select count(email) from $SRC_TABLE where segment='$cpm_seg' and subseg='$cpm_subseg'  and del_date='$cpm_deldate'  and status in (2,3)" `



                                if [[ $avl_open_cnt -gt $cpm_open ]]
                                then

                                        req_open_cnt=`echo $avl_open_cnt-$cpm_open | bc`

                                        $CONNECTION_STRING -vv -c " with cte as ( select id from  $SRC_TABLE  where segment='$cpm_seg' and subseg='$cpm_subseg' and del_date='$cpm_deldate' and flag is null and unsub=0 and status=2 order by random() limit $req_open_cnt) update $SRC_TABLE a set status=1 from cte b where a.id=b.id "


                                                if [[ $? -ne 0 ]]
                                                then

                                                        error_fun "4" "Unable to adjust opens del_date wise to src table "
                                                        exit

                                                fi

                                fi
				$CONNECTION_STRING -vv -c "vacuum analyze $SRC_TABLE"




                        done <$SPOOLPATH/deldate_wise_counts

                        #=== UPDATING UNSUBS AS OPENS ===#

                        $CONNECTION_STRING -vv -c "update $SRC_TABLE set status=2 where unsub=1"

                        $CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE set REQUEST_STATUS='R',REQUEST_DESC='Source Partitioning' where REQUEST_ID=$REQUEST_ID "


                        #=== GET UNIQUE DELIVERED COUNT ===#


                        uniq_count=`$CONNECTION_STRING -qtAX -c " select count(distinct email) from $SRC_TABLE "`

                        $CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET UNIQUE_DELIVERED_COUNT=$uniq_count WHERE REQUEST_ID=$REQUEST_ID "

                        new_count=`$CONNECTION_STRING -qtAX -c " select count(distinct a.email) from $SRC_TABLE a left join $OLD_DATA b on a.email=b.email where b.email is null" `

                                                total_del_cnt=`$CONNECTION_STRING -qtAX -c " select count(email) from $OLD_DATA "  `

                                                total_running_cnt=`expr $total_del_cnt + $new_count | bc `

                        $CONNECTION_STRING -vv -c "UPDATE $QA_TABLE SET NEW_RECORD_CNT=$new_count WHERE REQUEST_ID=$REQUEST_ID "

                                                $CONNECTION_STRING -vv -c " UPDATE $QA_TABLE SET TOTAL_RUNNING_UNIQ_CNT=$total_running_cnt WHERE REQUEST_ID=$REQUEST_ID "

                        #=== PARTITIONING SOURCE TABLE ON DELIVERED DATE ===#



                        trt_header_1=`$CONNECTION_STRING  --pset footer  -qAX -c "select * from $TRT_TABLE limit 1" | head -1 | sed 's/|/ varchar,/g'`


                        $CONNECTION_STRING -vv -c "create table $PARTITION_SRC ($trt_header_1 varchar,status int default 0,unsub int default 0,freq int default 1,flag varchar,del_date varchar,id bigint,touch int default 1) PARTITION BY LIST(del_date)"

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to create source partition table"
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

                                        error_fun "4" "Unable to perform partitioning on SRC table"
                                        exit

                                fi

                        done <$SPOOLPATH/uniq_deldates


                        #+== INSERT SRC INTO PARTITION SOURCE TABLE ===#

                        $CONNECTION_STRING -vv -c "insert into $PARTITION_SRC select * from $SRC_TABLE where del_date is not null "

                        if [[ $? -ne 0 ]]
                        then

                                error_fun "4" "Unable to insert source into Partitioned source table"
                                exit

                        fi



                        #==== INDEXING ON PARTITIONED TABLES ====#


                        while read date_
                        do

                                date1=`echo $date_ | sed 's/-//g'`
                                part_src_table=$PARTITION_SRC\_$date1
                                index_name=$part_src_table\_id
                                index_name2=$part_src_table\_combine

                                $CONNECTION_STRING -vv -c "create index $index_name on $part_src_table(id)"

                                if [[ $? -ne 0 ]]
                                then

                                        error_fun "4" "Unable to create index on ID to partitioned source table"
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

                                        error_fun "4" "Unable to create combine index  to partitioned source table"
                                        exit

                                fi

                        done <$SPOOLPATH/uniq_deldates




$CONNECTION_STRING -vv -c "UPDATE $REQUEST_TABLE set REQUEST_STATUS='R',REQUEST_DESC='Partitioning Completed on Source' where REQUEST_ID=$REQUEST_ID "


sh -x $SCRIPTPATH/deliveredScript.sh  $REQUEST_ID >>$HOMEPATH/LOGS/$REQUEST_ID.log 2>>$HOMEPATH/LOGS/$REQUEST_ID.log


