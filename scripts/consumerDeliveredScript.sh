source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo "MODULE5 Consumer Start Time: `date`"



#==== ERROR FUNCTION ===#

error_fun()
{
    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set REQUEST_STATUS='E',ERROR_CODE=$1  ,request_end_time=now(), REQUEST_DESC='$2' where REQUEST_ID=$REQUEST_ID"
    $CONNECTION_STRING -vv -c "update $REQUEST_TABLE set execution_time = TO_CHAR(AGE(request_end_time,request_start_time), 'HH24:MI:SS') where REQUEST_ID=$REQUEST_ID"
	exit

}


echo " CONSUMER EXECUTION StartTime:: `date` "

res_date=`$CONNECTION_STRING -qtAX -c "select RESIDUAL_DATE from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`

if [[ $? -ne 0 ]]
then

        error_fun "5" "Unable to fetch residual date from request table"
		exit

fi



report_arg=$2
PART_PB_TABLE=$3
date_=$4

subseg_report=`$CONNECTION_STRING -qtAX -c "select UPPER(SUB_SEG) from  $REQUEST_TABLE where REQUEST_ID=$REQUEST_ID"`


$CONNECTION_STRING  -c" CREATE TABLE $PART_PB_TABLE (like $PB_TABLE including all)"

if [[ $? -ne 0 ]]
then

        error_fun "5" "Unable to create partition delivered table in consumer script"
		exit

fi

src_header=`$CONNECTION_STRING  --pset footer  -qAX -c "select * from $PARTITION_SRC limit 1" | head -1 | sed 's/|/,/g'`

if [[ $? -ne 0 ]]
then

        error_fun "5" "Unable to fetch src table header in consumer script"
		$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
		exit

fi


while read report_file
do

CAMPAIGN=`echo $report_file | cut -d'|' -f1 `

del_date=`echo $report_file | cut -d'|' -f2 `

del_cnt=`echo $report_file | cut -d'|' -f3 `

open_cnt=`echo $report_file | cut -d'|' -f4`

clk_cnt=`echo $report_file | cut -d'|' -f5 `

unsub_cnt=`echo $report_file | cut -d'|' -f6`

soft_cnt=`echo $report_file | cut -d'|' -f7`

hard_cnt=`echo $report_file | cut -d'|' -f8`

subj=`echo $report_file | cut -d'|' -f9`

creative=`echo $report_file | cut -d'|' -f10`

creativeid=`echo $report_file | cut -d'|' -f11`

offerid=`echo $report_file | cut -d'|' -f12`


if [[ $subseg_report == 'Y' ]]
then

	segment=`echo $report_file | cut -d'|' -f13`
	
	subseg=`echo $report_file | cut -d'|' -f14`
	

	seg_var=" a.segment='$segment' and a.subseg='$subseg' "
	seg_var1=" segment='$segment' and subseg='$subseg' "

else

	segment=`echo $report_file | cut -d'|' -f13`
	seg_var=" a.segment='$segment'  "
	seg_var1=" segment='$segment'  "
	
fi




		#days=$($CONNECTION_STRING -qtAX  -c"select date_part('day',age('$res_date', '$del_date'))" )
		days=$($CONNECTION_STRING -qtAX  -c" select '$res_date'::date - '$del_date'::date ")

		if [[ $? -ne 0 ]]
		then
			
			error_fun "5" "Unable to fetch age between del_date and residual date in consumer script"
			$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
			exit
			
		fi

              #== GENERATING OPEN DATES FOR RESPONDERS IN A ROW WISE ==#
			
		per=75

		> $SPOOLPATH/day_stats$date_\.txt

        while [ $days -ge 0 ]
        do

                if [ $per -eq 0 ]
                then
                        per=60

                fi
		
		if [ "$days" -eq 1 ] || [ "$days" -eq 0 ]
                then
                        per=100

                fi


#                        open=`expr $per*$open_cnt/100 | bc`
#                        click=`expr $per*$clk_cnt/100 | bc`
#                        unsub=`expr $per*$unsub_cnt/100 | bc`
                        open=`echo "($per*$open_cnt+99)/100" |bc `
                        click=`echo "($per*$clk_cnt+99)/100" |bc `
                        unsub=`echo "($per*$unsub_cnt+99)/100" |bc `

                        add_date=$($CONNECTION_STRING -qtAX  -c"select date '$res_date' - $days")

                        if [ $days -ne 0 ]
                        then
							echo "Test in if block $add_date"
							echo "$add_date,$open,$click,$unsub" >>$SPOOLPATH/day_stats$date_\.txt

                        else
							echo "Test in else block $add_date,$open,$click,$unsub"
							echo "$add_date,$open,$click,$unsub" >>$SPOOLPATH/day_stats$date_.txt


                        fi


                        per=`expr $per-15 | bc`

                        open_cnt=`expr $open_cnt-$open | bc`
                        clk_cnt=`expr $clk_cnt-$click | bc`
                        unsub_cnt=`expr $unsub_cnt-$unsub | bc`
                        days=`expr $days-1 | bc`

        done



        while read file
        do
                open_date=`echo $file | cut -d',' -f1`
                open_count=`echo $file | cut -d',' -f2`
                click_count=`echo $file | cut -d',' -f3`
                unsub_count=`echo $file | cut -d',' -f4`

		if [ -n "$open_date" ]
		then

				#=== UNSUBS INSERT ==#

				if [ $unsub_count -gt 0 ]
				then

					$CONNECTION_STRING -vv -c"insert into  $PART_PB_TABLE ($src_header ,campaign,subject,creative,open_date,unsub_date,offerid) select a.* ,'$CAMPAIGN','$subj','$creative','$open_date','$open_date','$offerid' from $PARTITION_SRC  a left join $PART_PB_TABLE b on a.id=b.id where b.id is null and $seg_var and a.status=2 and a.unsub=1 and a.del_date='$del_date' and a.flag is null limit $unsub_count "
					
					
					if [[ $? -ne 0 ]]
					then
					
							error_fun "5" "Unable to insert unsubs into delivered table in consumer script"
							$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
							exit
					
					fi



				fi




				#=== CLICKS INSERT ==#

				if [ $click_count -gt 0 ]
				then

					$CONNECTION_STRING -vv -c"insert into  $PART_PB_TABLE ($src_header ,campaign,subject,creative,open_date,click_date,offerid) select a.* ,'$CAMPAIGN','$subj','$creative','$open_date','$open_date','$offerid' from $PARTITION_SRC  a left join $PART_PB_TABLE b on a.id=b.id where b.id is null and $seg_var and a.unsub=0 and a.del_date='$del_date' and a.flag is null order by a.status desc,random()  limit $click_count "
					
					if [[ $? -ne 0 ]]
					then
					
							error_fun "5" "Unable to insert clicks into delivered table in consumer script"
							$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
							exit
					
					fi					
					
				fi


				#=== OPENS INSERT ==#

				op_cnt=`expr $open_count - $click_count - $unsub_count | bc`

				if [ $op_cnt -gt 0 ]
				then

					$CONNECTION_STRING  -c"insert  into  $PART_PB_TABLE ($src_header ,campaign,subject,creative,open_date,offerid) select a.* ,'$CAMPAIGN','$subj','$creative','$open_date','$offerid' from $PARTITION_SRC  a left join $PART_PB_TABLE b on a.id=b.id where b.id is null and $seg_var and a.status in (2,1,0)  and a.unsub=0 and a.del_date='$del_date' and a.flag is null order by a.status desc,random()  limit $op_cnt "

					if [[ $? -ne 0 ]]
					then
					
							error_fun "5" "Unable to insert opens into delivered table in consumer script"
							$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
							exit
					
					fi					
					
					
				fi

	fi


        done <$SPOOLPATH/day_stats$date_\.txt
		
		#=== INSERT BOUNCES ===#

		$CONNECTION_STRING -vv -c"insert into  $PART_PB_TABLE ($src_header ,campaign,subject,creative,offerid) select a.* ,'$CAMPAIGN','$subj','$creative','$offerid' from $PARTITION_SRC  a left join $PART_PB_TABLE b on a.id=b.id where b.id is null and $seg_var and a.flag='S' and a.del_date='$del_date' limit $soft_cnt "
		
		if [[ $? -ne 0 ]]
		then
					
			error_fun "5" "Unable to insert softs into delivered table in consumer script"
			$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
			exit
					
		fi


		$CONNECTION_STRING -vv -c"insert into  $PART_PB_TABLE ($src_header ,campaign,subject,creative,offerid) select a.* ,'$CAMPAIGN','$subj','$creative','$offerid' from $PARTITION_SRC  a left join $PART_PB_TABLE b on a.id=b.id where b.id is null and $seg_var and a.flag='B' and a.del_date='$del_date' limit $hard_cnt "
		
		if [[ $? -ne 0 ]]
		then
					
			error_fun "5" "Unable to insert hards into delivered table in consumer script"
			$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
			exit
					
		fi		
		#=== INSERT DELIVERED COUNT ===#
		

		inserted_cnt=$($CONNECTION_STRING -qtAX  -c"select count(email) from $PART_PB_TABLE where $seg_var1 and creative='$creative' and subject='$subj' and del_date='$del_date' and offerid='$offerid'")
		
		if [[ $? -ne 0 ]]
		then
					
			error_fun "5" "Unable to get total available delivered count from delivered table in consumer script"
			$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
			exit
					
		fi		

		d_cnt=`expr $del_cnt - $inserted_cnt | bc`


        if [ $d_cnt -gt 0 ]
        then

			$CONNECTION_STRING  -c"insert  into  $PART_PB_TABLE ($src_header ,campaign,subject,creative,offerid) select a.* ,'$CAMPAIGN','$subj','$creative','$offerid' from $PARTITION_SRC  a left join $PART_PB_TABLE b on a.id=b.id where b.id is null and $seg_var and a.status in (1,0) and a.unsub=0 and a.del_date='$del_date' and a.flag is null order by random() limit $d_cnt "
			
			if [[ $? -ne 0 ]]
			then

				error_fun "5" "Unable to insert delivered records into delivered table in consumer script"
				$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
				exit
						
			fi				

		fi

		$CONNECTION_STRING  -c"with cte as ( select id,del_date from  $PART_PB_TABLE ) delete from $PARTITION_SRC a using cte b where a.id=b.id and a.del_date='$del_date'"
		
		if [[ $? -ne 0 ]]
		then

			error_fun "5" "Unable to delete records to partition source table in consumer script"
			$CONNECTION_STRING -vv -c" drop table $PART_PB_TABLE"
			exit
						
		fi	

done < $report_arg



$CONNECTION_STRING -vv -c"vacuum analyze $PB_TABLE"

$CONNECTION_STRING -vv -c"insert into $PB_TABLE select * from $PART_PB_TABLE on conflict do nothing"

	if [[ $? -ne 0 ]]
	then

		error_fun "5" "Unable to insert into delivered table from delivered partition table"
		
	fi	
		
		
$CONNECTION_STRING -vv -c"drop table $PART_PB_TABLE"




	
