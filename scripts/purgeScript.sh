#/bin/bash

CONNECTION_STRING="psql -U datateam -h zds-prod-pgdb01-01.bo3.e-dialog.com -d apt_tool_db"
MAIN_PATH=/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB
HOMEPATH=$MAIN_PATH
SPOOLPATH=$HOMEPATH/PURGED_DATA
BKP_PATH=$HOMEPATH/PURGED_DATA

REQUEST_TABLE="APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND"
CLIENT_TABLE="APT_CUSTOM_CLIENT_INFO_TABLE_DND"
OLD_IP_TABLE="APT_CUSTOM_VERIZON_IPS_USED_DND"
NEW_IP_TABLE="APT_CUSTOM_VERIZON_NEW_IPS_DND"

alert_to="akhan@aptroid.com"

echo "START TIME :: `date`"
#------------------ REQUEST_ID'S PULLING -------------#

#=== CLIENT TABLE BACKUP ===#

$CONNECTION_STRING -F'|' --pset footer -X -A -c  "select * from $CLIENT_TABLE " > $MAIN_PATH/BKP/$CLIENT_TABLE
$CONNECTION_STRING -F'|' --pset footer -X -A -c  "select * from $REQUEST_TABLE" > $MAIN_PATH/BKP/$REQUEST_TABLE
$CONNECTION_STRING -F'|' --pset footer -X -A -c  "select * from $OLD_IP_TABLE " > $MAIN_PATH/BKP/$OLD_IP_TABLE
$CONNECTION_STRING -F'|' --pset footer -X -A -c  "select * from $NEW_IP_TABLE " > $MAIN_PATH/BKP/$NEW_IP_TABLE

/usr/local/bin/aws s3 cp $MAIN_PATH/BKP/ s3://new-datateam/APT_TOOL_DB/IMP_FILES/ --recursive


$CONNECTION_STRING -qtAX -c " select posted_unsub_hards_table from  $CLIENT_TABLE "  > $SPOOLPATH/unsub_files

if [ $? -ne 0 ]
then
        echo -e " Hi Team, \n \n Unable to fetch posted_unsub_hards_table data from Client Table. Please look into this. \n\n Thanks, \n DataAttribution " | mail -r "DataAttribution" -s "PURGING :: APT REQUESTS" $alert_to
        exit
fi

while read file
do

$CONNECTION_STRING -qAX --pset footer -c "select * from $file "  > $BKP_PATH/$file


/usr/local/bin/aws s3 cp $BKP_PATH/$file s3://new-datateam/APT_TOOL_DB/IMP_FILES/POSTED_UNSUBS/

done<$SPOOLPATH/unsub_files



$CONNECTION_STRING -qtAX -c " select request_id,client_id,upper(week) from  $REQUEST_TABLE where request_start_time<(CURRENT_DATE - INTERVAL '45 days') and purged=0 and request_status='C' "  > $SPOOLPATH/request_id.txt

if [ $? -ne 0 ]
then
        echo -e " Hi Team, \n \n Unable to fetch data from Request Table. Please look into this. \n\n Thanks, \n DataAttribution " | mail -r "DataAttribution" -s "PURGING :: APT REQUESTS" $alert_to
        exit
fi

if [ -s $SPOOLPATH/request_id.txt ]
then
        while read line
        do
                REQUEST_ID=`echo $line | cut -d'|' -f1 `
                CLIENT_ID=`echo $line | cut -d'|' -f2 `
                WEEK=`echo $line | cut -d'|' -f3 `

		CLIENT_NAME=`$CONNECTION_STRING -qtAX -c " select upper(client_name) from $CLIENT_TABLE where client_id=$CLIENT_ID " `

		if [ $? -ne 0 ]
		then
			echo -e " Hi Team, \n \n Unable to fetch client name from Client Table. Please look into this. \n\n Thanks, \n DataAttribution " | mail -r "DataAttribution" -s "PURGING :: APT REQUESTS" $alert_to
			exit
		fi

                GEN_TABLE=APT_CUSTOM_$REQUEST_ID\_$CLIENT_NAME\_GEN_TABLE

                POSTBACK_TABLE=APT_CUSTOM_$REQUEST_ID\_$CLIENT_NAME\_$WEEK\_POSTBACK_TABLE

#------------------ GEN_TABLE PURGING -------------#

                $CONNECTION_STRING -qAX --pset footer -c "select * from $GEN_TABLE "  > $BKP_PATH/$GEN_TABLE

#                               if [ $? -ne 0 ]
#                               then
#                                       echo -e " Hi Team, \n \n Unable to take backup of GenuineDelivered table. Please look into this. \n\n Thanks, \n DataAttribution " | mail -r "DataAttribution" -s "PURGING :: APT REQUESTS" $alert_to
#                                       exit
#                               fi

                                $CONNECTION_STRING -c " DROP TABLE IF EXISTS $GEN_TABLE "

#------------------ POSTBACK_TABLE PURGING -------------#

                $CONNECTION_STRING -qAX --pset footer -c "select * from $POSTBACK_TABLE "  > $BKP_PATH/$POSTBACK_TABLE

#                               if [ $? -ne 0 ]
#                               then
#                                       echo -e " Hi Team, \n \n Unable to take backup of Postback table. Please look into this. \n\n Thanks, \n DataAttribution " | mail -r "DataAttribution" -s "PURGING :: APT REQUESTS" $alert_to
#                                       exit
#                               fi


                $CONNECTION_STRING -c " DROP TABLE IF EXISTS $POSTBACK_TABLE "


#-------------------BACKUP_APT_TABLES---------------------#
		gzip $BKP_PATH/$GEN_TABLE $BKP_PATH/$POSTBACK_TABLE 

                /usr/local/bin/aws s3 cp $BKP_PATH/$GEN_TABLE.gz      s3://new-datateam/APT_TOOL_DB/$REQUEST_ID/

                /usr/local/bin/aws s3 cp $BKP_PATH/$POSTBACK_TABLE.gz s3://new-datateam/APT_TOOL_DB/$REQUEST_ID/


                if [[ $? -eq 0 ]]
                then

			rm $BKP_PATH/*

		fi

#------------------ UPDATING PURGING DETAILS -------------#

                $CONNECTION_STRING -c " UPDATE $REQUEST_TABLE SET purged=1 WHERE request_id=$REQUEST_ID "

		if [ $? -ne 0 ]
		then
			echo -e " Hi Team, \n \n Unable to update purged status to Request Table. Please look into this. \n\n Thanks, \n DataAttribution " | mail -r "DataAttribution" -s "PURGING :: APT REQUESTS" $alert_to
			exit
		fi

	       # rm -rf /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$REQUEST_ID
		       
	       if [ -d "/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$REQUEST_ID" ]
	       then
		       rm -rf "/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$REQUEST_ID"
		       echo "Directory successfully deleted."
	       else
		       echo "Directory not found or already deleted."
	       fi


        done <$SPOOLPATH/request_id.txt

fi



echo "END TIME :: `date`"


