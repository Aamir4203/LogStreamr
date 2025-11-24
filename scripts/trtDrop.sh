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




$CONNECTION_STRING -qtAX -c " select request_id,client_id,week from  $REQUEST_TABLE where request_start_time<(CURRENT_DATE - INTERVAL '15 days') and purged=0 and request_status in ('C','E') "  > $SPOOLPATH/request_id.txt



if [ -s $SPOOLPATH/request_id.txt ]
then
        while read line
        do
                REQUEST_ID=`echo $line | cut -d'|' -f1 `
                CLIENT_ID=`echo $line | cut -d'|' -f2 `
                WEEK=`echo $line | cut -d'|' -f3 `

                CLIENT_NAME=`$CONNECTION_STRING -qtAX -c " select client_name from $CLIENT_TABLE where client_id=$CLIENT_ID " `

                TRT_TABLE=APT_CUSTOM_$REQUEST_ID\_$CLIENT_NAME\_$WEEK\_TRT_TABLE

                SUPP_TABLE=APT_CUSTOM_$REQUEST_ID\_$CLIENT_NAME\_SUPPRESSION_TABLE

                REPORT_TABLE=APT_CUSTOM_$REQUEST_ID\_$CLIENT_NAME\_REPORT_TABLE

                SRC_TABLE=APT_CUSTOM_$REQUEST_ID\_$CLIENT_NAME\_SRC_TABLE
				
                                $CONNECTION_STRING -c " DROP TABLE  IF EXISTS $SUPP_TABLE "

                                $CONNECTION_STRING -c " DROP TABLE  IF EXISTS $TRT_TABLE "

                                $CONNECTION_STRING -c " DROP TABLE  IF EXISTS $REPORT_TABLE "

                                $CONNECTION_STRING -c " DROP TABLE  IF EXISTS $SRC_TABLE "

        done <$SPOOLPATH/request_id.txt

fi

                 
echo "END TIME :: `date`"

