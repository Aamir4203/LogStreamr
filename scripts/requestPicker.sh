#/bin/bash

MAIN_PATH="/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB"
MAIN_SCRIPTS="/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/SCRIPTS/"

running_request_count=`psql -U datateam -h zds-prod-pgdb01-01.bo3.e-dialog.com -d apt_tool_db -qtAX -c "select count(request_id) from APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND where upper(request_status)='R'"`

if [[ $? -ne 0 ]]
then

        echo "ERROR: UNABLE TO CONNECT POSGRES DB SERVER"
        echo "ERROR: UNABLE TO CONNECT POSGRES DB SERVER" >$SPOOLPATH/fetchRequestDetails.csv

        sh $MAIN_SCRIPTS/sendMail.sh
        exit

else


        if [[ $running_request_count -lt 11 ]]
        then

		new_request_id=`psql -U datateam -h zds-prod-pgdb01-01.bo3.e-dialog.com -d apt_tool_db -qtAX -c "select request_id from APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND where upper(request_status) in ('W','RE','RW') and (request_validation is null or upper(request_validation)='Y') order by request_id limit 1"`

                if [[ $new_request_id != '' ]]
                then
						python3 $MAIN_SCRIPTS/requestValidation.py "$new_request_id"
						
						if [[ $? -eq 0 ]]
						then
						
							validation_status=`psql -U datateam -h zds-prod-pgdb01-01.bo3.e-dialog.com -d apt_tool_db -qtAX -c "select upper(request_validation) from APT_CUSTOM_POSTBACK_REQUEST_DETAILS_DND where request_id=$new_request_id "`
							
							if [[ $validation_status == 'Y' ]]
							then
							
								sh -x $MAIN_SCRIPTS/requestConsumer.sh "$new_request_id"
								
							fi
						else
						
						    echo "ERROR: Request Validation Script Execution Failed" >$SPOOLPATH/fetchRequestDetails.csv
						    sh $MAIN_SCRIPTS/sendMail.sh
		
						fi
						

                fi

 #       else

#             sleep 300
#
        fi

fi

