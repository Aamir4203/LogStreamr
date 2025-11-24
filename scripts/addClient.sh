#/bin/bash


MAIN_PATH="/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB"
MAIN_SCRIPTS="/u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/SCRIPTS/"
CONNECTION_STRING="psql -U datateam -h zds-prod-pgdb01-01.bo3.e-dialog.com -d apt_tool_db"
alert_to="datateam@aptroid.com"

NEW_CLIENT_NAME=$(echo $1 | tr 'a-z' 'A-Z')


if [[ $# -ne 1 ]]
then
		echo "ERROR: Client Name is missing as an Argument"
        echo -e " Hi Team, \n\n ERROR: Client Name is missing as an Argument. \n\n Thanks,\n DataAttribution" | mail -r "AttributionAlerts@zds-db3-02.bo3.e-dialog.com" -s "New Client Additon :: APT TOOL" $alert_to

        exit

fi


CLIENT_TABLE="APT_CUSTOM_CLIENT_INFO_TABLE_DND"

NEW_CLIENT_TOTAL_DEL_TABLE=APT_CUSTOM_$NEW_CLIENT_NAME\_TOTAL_DELIVERED_DND
NEW_CLIENT_PB_UNSUB_TABLE=APT_CUSTOM_$NEW_CLIENT_NAME\_POSTED_UNSUB_DND
NEW_CLIENT_PREV_PB_TABLE=APT_ADHOC_$NEW_CLIENT_NAME\_PREV_POSTBACK_DND



$CONNECTION_STRING -vv -c "create table $NEW_CLIENT_TOTAL_DEL_TABLE(email varchar unique,segment varchar,del_date varchar,week varchar default '#',touch int default 0) "

if [[ $? -ne 0 ]]
then

	echo -e " ERROR:: Unable to create total delivered table for New Client $NEW_CLIENT_NAME" 
	
	echo -e " Hi Team, \n\n ERROR: Unable to create total delivered table. \n\n Thanks,\n DataAttribution" | mail -r "AttributionAlerts@zds-db3-02.bo3.e-dialog.com" -s "New Client Additon :: APT TOOL" $alert_to
    exit

fi

$CONNECTION_STRING -vv -c "create table $NEW_CLIENT_PB_UNSUB_TABLE(email varchar unique,segment varchar,del_date varchar,unsub_date varchar,flag varchar) "

if [[ $? -ne 0 ]]
then

	echo -e " ERROR:: Unable to create posted unsub table for New Client $NEW_CLIENT_NAME" 
	
    echo -e " Hi Team, \n\n ERROR: Unable to create posted unsub table. \n\n Thanks,\n DataAttribution" | mail -r "AttributionAlerts@zds-db3-02.bo3.e-dialog.com" -s "New Client Additon :: APT TOOL" $alert_to

    exit

fi


$CONNECTION_STRING -vv -c "create table $NEW_CLIENT_PREV_PB_TABLE(email varchar unique,del_date varchar,open_date varchar,click_date varchar,unsub_date varchar,segment varchar,flag varchar) "

if [[ $? -ne 0 ]]
then

	echo -e " ERROR:: Unable to create temporary previous delivered table for New Client $NEW_CLIENT_NAME" 
	
    echo -e " Hi Team, \n\n ERROR: Unable to create temporary previous delivered table. \n\n Thanks,\n DataAttribution" | mail -r "AttributionAlerts@zds-db3-02.bo3.e-dialog.com" -s "New Client Additon :: APT TOOL" $alert_to

    exit

fi


$CONNECTION_STRING -vv -c "insert into $CLIENT_TABLE(
CLIENT_NAME,TOTAL_DELIVERED_TABLE,POSTED_UNSUB_HARDS_TABLE,PREV_WEEK_PB_TABLE,BKP_PREV_PB_TABLE) values (
'$NEW_CLIENT_NAME','$NEW_CLIENT_TOTAL_DEL_TABLE','$NEW_CLIENT_PB_UNSUB_TABLE','$NEW_CLIENT_PREV_PB_TABLE','$NEW_CLIENT_PREV_PB_TABLE')"


if [[ $? -ne 0 ]]
then

	echo -e " ERROR:: Unable to add New Client $NEW_CLIENT_NAME into the client table" 
	
    echo -e " Hi Team, \n\n ERROR: Unable to add New Client info to the Main client table. \n\n Thanks,\n DataAttribution" | mail -r "AttributionAlerts@zds-db3-02.bo3.e-dialog.com" -s "New Client Additon :: APT TOOL" $alert_to

    exit

else

new_client_id=`$CONNECTION_STRING -qtAX -c "select client_id from $CLIENT_TABLE where client_name='$NEW_CLIENT_NAME' "`

echo -e " Hi Team, Succesfully client details added to the table. Below are the details. \n\n Client Name: $NEW_CLIENT_NAME \n Client ID :: $new_client_id \n Posted Unsub Table :: $NEW_CLIENT_PB_UNSUB_TABLE \n Total Delivered Table :: $NEW_CLIENT_TOTAL_DEL_TABLE \n Postback Temporary Table :: $NEW_CLIENT_PREV_PB_TABLE \n\n Thanks,\n DataAttribution" | mail -r "AttributionAlerts@zds-db3-02.bo3.e-dialog.com" -s "New Client Additon :: $NEW_CLIENT_NAME :: APT TOOL" $alert_to

fi

