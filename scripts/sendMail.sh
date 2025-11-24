#/bin/bash
source /u1/techteam/PFM_CUSTOM_SCRIPTS/APT_TOOL_DB/REQUEST_PROCESSING/$1/ETC/config.properties

echo  "Hi Team,<BR></\BR><BR></\BR>Please find the below request details.<BR></\BR><BR></\BR>" > $SPOOLPATH/fetchRequestDetails.html
awk -F'|' 'BEGIN { 
print "<HTML><head>"
print "<style>"
print "tr {text-align: center;}"
print "td {text-align: center;}"
print "</style>"
print "</head>"
print "<TABLE border=\"1\">"}
{
	
    printf "<TR>"
    for(j=1;j<=NF;j++)
		printf "<TD>%s</TD>", $j
    print "</TR>"	
}
END { print "</TABLE></BODY></HTML>" }' $SPOOLPATH/fetchRequestDetails.csv >> $SPOOLPATH/fetchRequestDetails.html

echo  "<BR>Thanks,</\BR> <BR>SysAdmin</\BR>" >> $SPOOLPATH/fetchRequestDetails.html

(
echo "From: AttributionAlerts@zds-db3-02.bo3.e-dialog.com "
echo "To: $ALERT_TO "
echo "MIME-Version: 1.0"
echo "Subject: APT REQUEST DETAILS :: $CLIENT_NAME "
echo "Content-Type: text/html"
echo ""
cat $SPOOLPATH/fetchRequestDetails.html
)| sendmail -t


