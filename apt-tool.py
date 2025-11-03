from flask import Flask, render_template, request, redirect, url_for,jsonify
from datetime import datetime
import psycopg2
import subprocess
import logging
app = Flask(__name__)

logging.basicConfig(filename='app.log', level=logging.INFO)

conn = psycopg2.connect(database = "apt_tool_db",
                        user = "datateam",
                        host= 'zds-prod-pgdb01-01.bo3.e-dialog.com',
                        password = "Datat3amSU!",
                        port = 5432)


@app.route("/")
def home():
    return render_template("apt-tool.html")

@app.route("/apt-tool.html")
def dashboard():
    return render_template("apt-tool.html")
@app.route('/addRequest.html')
def add_request():
    cursor = conn.cursor()
    cursor.execute("SELECT CONCAT(UPPER(LEFT(client_name,1)),LOWER(RIGHT(client_name,LENGTH(client_name)-1))) FROM apt_custom_client_info_table_dnd")
    client_names = cursor.fetchall()
    cursor.execute("SELECT CONCAT(UPPER(LEFT(username,2)),LOWER(RIGHT(username,LENGTH(username)-2))) FROM APT_CUSTOM_APT_TOOL_USER_DETAILS_DND")
    added_by = cursor.fetchall()
    cursor.close()
    return render_template('addRequest.html', client_names=client_names, added_by=added_by)

@app.route('/submit_form', methods=['POST'])

def submit_form():
    logging.info(f"Received form data: {request.form}")
    try:
        logging.info(f"Received form data: {request.form}")
        if not request.form:
            return jsonify({"success": False, "message": "No form data received"}), 400
    except Exception as e:
        logging.error(f"Error receiving form data: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    if request.method == 'POST':
        client_name = request.form.get("clientName")
        added_by = request.form.get("addedBy")
        request_type = request.form.get("requestType")
        percentageInput = request.form.get("percentageInput")
        type2_filepath = request.form.get("filePath")
        start_date = request.form.get("startDate")
        endDate = request.form.get("endDate")
        residualStart = request.form.get("residualStart")
        week = request.form.get("week")
        addTimeStamp = request.form.get("addTimeStamp")
        addIpsLogs = request.form.get("addIpsLogs")
        reportpath = request.form.get("reportpath")
        decilepath = request.form.get("qspath")
        file_type = request.form.get("options")
        Offer_option = request.form.get("Offer_option")
        bounce_option = request.form.get("bounce_option")
        cs_option = request.form.get("cs_option")
        input_query = request.form.get("input_query")

        #print(client_name);
        logging.info(f'Client Name: {client_name}')
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT client_id FROM apt_custom_client_info_table_dnd WHERE client_name = upper(%s)", (client_name,))
            client_row = cursor.fetchone()
            if client_row:
                client_id = client_row[0]
                cursor.execute("INSERT INTO apt_custom_postback_request_details_dnd_test (client_id, added_by, type, old_delivered_per, unique_decile_report_path, from_date, end_date, residual_date, week, timestamp_append, ip_append, cpm_report_path, decile_wise_report_path, on_sent, offerid_unsub_supp, include_bounce_as_delivered, supp_path, query) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   (int(client_id), added_by, int(request_type), int(percentageInput), type2_filepath, start_date, endDate, residualStart, week, addTimeStamp, addIpsLogs, reportpath, decilepath, file_type, Offer_option, bounce_option, cs_option, input_query))
                conn.commit()
                logging.info("Data inserted successfully.")
            else:
                logging.warning(f'Client not found: {client_name}')
                return jsonify({"success": False, "message": "Client not found"}), 404

        except Exception as e:
            logging.error(f'Error during database operation: {e}')
            return jsonify({"success": False, "message": str(e)}), 500
        finally:
            cursor.close()

    return jsonify({"success": True, "message": "Request Added"}), 200
@app.route('/request.html')
def request_logs():
    cursor = conn.cursor()
    cursor.execute("select a.REQUEST_ID,CLIENT_NAME,ADDED_BY,RLTP_FILE_COUNT,REQUEST_STATUS,REQUEST_DESC,EXECUTION_TIME from apt_custom_postback_request_details_dnd a join apt_custom_client_info_table_dnd b on a.CLIENT_ID=b.CLIENT_ID  join apt_custom_postback_qa_table_dnd c on a.REQUEST_ID=c.REQUEST_ID  order by a.REQUEST_ID desc limit 50;")
    logs = cursor.fetchall()
    cursor.close()
    return render_template('request.html', logs=logs)




@app.route('/check_client', methods=['POST'])
def check_client():
    data = request.get_json()
    client_name = data['client_name']
    cursor = conn.cursor()
    cursor.execute("SELECT client_id FROM apt_custom_client_info_table_dnd WHERE client_name = upper(%s)", (client_name,))
    client_row = cursor.fetchone()
    cursor.close()
    if client_row:
        return jsonify({"exists": True, "client_id": client_row[0]})
    return jsonify({"exists": False})

@app.route('/add_client', methods=['POST'])
def add_client():
    data = request.get_json()
    client_name = data['client_name']
    # Execute the shell script
    result = subprocess.run(['sh', '-x','./scripts/addClient.sh', client_name], capture_output=True, text=True)
    if result.returncode == 0:
        # Fetch the client ID from the database
        cursor = conn.cursor()
        cursor.execute("SELECT client_id FROM apt_custom_client_info_table_dnd WHERE client_name = upper(%s)", (client_name,))
        client_row = cursor.fetchone()
        cursor.close()
        if client_row:
            return jsonify({"success": True, "client_id": client_row[0]})
    return jsonify({"success": False})
    
if __name__ == "__main__":
    app.run(debug=True,host='127.0.0.1',port=5000)

