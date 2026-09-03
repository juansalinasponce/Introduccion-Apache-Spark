import base64
import functions_framework
from flask import abort
import json
from google.cloud import bigquery

@functions_framework.http
def hello_pubsub(request):
    request_json = request.get_json(silent=True)

    if request_json is None or 'message' not in request_json:
        abort(400, description="Bad Request: 'message' field is missing")

    message_json = request_json['message']

    if message_json is None or 'data' not in message_json:
        abort(400, description="Bad Request: Pub/Sub message or data field is missing")

    encoded_data = message_json['data']
    try:
        pubsub_message = base64.b64decode(encoded_data).decode('utf-8')
        print(pubsub_message)
        print(type(pubsub_message))
        data_json = json.loads(pubsub_message)
        print(data_json)
        client = bigquery.Client()
        table_id = "juansalinasbigdata.dwh.vuelos_realtime_jmsp"
        errors = client.insert_rows_json(table_id, data_json)
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
        print(f"Pub/Sub message: {decoded_data}")
        return "OK"
    except Exception as e:
        print(f"Error processing Pub/Sub message: {e}")
        abort(500, description="Error processing message")
