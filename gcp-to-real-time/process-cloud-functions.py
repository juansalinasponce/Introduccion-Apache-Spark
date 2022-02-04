import base64
import json
from google.cloud import bigquery



def process(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     pubsub_message = base64.b64decode(event['data']).decode('utf-8')
     print(pubsub_message)
     print(type(pubsub_message))
     data_json = json.loads(pubsub_message)
     print(data_json)
     client = bigquery.Client()
     table_id = "course-big-data-336218.curso_bigdata.vuelos"
     errors = client.insert_rows_json(table_id, data_json)
     if errors == []:
          print("New rows have been added.")
     else:
          print("Encountered errors while inserting rows: {}".format(errors))
