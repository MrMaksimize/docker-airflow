"""CityIQ _jobs file."""
import requests
import json
from datetime import *
import logging
from trident.util import general
import math

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator

conf = general.config
bbox = "33.077762:-117.663817,32.559574:-116.584410"
event_types = ["PKIN","PKOUT"]
token_url = "https://auth.aa.cityiq.io/oauth/token"
event_url = "https://sandiego.cityiq.io/api/v2/event/locations/events"

def get_token_response():
	""" Get auth token for making requests """

	query_string = {"grant_type":"client_credentials"}

	payload = ""
	headers = {
		'Authorization': "Basic UHVibGljQWNjZXNzOnVWZWVNdWl1ZTRrPQ==",
		'User-Agent': "PostmanRuntime/7.11.0",
		'Accept': "*/*",
		'Cache-Control': "no-cache",
		'Host': "auth.aa.cityiq.io",
		'accept-encoding': "gzip, deflate",
		'Connection': "keep-alive",
		'cache-control': "no-cache"
		}

	try:

		response = requests.request("GET", token_url, data=payload, headers=headers, params=query_string)
		token_data = json.loads(response.text)
		token = token_data['access_token']
		return token

	except Exception as e:

		logging.error(e)


def get_events(**kwargs):
	""" Send GET request for events using token """
	
	logging.info("Getting token from previous task")
	ti = kwargs['ti']
	token = ti.xcom_pull(task_ids='get_token_response')
	
	logging.info("Calculating start and end dates")
	end = datetime.now()
	start = end - timedelta(days=1)

	logging.info(f"Starting at {start} and ending at {end}")

	headers = {
	'Authorization': f"Bearer {token}",
	'predix-zone-id': "SD-IE-PARKING",
	'cache-control': "no-cache"
	}

	for et in event_types:
		logging.info(f"Retrieving metadata for {et}")

		query_string = {
			"bbox": bbox,
			"locationType": "PARKING_ZONE",
			"eventType": et,
			"startTime": str(int(start.timestamp() * 1000)),
			"endTime": str(int(end.timestamp() * 1000)),
			"pageSize": 10
		}

		try:
			response_md = requests.request("GET", event_url, headers=headers, params=query_string)

			if response_md.status_code != 200:
				
				raise Exception(response_md.status_code)
			
			else:
				logging.info("Got metadata")
				md = response_md.json()
				records = md['metaData']['totalRecords']
				query_string['pageSize'] = records
				pages = math.ceil(records/1000)
				for page in range(0,1):
					logging.info(query_string)
					#query_string['page'] = page
					logging.info(f"Requesting {query_string['pageSize']} records")
					try:
						response = requests.request("GET", event_url, headers=headers, params=query_string)
						if response.status_code != 200:
							raise Exception(response.status_code)
						else:
							assets = response_md.json()
							logging.info("Got assets")
							file_date = datetime.now().strftime('%Y_%m_%d')
							file_path = f"{conf['prod_data_dir']}/{file_date}_{et.lower()}_{page}.json"
							logging.info("Writing events")
							with open(file_path, 'w') as f:
								json.dump(assets, f)

							logging.info(f"Successfully requested events for {et}")

					except Exception as e:

						logging.error(e)

		except Exception as e:

			logging.error(e)
		
	
	return "Successfully requested events for all event types"