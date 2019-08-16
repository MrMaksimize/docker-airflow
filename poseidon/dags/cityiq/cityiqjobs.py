import requests
import json
from datetime import *

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator

# starting to test the dag functuon
def get_token_response():
	url = "https://auth.aa.cityiq.io/oauth/token"

	query_string = {"grant_type":"client_credentials"}

	payload = ""
	headers = {
		'Authorization': "Basic UHVibGljQWNjZXNzOnVWZWVNdWl1ZTRrPQ==",
		'User-Agent': "PostmanRuntime/7.11.0",
		'Accept': "*/*",
		'Cache-Control': "no-cache",
		'Postman-Token': "c9257e3c-29d1-4999-912b-a488c04d397e,d9e0d977-bcd1-4ff3-a10a-8abc960f7c7d",
		'Host': "auth.aa.cityiq.io",
		'accept-encoding': "gzip, deflate",
		'Connection': "keep-alive",
		'cache-control': "no-cache"
		}

	response = requests.request("GET", url, data=payload, headers=headers, params=query_string)
	token_data = json.loads(response.text)
	token = token_data['access_token']
	print(token)
	return token


def get_pkout_bbox(**kwargs):
	ti = kwargs['ti']
	token = ti.xcom_pull(task_ids='get_token_response') # get outputs from get_token_response task
	
	url = "https://sandiego.cityiq.io/api/v2/event/locations/events"
	end = datetime.now()
	start = end - timedelta(days=1)
	query_string = {
		"bbox": "33.077762:-117.663817,32.559574:-116.584410",
		"locationType": "PARKING_ZONE",
		"eventType": "PKOUT",
		"startTime": str(int(start.timestamp() * 1000)),
		"endTime": str(int(end.timestamp() * 1000)),
		"pageSize": "100"
		}
	headers = {
		'Authorization': "Bearer {}".format(token),
		'predix-zone-id': "SD-IE-PARKING",
		'cache-control': "no-cache",
		'postman-token': "e8199816-04a2-29e4-ec0d-1a68a94fe771"
		}
	response = requests.request("GET", url, headers=headers, params=query_string)
	assets = response.json()

	path = '/data/temp/'
	file_name = datetime.now().strftime('%Y_%m_%d_pkout.json')
	path += file_name
	with open(path, 'w') as f:
		json.dump(assets, f)
	return


def get_pkin_bbox(**kwargs):
	ti = kwargs['ti']
	token = ti.xcom_pull(task_ids='get_token_response')

	url = "https://sandiego.cityiq.io/api/v2/event/locations/events"
	end = datetime.now()
	start = end - timedelta(days=1)
	query_string = {
		"bbox": "33.077762:-117.663817,32.559574:-116.584410",
		"locationType": "PARKING_ZONE",
		"eventType": "PKIN",
		"startTime": str(int(start.timestamp() * 1000)),
		"endTime": str(int(end.timestamp() * 1000)),
		"pageSize": "100"
		}
	headers = {
		'Authorization': "Bearer {}".format(token),
		'predix-zone-id': "SD-IE-PARKING",
		'cache-control': "no-cache",
		'postman-token': "63188df1-b0db-c553-aecd-4882b65e5dcc"
	}

	response = requests.request("GET", url, headers=headers, params=query_string)
	assets = response.json()

	path = '/data/temp/'
	file_name = datetime.now().strftime('%Y_%m_%d_pkin.json')
	path += file_name
	with open(path, 'w') as f:
		json.dump(assets, f)
	return

# def get_pedevt_bbox(**kwargs):
# 	ti = kwargs['ti']
# 	token = ti.xcom_pull(task_ids='get_token_response')

# 	url = "https://sandiego.cityiq.io/api/v2/event/locations/events"
# 	end = datetime.now()
# 	start = end - timedelta(days=1)
# 	query_string = {
# 		"bbox": "33.077762:-117.663817,32.559574:-116.584410",
# 		"locationType": "WALKWAY",
# 		"eventType": "PEDEVT",
# 		"startTime": str(int(start.timestamp() * 1000)),
# 		"endTime": str(int(end.timestamp() * 1000)),
# 		"pageSize": "100"
# 		}
# 	headers = {
# 		'Authorization': "Bearer {}".format(token),
# 		'predix-zone-id': "SD-IE-PEDESTRIAN",
# 		'cache-control': "no-cache",
# 		'postman-token': "5253f845-d1c8-4dd4-bea6-65a192f8d9e8"
# 	}

# 	response = requests.request("GET", url, headers=headers, params=query_string)
# 	assets = response.json()

# 	path = '/data/temp/'
# 	file_name = datetime.now().strftime('%Y_%m_%d_pedevt.json')
# 	path += file_name
# 	with open(path, 'w') as f:
# 		json.dump(assets, f)
# 	return


# def get_tfevt_bbox(**kwargs):
# 	ti = kwargs['ti']
# 	token = ti.xcom_pull(task_ids='get_token_response')

# 	url = "https://sandiego.cityiq.io/api/v2/event/locations/events"
# 	end = datetime.now()
# 	start = end - timedelta(days=1)
# 	query_string = {
# 		"bbox": "33.077762:-117.663817,32.559574:-116.584410",
# 		"locationType": "WALKWAY",
# 		"eventType": "PEDEVT",
# 		"startTime": str(int(start.timestamp() * 1000)),
# 		"endTime": str(int(end.timestamp() * 1000)),
# 		"pageSize": "100"
# 		}
# 	headers = {
# 		'Authorization': "Bearer {}".format(token),
# 		'predix-zone-id': "SD-IE-TRAFFIC",
# 		'User-Agent': "PostmanRuntime/7.15.0",
# 		'Accept': "*/*",
# 		'Cache-Control': "no-cache",
# 		'Postman-Token': "ef36dfb8-dde2-47c2-a65e-b6b35d8397c2,7f7cb66d-f7e7-4f2f-b841-fbe580ea4287",
# 		'Host': "sandiego.cityiq.io",
# 		'accept-encoding': "gzip, deflate",
# 		'Connection': "keep-alive",
# 		'cache-control': "no-cache"
#  	}

# 	response = requests.request("GET", url, headers=headers, params=query_string)
# 	assets = response.json()

# 	path = '/data/temp/'
# 	file_name = datetime.now().strftime('%Y_%m_%d_tfevt.json')
# 	path += file_name
# 	with open(path, 'w') as f:
# 		json.dump(assets, f)
# 	return


# def get_assets(**kwargs):
# 	ti = kwargs['ti']
# 	token = ti.xcom_pull(task_ids='get_token_response') # get outputs from get_token_response task

# 	url = "https://sandiego.cityiq.io/api/v2/metadata/assets/search"
# 	query_string = {
# 		"bbox": "33.077762:-117.663817,32.559574:-116.584410",
# 		"page": "0",
# 		"size": "2000000",
# 		"q": "assetType:CAMERA"
# 		}
# 	payload = ""
# 	headers = {
# 		'Authorization': "Bearer {}".format(token),
# 		'Predix-Zone-Id': "SD-IE-TRAFFIC",
# 		#'User-Agent': "PostmanRuntime/7.11.0",
# 		'Accept': "*/*",
# 		'Cache-Control': "no-cache",
# 		#'Postman-Token': "a90832bf-90a6-4e4c-a5b6-4c3414628a00,9405e923-b326-4760-8d29-9222b0d919ec",
# 		'Host': "sandiego.cityiq.io",
# 		#'accept-encoding': "gzip, deflate",
# 		'Connection': "keep-alive",
# 		'cache-control': "no-cache"
# 		}

# 	response = requests.request("GET", url, data=payload, headers=headers, params=query_string)
# 	assets = response.json()

# 	### --- Modify file path --- ###
# 	path = '/data/temp/'
# 	file_name = datetime.now().strftime('%Y_%m_%d_assets.json')
# 	path += file_name
# 	with open(path, 'w') as f:
# 		json.dump(assets, f)
# 	return


# def get_asset_details():
# 	with open('/usr/local/airflow/poseidon/dags/cityiq/assets.json') as f:
# 		temp = json.loads(f.read())
# 		data = temp.get('content')
# 		d=0
# 		assets = [] 
# 		while d < len(data):
# 			assets.append(data[d].get('assetUid'))
# 			d+=1
# 		print(assets)
    
# 	print('getting asset details')
# 	#data = json.loads(temp)
# 	#data[0]
# 	#print([d.get('assetUid') for d in data])


# def get_parking(assetID):
# 	token = get_token_response()
# 	url = "https://sandiego.cityiq.io/api/v2/event/assets/{}/events".format(assetID)
# 	#look into increasing pageSize
# 	query_string = {
# 		"eventType": "PKIN",
# 		"startTime": "1558049253306",
# 		"endTime": "1558567653306",
# 		"pageSize": "100"
# 		}
# 	headers = {
# 		'Authorization': "Bearer {}".format(token),
# 		'predix-zone-id': "SD-IE-PARKING",
# 		'cache-control': "no-cache",
# 		'postman-token': "abbbec83-ff55-1748-f703-b617bd17b794"
# 		}
# 	response = requests.request("GET", url, headers=headers, params=query_string)
# 	return