import requests
import json

token = ''

# starting to test the dag functuon
def get_token_response():
	url = "https://auth.aa.cityiq.io/oauth/token"

	querystring = {"grant_type":"client_credentials"}

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

	response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
	token_data = json.loads(response.text)
	token = token_data['access_token']
	return token

def get_assets():
	token = get_token_response()
	url = "https://sandiego.cityiq.io/api/v2/metadata/assets/search"

	querystring = {"bbox":"33.077762:-117.663817,32.559574:-116.584410","page":"0","size":"2000000","q":"assetType:CAMERA"}

	payload = ""
	headers = {
		'Authorization': "Bearer {}".format(token),
		'Predix-Zone-Id': "SD-IE-TRAFFIC",
		#'User-Agent': "PostmanRuntime/7.11.0",
		'Accept': "*/*",
		'Cache-Control': "no-cache",
		#'Postman-Token': "a90832bf-90a6-4e4c-a5b6-4c3414628a00,9405e923-b326-4760-8d29-9222b0d919ec",
		'Host': "sandiego.cityiq.io",
		#'accept-encoding': "gzip, deflate",
		'Connection': "keep-alive",
		'cache-control': "no-cache"
		}

	response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
	assets = response.json()

	### --- Modify file path --- ###
	with open('/usr/local/airflow/poseidon/dags/cityiq/assets.json', 'w') as f:
		json.dump(assets, f)

def get_asset_details():
	with open('/usr/local/airflow/poseidon/dags/cityiq/assets.json') as f:
		temp = json.loads(f.read())
		data = temp.get('content')
		d=0
		assets = [] 
		while d < len(data):
			assets.append(data[d].get('assetUid'))
			d+=1
		print(assets)
    
	print('getting asset details')
	#data = json.loads(temp)
	#data[0]
	#print([d.get('assetUid') for d in data])

#get_assets()

