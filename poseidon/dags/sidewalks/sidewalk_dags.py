"""Sidewalk _dags file."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.models import DAG

from trident.util import general
from trident.util.geospatial import *
from trident.util.notifications import notify

from dags.sidewalks.sidewalk_jobs import *
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

# All times in Airflow UTC.  Set Start Time in PST?
args = general.args
conf = general.config
schedule = general.schedule['streets_sdif']
start_date = general.start_date['streets_sdif']

#: Dag spec
dag = DAG(dag_id='sidewalk', default_args=args, start_date=start_date, schedule_interval=schedule)

#: Latest Only Operator for sdif
sidewalk_latest_only = LatestOnlyOperator(task_id='sidewalk_latest_only', dag=dag)

#: Get sidewalk data from DB
get_sidewalk_data = PythonOperator(
    task_id='get_sidewalk_oci',
    python_callable=get_sidewalk_data,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Get sidewalks shapefile from Atlas
get_sw_shapefiles = PythonOperator(
    task_id='get_sidewalk_gis',
    python_callable=get_sidewalk_gis,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert shp to geojson
sidewalks_to_geojson = BashOperator(
    task_id='sidewalks_to_geojson',
    bash_command=shp_to_geojson(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert shp to topojson
sidewalks_to_topojson = BashOperator(
    task_id='sidewalks_to_topojson',
    bash_command=shp_to_topojson(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert geojson to geobuf
sidewalks_to_geobuf = PythonOperator(
    task_id='sidewalks_to_geobuf',
    python_callable=geojson_to_geobuf,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert geojson to geobuf
geobuf_zip = PythonOperator(
    task_id='sidewalks_geobuf_to_zip',
    python_callable=geobuf_to_gzip,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert geojson to geobuf
shape_zip = PythonOperator(
    task_id='sidewalks_shape_to_zip',
    python_callable=shp_to_zip,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload OCI file to S3
upload_oci_file = S3FileTransferOperator(
    task_id='upload_oci',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalk_cond_datasd_v1.csv',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sidewalk_cond_datasd_v1.csv',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload shp GIS file to S3
upload_shp_file = S3FileTransferOperator(
    task_id='sidewalks_shp_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalks.zip',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sidewalks.zip',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload geojson GIS file to S3
upload_geojson_file = S3FileTransferOperator(
    task_id='sidewalks_geojson_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalks.geojson',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sidewalks.geojson',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload topojson GIS file to S3
upload_topojson_file = S3FileTransferOperator(
    task_id='sidewalks_topojson_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalks.topojson',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sidewalks.topojson',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload topojson GIS file to S3
upload_pbf_file = S3FileTransferOperator(
    task_id='sidewalks_pbf_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='sidewalks.pbf',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='tsw/sidewalks.pbf',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update portal modified date
update_gis_md = get_seaboard_update_dag('sidewalk-gis.md', dag)

#: Execution order

#: Latest only operator must run before getting sidewalk data
get_sidewalk_data.set_upstream(sidewalk_latest_only)

#: Getting sidewalk data must run before uploading
upload_oci_file.set_upstream(get_sidewalk_data)

#: get_sidewalk_data must run before get shapefiles so they can be joined
get_sw_shapefiles.set_upstream(sidewalk_latest_only)

#: get_sw_shapefiles must run before converting to geojson
sidewalks_to_geojson.set_upstream(get_sw_shapefiles)

#: get_sw_shapefiles must run before converting to topojson
sidewalks_to_topojson.set_upstream(get_sw_shapefiles)

#: to_geojson must run before converting to geobuf
sidewalks_to_geobuf.set_upstream(sidewalks_to_geojson)

#: to_geobuf must run before zipping geobuf
geobuf_zip.set_upstream(sidewalks_to_geobuf)

#: get_sw_shapefile must run before zipping shapefile
shape_zip.set_upstream(get_sw_shapefiles)

#: zipping shapefile must run before uploading
upload_shp_file.set_upstream(shape_zip)

#: converting to geojson must run before uploading
upload_geojson_file.set_upstream(sidewalks_to_geojson)

#: converting to topojson must run before uploading
upload_topojson_file.set_upstream(sidewalks_to_topojson)

#: zip geobuf must run before uploading
upload_pbf_file.set_upstream(geobuf_zip)

#: upload pbf must run before updating dataset on github
update_gis_md.set_upstream(upload_pbf_file)
