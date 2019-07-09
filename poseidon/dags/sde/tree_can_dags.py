"""_dags file for tree canopy sde extraction."""
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

from dags.sde.tree_can_jobs import *
from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

args = general.args
conf = general.config
schedule = general.schedule['gis_tree_canopy']
start_date = general.start_date['gis_tree_canopy']
folder = 'trees'
layer = 'tree_canopy'
datasd_name = 'tree_canopy_datasd'
path_to_file = conf['prod_data_dir'] + '/' + datasd_name

dag = DAG(dag_id='gis_{layer}'.format(layer=layer),
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule)


#: Latest Only Operator for sdif
treecan_latest_only = LatestOnlyOperator(task_id='tree_canopy_latest_only', dag=dag)

#: Get tree canopy shapefile from Atlas
get_shapefiles = PythonOperator(
    task_id='get_tree_canopy_gis',
    python_callable=sde_to_shp,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert shp to geojson
shp_to_geojson = BashOperator(
    task_id='tree_canopy_to_geojson',
    bash_command=shp_to_geojson(),
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert geojson to geobuf
geojson_to_geobuf = PythonOperator(
    task_id='tree_canopy_to_geobuf',
    python_callable=geojson_to_geobuf,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert geojson to geobuf
geobuf_zip = PythonOperator(
    task_id='geobuf_to_zip',
    python_callable=geobuf_to_gzip,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Convert geojson to geobuf
shape_zip = PythonOperator(
    task_id='shape_to_zip',
    python_callable=shp_to_zip,
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    dag=dag)

#: Upload shp GIS file to S3
upload_shp_file = S3FileTransferOperator(
    task_id='tree_canopy_shp_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='tree_canopy_datasd.zip',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='sde/tree_canopy_datasd.zip',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload geojson GIS file to S3
upload_geojson_file = S3FileTransferOperator(
    task_id='tree_canopy_geojson_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='tree_canopy_datasd.geojson',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='sde/tree_canopy_datasd.geojson',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Upload topojson GIS file to S3
upload_pbf_file = S3FileTransferOperator(
    task_id='tree_canopy_pbf_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='tree_canopy_datasd.pbf',
    dest_s3_conn_id=conf['default_s3_conn_id'],
    dest_s3_bucket=conf['dest_s3_bucket'],
    dest_s3_key='sde/tree_canopy_datasd.pbf',
    on_failure_callback=notify,
    on_retry_callback=notify,
    on_success_callback=notify,
    replace=True,
    dag=dag)

#: Update portal modified date
update_gis_md = get_seaboard_update_dag('tree-canopy-2014.md', dag)

#: Execution order

#: Latest only operator must run before getting tree canopy data
get_shapefiles.set_upstream(treecan_latest_only)

#: get_shapefiles must run before converting to geojson
shp_to_geojson.set_upstream(get_shapefiles)

#: to_geojson must run before converting to geobuf
geojson_to_geobuf.set_upstream(shp_to_geojson)

#: to_geobuf must run before zipping geobuf
geobuf_zip.set_upstream(geojson_to_geobuf)

#: get_shapefile must run before zipping shapefile
shape_zip.set_upstream(get_shapefiles)

#: zipping shapefile must run before uploading
upload_shp_file.set_upstream(shape_zip)

#: converting to geojson must run before uploading
upload_geojson_file.set_upstream(shp_to_geojson)

#: zip geobuf must run before uploading
upload_pbf_file.set_upstream(geobuf_zip)

#: upload pbf must run before updating dataset on github
update_gis_md.set_upstream(upload_pbf_file)