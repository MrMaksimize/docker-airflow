"""_dags file for tree canopy sde extraction."""
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator
from trident.operators.poseidon_sonar_operator import PoseidonSonarCreator
from airflow.models import DAG

from trident.util import general
from trident.util.notifications import afsys_send_email
from trident.util.geospatial import *
from dags.sde.tree_can_jobs import *
from trident.util.seaboard_updates import *

args = general.args
conf = general.config
schedule = general.schedule['gis_tree_canopy']
start_date = general.start_date['gis_tree_canopy']
folder = 'trees'
layer = 'tree_canopy'
datasd_name = 'tree_canopy_datasd'
path_to_file = f"{conf['prod_data_dir']}/{datasd_name}"

dag = DAG(dag_id=f'gis_{layer}',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule,
          catchup=False)

#: Get tree canopy shapefile from Atlas
get_shapefiles = PythonOperator(
    task_id='get_tree_canopy_gis',
    python_callable=sde_to_shp,
    
    dag=dag)

#: Convert shp to geojson
shp_to_geojson = BashOperator(
    task_id='tree_canopy_to_geojson',
    bash_command=shp_to_geojson(),
    
    dag=dag)

#: Convert geojson to geobuf
geojson_to_geobuf = PythonOperator(
    task_id='tree_canopy_to_geobuf',
    python_callable=geojson_to_geobuf,
    
    dag=dag)

#: Convert geojson to geobuf
geobuf_zip = PythonOperator(
    task_id='geobuf_to_zip',
    python_callable=geobuf_to_gzip,
    
    dag=dag)

#: Convert geojson to geobuf
shape_zip = PythonOperator(
    task_id='shape_to_zip',
    python_callable=shp_to_zip,
    
    dag=dag)

#: Upload shp GIS file to S3
upload_shp_file = S3FileTransferOperator(
    task_id='tree_canopy_shp_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='tree_canopy_datasd.zip',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='sde/tree_canopy_datasd.zip',
    
    replace=True,
    dag=dag)

#: Upload geojson GIS file to S3
upload_geojson_file = S3FileTransferOperator(
    task_id='tree_canopy_geojson_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='tree_canopy_datasd.geojson',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='sde/tree_canopy_datasd.geojson',
    
    replace=True,
    dag=dag)

#: Upload topojson GIS file to S3
upload_pbf_file = S3FileTransferOperator(
    task_id='tree_canopy_pbf_to_S3',
    source_base_path=conf['prod_data_dir'],
    source_key='tree_canopy_datasd.pbf',
    dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
    dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
    dest_s3_key='sde/tree_canopy_datasd.pbf',
    
    replace=True,
    dag=dag)

#: Execution order
get_shapefiles >> shp_to_geojson >> geojson_to_geobuf >> geobuf_zip
get_shapefiles >> shape_zip >> upload_shp_file
shp_to_geojson >> upload_geojson_file
geobuf_zip >> upload_pbf_file