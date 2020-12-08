"""Dynamically create sde tasks."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator


from airflow.models import DAG
from trident.util.notifications import afsys_send_email

from trident.util.seaboard_updates import *

from trident.util.geospatial import *

from trident.util.general import config as conf


no_pbf = ('addrapn')


def shp_to_geojson(path_to_file):
    """Shapefile to GeoJSON."""
    cmd = shp2geojson(path_to_file)
    return cmd


def shp_to_topojson(path_to_file):
    """Shapefile to TopoJSON."""
    cmd = shp2topojson(path_to_file)
    return cmd


def geojson_to_geobuf(path_to_file):
    """Geojson to Geobuf."""
    geojson2geobuf(layer=path_to_file)
    return 'Successfully converted geojson to geobuf.'


def geobuf_to_gzip(datasd_name):
    """Geobuf to gzip."""
    geobuf2gzip(layername=datasd_name)
    return 'Successfully compressed geobuf.'


def shp_to_zip(datasd_name):
    """Shapefile to zip."""
    shp2zip(layername=datasd_name)
    return 'Successfully transfered shapefiles to zip archive.'


def create_sde_tasks(dag,
                     folder,
                     layer,
                     datasd_name,
                     md,
                     path_to_file,
                     sde_to_shp):
    """Dynamically create SDE Airflow tasks.

    dag: DAG defined in _dags file.
    folder: subfolder in the sde folder on S3.
    layer: layer name.
    datasd_name: layer name + _datasd.
    md: name of md file on Seaboard.
    path_to_file: poseidon path + datasd_name.
    sde_to_shp: _jobs specific sde_to_shp function
    """

    #: Convert sde table to shapefile format
    to_shp = PythonOperator(
        task_id=f'{layer}_to_shp',
        python_callable=sde_to_shp,
        dag=dag)

    #: Convert shapefile to GeoJSON format
    to_geojson = BashOperator(
        task_id=f'{layer}_to_geojson',
        bash_command=shp_to_geojson(path_to_file),
        dag=dag)

    #: Convert shapefile to TopoJSON format
    to_topojson = BashOperator(
        task_id=f'{layer}_to_topojson',
        bash_command=shp_to_topojson(path_to_file),
        dag=dag)

    #: Compress shapefile components
    to_zip = PythonOperator(
        task_id=f'{layer}_shp_to_zip',
        python_callable=shp_to_zip,
        op_kwargs={'datasd_name': datasd_name},
        dag=dag)

    #: Upload shapefile to S3
    shp_to_S3 = S3FileTransferOperator(
        task_id=f'{layer}_shp_to_S3',
        source_base_path=conf['prod_data_dir'],
        source_key=f'{datasd_name}.zip',
        dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
        dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
        dest_s3_key=f'sde/{folder}/{datasd_name}.zip',
        replace=True,
        dag=dag)

    #: Upload geojson to S3
    geojson_to_S3 = S3FileTransferOperator(
        task_id=f'{layer}_geojson_to_S3',
        source_base_path=conf['prod_data_dir'],
        source_key=f'{datasd_name}.geojson',
        dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
        dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
        dest_s3_key=f'sde/{folder}/{datasd_name}.geojson',
        replace=True,
        dag=dag)

    #: Upload topojson to S3
    topojson_to_S3 = S3FileTransferOperator(
        task_id=f'{layer}_topojson_to_S3',
        source_base_path=conf['prod_data_dir'],
        source_key=f'{datasd_name}.topojson',
        dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
        dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
        dest_s3_key=f'sde/{folder}/{datasd_name}.topojson',
        replace=True,
        dag=dag)

    #: Update portal modified date
    update_md = get_seaboard_update_dag(f'{md}.md', dag)

    if layer not in no_pbf:
        #: Convert GeoJSON to Geobuf format
        to_geobuf = PythonOperator(
            task_id=f'{layer}_to_geobuf',
            python_callable=geojson_to_geobuf,
            op_kwargs={'path_to_file': path_to_file},
            dag=dag)

        #: Convert geobuf to gzipped geobuf
        to_gzip = PythonOperator(
            task_id=f'{layer}_geobuf_to_gzip',
            python_callable=geobuf_to_gzip,
            op_kwargs={'datasd_name': datasd_name},
            dag=dag)

        #: Upload geobuf to S3
        geobuf_to_S3 = S3FileTransferOperator(
            task_id=f'{layer}_geobuf_to_S3',
            source_base_path=conf['prod_data_dir'],
            source_key=f'{datasd_name}.pbf',
            dest_s3_conn_id="{{ var.value.DEFAULT_S3_CONN_ID }}",
            dest_s3_bucket="{{ var.value.S3_DATA_BUCKET }}",
            dest_s3_key=f'sde/{folder}/{datasd_name}.pbf',
            replace=True,
            use_gzip=True,
            dag=dag)

        to_geojson >> to_geobuf >> to_gzip >> geobuf_to_S3 >> update_md


    #: Execution rules:

    to_shp >> [to_geojson,to_topojson] >> to_zip >> shp_to_S3
    to_geojson >> geojson_to_S3
    to_topojson >> topojson_to_S3
    [shp_to_S3,geojson_to_S3,topojson_to_S3] >> update_md
