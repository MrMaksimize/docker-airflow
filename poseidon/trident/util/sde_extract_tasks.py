"""Dynamically create sde tasks."""
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from trident.operators.s3_file_transfer_operator import S3FileTransferOperator

from airflow.operators.latest_only_operator import LatestOnlyOperator

from airflow.models import DAG
from trident.util.notifications import notify

from trident.util.seaboard_updates import update_seaboard_date, get_seaboard_update_dag

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
    #: Latest Only Operator for sde layer
    sde_latest_only = LatestOnlyOperator(task_id='{layer}_latest_only'
                                         .format(layer=layer),
                                         dag=dag)

    #: Convert sde table to shapefile format
    to_shp = PythonOperator(
        task_id='{layer}_to_shp'.format(layer=layer),
        python_callable=sde_to_shp,
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Convert shapefile to GeoJSON format
    to_geojson = BashOperator(
        task_id='{layer}_to_geojson'.format(layer=layer),
        bash_command=shp_to_geojson(path_to_file),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Convert shapefile to TopoJSON format
    to_topojson = BashOperator(
        task_id='{layer}_to_topojson'.format(layer=layer),
        bash_command=shp_to_topojson(path_to_file),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Compress shapefile components
    to_zip = PythonOperator(
        task_id='{layer}_shp_to_zip'.format(layer=layer),
        python_callable=shp_to_zip,
        op_kwargs={'datasd_name': datasd_name},
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        dag=dag)

    #: Upload shapefile to S3
    shp_to_S3 = S3FileTransferOperator(
        task_id='{layer}_shp_to_S3'.format(layer=layer),
        source_base_path=conf['prod_data_dir'],
        source_key='{datasd_name}.zip'.format(datasd_name=datasd_name),
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key='sde/{folder}/{datasd_name}.zip'
                    .format(folder=folder, datasd_name=datasd_name),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)

    #: Upload geojson to S3
    geojson_to_S3 = S3FileTransferOperator(
        task_id='{layer}_geojson_to_S3'.format(layer=layer),
        source_base_path=conf['prod_data_dir'],
        source_key='{datasd_name}.geojson'.format(datasd_name=datasd_name),
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key='sde/{folder}/{datasd_name}.geojson'
                    .format(folder=folder, datasd_name=datasd_name),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)

    #: Upload topojson to S3
    topojson_to_S3 = S3FileTransferOperator(
        task_id='{layer}_topojson_to_S3'.format(layer=layer),
        source_base_path=conf['prod_data_dir'],
        source_key='{datasd_name}.topojson'.format(datasd_name=datasd_name),
        dest_s3_conn_id=conf['default_s3_conn_id'],
        dest_s3_bucket=conf['dest_s3_bucket'],
        dest_s3_key='sde/{folder}/{datasd_name}.topojson'
                    .format(folder=folder, datasd_name=datasd_name),
        on_failure_callback=notify,
        on_retry_callback=notify,
        on_success_callback=notify,
        replace=True,
        dag=dag)

    #: Update portal modified date
    update_md = get_seaboard_update_dag('{md}.md'.format(md=md), dag)

    if layer not in no_pbf:
        #: Convert GeoJSON to Geobuf format
        to_geobuf = PythonOperator(
            task_id='{layer}_to_geobuf'.format(layer=layer),
            python_callable=geojson_to_geobuf,
            op_kwargs={'path_to_file': path_to_file},
            on_failure_callback=notify,
            on_retry_callback=notify,
            on_success_callback=notify,
            dag=dag)

        #: Convert geobuf to gzipped geobuf
        to_gzip = PythonOperator(
            task_id='{layer}_geobuf_to_gzip'.format(layer=layer),
            python_callable=geobuf_to_gzip,
            op_kwargs={'datasd_name': datasd_name},
            on_failure_callback=notify,
            on_retry_callback=notify,
            on_success_callback=notify,
            dag=dag)

        #: Upload geobuf to S3
        geobuf_to_S3 = S3FileTransferOperator(
            task_id='{layer}_geobuf_to_S3'.format(layer=layer),
            source_base_path=conf['prod_data_dir'],
            source_key='{datasd_name}.pbf'.format(datasd_name=datasd_name),
            dest_s3_conn_id=conf['default_s3_conn_id'],
            dest_s3_bucket=conf['dest_s3_bucket'],
            dest_s3_key='sde/{folder}/{datasd_name}.pbf'
                        .format(folder=folder, datasd_name=datasd_name),
            on_failure_callback=notify,
            on_retry_callback=notify,
            on_success_callback=notify,
            replace=True,
            use_gzip=True,
            dag=dag)

        #: Conversion to geobuf is triggered after conversion to geojson.
        to_geobuf.set_upstream(to_geojson)

        #: Compression to gzip is triggered after conversion to geobuf.
        to_gzip.set_upstream(to_geobuf)

        #: geobuf upload to S3 is triggered after compression to gzipped geobuf.
        geobuf_to_S3.set_upstream(to_gzip)

        #: Github update depends on shapefile S3 upload success.
        update_md.set_upstream(geobuf_to_S3)

    #: Execution rules:
    #: sde_latest_only must run before shp conversion.
    to_shp.set_upstream(sde_latest_only)

    #: Conversion to geojson is triggered after conversion to shp.
    to_geojson.set_upstream(to_shp)

    #: Conversion to topojson is triggered after conversion to shapefile.
    to_topojson.set_upstream(to_shp)

    #: Compression to zip is triggered after conversion to geojson and topojson.
    to_zip.set_upstream(to_geojson)
    to_zip.set_upstream(to_topojson)

    #: shapefile upload to S3 is triggered after conversion to zip.
    shp_to_S3.set_upstream(to_zip)

    #: geojson upload to S3 is triggered after conversion to geojson.
    geojson_to_S3.set_upstream(to_geojson)

    #: topojson upload to S3 is triggered after conversion to topojson.
    topojson_to_S3.set_upstream(to_topojson)

    #: Github update depends on shapefile S3 upload success.
    update_md.set_upstream(shp_to_S3)
    update_md.set_upstream(geojson_to_S3)
    update_md.set_upstream(topojson_to_S3)
