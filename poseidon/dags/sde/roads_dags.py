"""_dags file for 'roads' sde extraction."""
from airflow.models import DAG
from trident.util import general
from dags.sde.roads_jobs import sde_to_shp
from trident.util.sde_extract_tasks import create_sde_tasks


args = general.args
conf = general.config
schedule = general.schedule['gis_weekly']
start_date = general.start_date['gis_weekly']
folder = 'roads_all'
layer = 'roads'
datasd_name = 'roads_datasd'
md = 'roads-lines'
path_to_file = f"{conf['prod_data_dir']}/{datasd_name}"

dag = DAG(dag_id=f'gis_{layer}',
          default_args=args,
          start_date=start_date,
          schedule_interval=schedule,
          catchup=False)


#: Create tasks dynamically
create_sde_tasks(
    dag=dag,
    folder=folder,
    layer=layer,
    datasd_name=datasd_name,
    md=md,
    path_to_file=path_to_file,
    sde_to_shp=sde_to_shp)
