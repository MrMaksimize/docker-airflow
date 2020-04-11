"""_dags file for 'right of way' sde extraction."""
from airflow.models import DAG
from trident.util import general
from trident.util.sde_extract_tasks import create_sde_tasks
from dags.sde.row_jobs import sde_to_shp


args = general.args
conf = general.config
schedule = general.schedule['gis_weekly']
start_date = general.start_date['gis_weekly']
folder = 'right_of_way'
layer = 'right_of_way'
datasd_name = 'right_of_way_datasd'
md = 'right-of-way'
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
