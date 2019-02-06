"""_dags file for 'right of way' sde extraction."""
from airflow.models import DAG
from poseidon.util import general
from poseidon.util.sde_extract_tasks import create_sde_tasks
from poseidon.dags.sde.row_jobs import sde_to_shp


args = general.args
conf = general.config
schedule = general.schedule['gis_weekly']
folder = 'right_of_way'
layer = 'right_of_way'
datasd_name = 'right_of_way_datasd'
md = 'right-of-way'
path_to_file = conf['prod_data_dir'] + '/' + datasd_name

dag = DAG(dag_id='gis_{layer}'.format(layer=layer),
          default_args=args,
          schedule_interval=schedule)


#: Create tasks dynamically
create_sde_tasks(
    dag=dag,
    folder=folder,
    layer=layer,
    datasd_name=datasd_name,
    md=md,
    path_to_file=path_to_file,
    sde_to_shp=sde_to_shp)
