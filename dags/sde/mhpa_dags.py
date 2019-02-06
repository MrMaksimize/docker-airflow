"""_dags file for 'mhpa' sde extraction."""
from airflow.models import DAG
from poseidon.util import general
from poseidon.dags.sde.mhpa_jobs import sde_to_shp
from poseidon.util.sde_extract_tasks import create_sde_tasks


args = general.args
conf = general.config
schedule = general.schedule['gis_weekly']
folder = 'mhpa'
layer = 'mhpa'
datasd_name = 'mhpa_areas_datasd'
md = 'mhpa-areas'
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
