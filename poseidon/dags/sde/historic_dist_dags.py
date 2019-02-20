"""_dags file for 'HistoricDistricts' sde extraction."""
from airflow.models import DAG
from poseidon.util import general
from poseidon.dags.sde.historic_dist_jobs import sde_to_shp
from poseidon.util.sde_extract_tasks import create_sde_tasks


args = general.args
conf = general.config
schedule = general.schedule['gis_weekly']
folder = 'historic_districts'
layer = 'historic_districts'
datasd_name = 'historic_districts_datasd'
md = 'historic-districts'
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
