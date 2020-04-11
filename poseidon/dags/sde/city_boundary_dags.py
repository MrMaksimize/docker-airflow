"""_dags file for 'city boundary' sde extraction."""
from airflow.models import DAG
from trident.util import general
from dags.sde.city_boundary_jobs import sde_to_shp
from trident.util.sde_extract_tasks import create_sde_tasks


args = general.args
conf = general.config
schedule = general.schedule['gis_weekly']
start_date = general.start_date['gis_weekly']
folder = 'city_boundary'
layer = 'city_boundary'
datasd_name = 'san_diego_boundary_datasd'
md = 'san-diego-boundary'
path_to_file = conf['prod_data_dir'] + '/' + datasd_name

dag = DAG(dag_id='gis_{layer}'.format(layer=layer),
		  start_date=start_date,
          default_args=args,
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
