"""_dags file for tree canopy sde extraction."""
from airflow.models import DAG
from trident.util import general
from dags.sde.tree_can_jobs import sde_to_shp
from trident.util.sde_extract_tasks import create_sde_tasks


args = general.args
conf = general.config
schedule = general.schedule['gis_tree_canopy']
start_date = general.start_date['gis_tree_canopy']
folder = 'trees'
layer = 'tree_canopy'
datasd_name = 'tree_canopy_datasd'
md = 'tree-canopy-2014'
path_to_file = conf['prod_data_dir'] + '/' + datasd_name

dag = DAG(dag_id='gis_{layer}'.format(layer=layer),
          default_args=args,
          start_date=start_date,
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
