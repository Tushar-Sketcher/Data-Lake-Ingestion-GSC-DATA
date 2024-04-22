import yaml
from dateutil.parser import parse
from pathlib import Path
from airflow.models import DAG, Variable
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.zacs_plugin import ZacsSparkSubmitOperator


environment = Variable.get('env')

global_config = yaml.safe_load(
    open(Path(__file__).absolute().parent.joinpath('global_config.yaml')))
config = {**global_config['common'], **global_config[environment]}
version = Path(__file__).absolute().parent.joinpath('VERSION').read_text()
script_loc = config['script_path'].format(version=version)

default_args = {
    'owner': 'big-data-engineering',
    'depends_on_past': False,
    'start_date': parse(config['start_date']),
    'email': config['email'].split(','),
    'email_on_failure': config['email_on_failure'],
    'email_on_retry': config['email_on_retry'],
    'retries': config['retries'],
    'retry_delay': timedelta(minutes=config['retry_delay'])
}

dag_name = 'gsc_data_daily'
dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval=config['schedule_interval'],
    max_active_runs=config['max_active_runs'],
    concurrency=config['concurrency']
)
dag.doc_md = __doc__


def raw_to_datalake(parent_dag, table_obj):
    src_location = "s3://{src}{path}{table}/{current_date}". \
        format(src=table_obj["src"], path=table_obj["path"],
               table=table_obj["table"],
               current_date="{{ macros.ds_format(ds, '%Y-%m-%d', "
                            "'%Y/%m/%d') }}")
    dest_location = "s3://{dest}{path}{table}/snapshotdate={current_date}". \
        format(dest=table_obj["dest"], path=table_obj["path"],
               table=table_obj["table"], current_date='{{ ds }}')

    app_arguments = [
        '--assume_role', config['assume_role'],
        '--table_name', table_obj["table"],
        '--partition', '{{ ds }}',
        '--src_location', src_location,
        '--dest_location', dest_location
    ]

    raw_to_datalake = ZacsSparkSubmitOperator(
        task_id="raw_to_datalake_{table}".format(table=table_obj["table"]),
        zodiac_environment=environment,
        storage_role_arn=config['assume_role'],
        zodiac_info=config['zodiac_config'],
        image=config['spark_docker_image'],
        app_arguments=app_arguments,
        spark_file='{}/raw_to_datalake.py'.format(script_loc),
        dag=parent_dag)

    return raw_to_datalake


start_task = DummyOperator(dag=dag, task_id="start",
                           execution_timeout=timedelta(minutes=1))

end_task = DummyOperator(dag=dag, task_id="end",
                         execution_timeout=timedelta(minutes=1))

tables = config['tables'].split(',')
table_obj = {}
for table in tables:
    table_obj["table"] = table
    table_obj["src"] = config['raw_bucket']
    table_obj["dest"] = config['datalake_bucket']
    table_obj["path"] = config['path']
    raw_to_datalake_task = raw_to_datalake(dag, table_obj)

    start_task >> raw_to_datalake_task >> end_task
