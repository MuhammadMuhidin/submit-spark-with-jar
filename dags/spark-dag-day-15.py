from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'dibimbing',
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id='spark_airflow_dag',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description='Exercise for spark submit',
    start_date=days_ago(1),
)

Extract = SparkSubmitOperator(
    application='/spark-scripts/spark-script-day-15.py',
    conn_id='spark_tgs',
    task_id='spark_submit_task',
    # Mention the .jar file to the driver class path for Spark
    driver_class_path='/spark-scripts/postgresql-42.2.18.jar',
    dag=spark_dag,
)

Extract
