# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'dummy_name',
    'start_date': days_ago(0), #today,
    'email': ['dummies@email.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks
# define the first task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/tolldata.tgz -C /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment ',
    dag=dag,
)


extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/vehicle-data.csv > /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/csv_data.csv ',
    dag=dag,
)


extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 --output-delimiter="," /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/tollplaza-data.tsv > /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/tsv_data.csv ',
    dag=dag,
)

extract_data_from_fixed_width=BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command='cut -c59-62,63-68 --output-delimiter="," /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/payment-data.txt > /home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment/fixed_width_data.csv' ,
    dag=dag
)

HOME_URL="/home/rafaelrb/DataEngineer/apache_airflow/dags/finalassignment"
consolidate_data=BashOperator(
    task_id="consolidate_data",
    bash_command= f'paste -d"," {HOME_URL}/csv_data.csv {HOME_URL}/tsv_data.csv {HOME_URL}/fixed_width_data.csv > {HOME_URL}/extracted_data.csv',
    dag=dag
)

transform_data=BashOperator(
    task_id="transform_data",
    bash_command= f'cat {HOME_URL}/extracted_data.csv | tr [:lower:] [:upper:] > {HOME_URL}/transformed_data.csv',
    dag=dag
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data