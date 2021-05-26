try:
    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    print("All dag modules are OK ..........")
except Exception as e:
    print("Error {}".format(e))


def first_func_execute(**context):
    print("first function execute ...")
    value = context.get("value", "Argument value not found")
    context['ti'].xcom_push(key="key", value="value={} from first_func".format(value))

def second_func_execute(**context):
    value = context['ti'].xcom_pull(key="key")
    print("function 2 received {}".format(value))


with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021, 5, 1),
    },
    catchup=False
) as f:
    first_func_execute = PythonOperator(
        task_id="first_func_execute",
        python_callable=first_func_execute,
        provide_context=True,
        op_kwargs={"value": "hakim"}
    )
    second_func_execute = PythonOperator(
        task_id="second_func_execute",
        python_callable=second_func_execute,
        provide_context=True
    )

first_func_execute >> second_func_execute