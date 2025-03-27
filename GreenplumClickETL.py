# импорт необходимых библиотек
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from clickhouse_driver import Client
from airflow.operators.python import PythonOperator

# запросы на загрузку данных таблиц фактов
LOAD_BILLS_HEAD_PARTITION = "select std10_44_1.f_load_delta_partitions1('std10_44_1.bills_head', 'std10_44_1.bills_head_ext','calday','2021-01-01','2021-03-01')"

LOAD_BILLS_ITEM_PARTITION = "select std10_44_1.f_load_delta_partitions1('std10_44_1.bills_item','std10_44_1.bills_item_ext', 'calday', '2021-01-01', '2021-03-01')"

LOAD_TRAFFIC_PARTITION = "select std10_44_1.f_load_delta_traffic('std10_44_1.traffic', 'gp.traffic', 'date', '2021-01-01', '2021-03-01')"

# запросы на создание витрины данных: общей, по дням, по месяцам
LOAD_DATA_MART_TOTAL = "select std10_44_1.f_load_mart_total('2021-03-01')"
LOAD_DATA_MART_MONTH = "select std10_44_1.f_load_mart_month('2021-03-01')"
LOAD_DATA_MART_DAY = "select std10_44_1.f_load_mart_day('2021-03-01')"
# названия схем подключения и способы подключения
DB_CONN = "gp_std10_44"
DB_SCHEMA = "std10_44_1"
CH_SCHEMA = "std10_44"
CH_CLIENT = Client(
    host='**',
    port='9000',
    database=CH_SCHEMA,
    user='std10_44',
    password='**',
)
# список столбцов для загрузки таблиц справочников и запрос на их создание
referencedata_tables = ['stores', 'promos', 'promo_types', 'coupons']
LOAD_REFERENCEDATA = "select std10_44_1.f_full_load(%(tab_name)s,%(file_name)s)"

# очистка таблицы в ClickHouse и создание с загрузкой этой же таблицы
CH_DROP_TABLE = 'DROP TABLE IF EXISTS std10_44.data_mart_total'
CH_CREATE_TABLE = f'''
CREATE TABLE std10_44.data_mart_total
(

    `plant` String,

    `turnover` Float32,

    `total_discount` Float32,

    `net_turnover` Float32,

    `quantity` Float64,

    `total_bills` Int32,

    `total_traffic` Int32,

    `total_coupons` Int32,

    `Share of discounted items` Float32,

    `Average number of items in a receipt` Float64,

    `Store conversion rate` Float32,

    `Average check` Float32,

    `turnover per visitor` Float32
)
ENGINE = PostgreSQL('host:port',
 'dbname',
 'tablename',
 'login',
 'pass',
 'schemaname');
'''

# функция для обращения запросов к clickhouse
def ch_action(client, query):
    try:
        result = client.execute(query)
        print(f"Query executed successfully: {result}")
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        raise

# словарь с настройками Dag
default_args = {
    'depends_on_past': False,
    'owner': 'std10_44_1',
    'start_date': datetime(2025, 3, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG( # объявление дага
    'std10_44_1_final',  # название дага
    max_active_runs=4, # максимальное число одновременно запущенных дагов
    schedule_interval=None, # можно задать автоматический запуск дага, None-запуск только вручную 
    default_args=default_args, # словарь, содержит параметры по умолчанию в задачах Даг
    catchup=False, # если даг не запускался какое то время или был пропущен запуск дага, то он запустится если стоит True
) as dag:
    task_start = DummyOperator(task_id="start") # пустой оператор, просто пишет start

    # запуск PostgresOperator на загрузку таблиц фактов
    task_bills_head_load = PostgresOperator(
        task_id="start_bills_head_load",
        postgres_conn_id="gp_std10_44",
        sql=LOAD_BILLS_HEAD_PARTITION,
    )
    task_bills_item_load = PostgresOperator(
        task_id="start_bills_item_load",
        postgres_conn_id="gp_std10_44",
        sql=LOAD_BILLS_ITEM_PARTITION,
    )

    task_traffic_load = PostgresOperator(
        task_id="start_traffic_load",
        postgres_conn_id="gp_std10_44",
        sql=LOAD_TRAFFIC_PARTITION,
    )


    
    # запуск в контекстном менеджере оператора PostgresOperator для вызова запроса LOAD_REFERENCEDATA для каждой таблицы справочника
    with TaskGroup('load_reference_tables') as load_reference_tables:
        for tablename in referencedata_tables:
            task = PostgresOperator(
                task_id=f"load_table_{tablename}",
                postgres_conn_id="gp_std10_44",
                sql=LOAD_REFERENCEDATA,
                parameters={'tab_name': tablename, 'file_name': tablename},
            )
    # запуск тасков по расчету витрин данных: общей, месяцам, дням
    task_load_mart_total = PostgresOperator(
        task_id="start_total_mart_load",
        postgres_conn_id="gp_std10_44",
        sql=LOAD_DATA_MART_TOTAL,
    )
    task_load_mart_month = PostgresOperator(
        task_id="start_month_mart_load",
        postgres_conn_id="gp_std10_44",
        sql=LOAD_DATA_MART_MONTH,
    )
    task_load_mart_day = PostgresOperator(
        task_id="start_day_mart_load",
        postgres_conn_id="gp_std10_44",
        sql=LOAD_DATA_MART_DAY,
    )
    # таски по удаление витрины из clickhouse и созданию таблицы с данными из Greenplum
    task_drop = PythonOperator(
        task_id='ch_drop_mart',
        python_callable=ch_action,
        op_kwargs={'client': CH_CLIENT, 'query': CH_DROP_TABLE},
    )

    task_create_mart = PythonOperator(
        task_id='ch_create_mart',
        python_callable=ch_action,
        op_kwargs={'client': CH_CLIENT, 'query': CH_CREATE_TABLE},
    )
    
    # тоже пустой оператор показывает, что таски перед ним завершились
    task_end = DummyOperator(task_id="end")
    
# порядок запуска тасков
(
    task_start
    >> load_reference_tables
    >> [
        task_bills_head_load,
        task_bills_item_load,
        task_traffic_load,
    ]
    >> task_load_mart_total
    >> task_load_mart_month
    >> task_load_mart_day
    >> task_drop
    >> task_create_mart
    >> task_end
)
