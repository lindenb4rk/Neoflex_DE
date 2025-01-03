# from airflow import DAG
# from airflow.operators.dummy_operatopr import DummyOperator





from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.configuration import conf
from airflow.models import Variable

from time import sleep

PATH = Variable.get("my_path")

conf.set("core", "template_searchpath",PATH)

import pandas
from datetime import datetime

# функция используется только для ft_postings_f.cvs тк там нет индекса
def insert_data2(table_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv",delimiter=";")
 #   df = pandas.read_csv(f"/files/{table_name}.csv", delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name,engine,schema="ds",if_exists="replace",index=False)
    sleep(5)

# ещё одна функция нужна для таблицы md_currency_d, тк там с данными есть ошибка кодировки
# разница только в функции pandas.read_csv(,,encoding='cp1252')
def insert_data1(table_name,index_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv",delimiter=";",encoding='cp1252')
 #   df = pandas.read_csv(f"/files/{table_name}.csv", delimiter=";",encoding='cp1252')
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.set_index(index_name)
    df.to_sql(table_name,engine,schema="ds",if_exists="replace",index=False)
    sleep(5)


def insert_data(table_name,index_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv",delimiter=";")
 #   df = pandas.read_csv(f"/files/{table_name}.csv", delimiter=";")
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.set_index(index_name)
    df.to_sql(table_name,engine,schema="ds",if_exists="replace",index=False)
    sleep(5)

default_args = {
    "owner" : "mlickov",
    "start_date" : datetime(2024,2,25),
    "retries": 2
}

with DAG (
    "insert_data_in_to_DS",
    default_args=default_args,
    description="Загрузка данных в DS",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 * * *"
 ) as dag:

    start = EmptyOperator(
        task_id = "start"
    )

    ft_balance_f = PythonOperator(
        task_id = "ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name" : "ft_balance_f","index_name" : ["ON_DATE","ACCOUNT_RK"]}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data2,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_account_d","index_name" : ["DATA_ACTUAL_DATE", "ACCOUNT_RK"]}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data1,
        op_kwargs={"table_name": "md_currency_d","index_name" : ["CURRENCY_RK", "DATA_ACTUAL_DATE"]}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_exchange_rate_d","index_name" : ["DATA_ACTUAL_DATE", "CURRENCY_RK"]}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name": "md_ledger_account_s","index_name" : ["LEDGER_ACCOUNT", "START_DATE"]}
    )


    # split = EmptyOperator(
    #     task_id="split"
    # )

    # here SQL operators

    sql_ft_posting_f_start = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f_start",
        conn_id="postgres-db",
        sql="sql_logs/ft_posting_f_start.sql"
    )

    sql_ft_balance_f_start = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f_start",
        conn_id="postgres-db",
        sql="sql_logs/ft_balance_f_start.sql"
    )
    sql_md_account_d_start = SQLExecuteQueryOperator(
        task_id="sql_md_account_d_start",
        conn_id="postgres-db",
        sql="sql_logs/md_account_d_start.sql"
    )

    sql_md_currency_d_start = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d_start",
        conn_id="postgres-db",
        sql="sql_logs/md_currency_d_start.sql"
    )

    sql_md_exchange_rate_d_start = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d_start",
        conn_id="postgres-db",
        sql="sql_logs/md_exchange_rate_d_start.sql"
    )

    sql_md_ledger_account_s_start = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s_start",
        conn_id="postgres-db",
        sql="sql_logs/md_ledger_account_s_start.sql"
    )


    end = EmptyOperator(
        task_id = "end"
    )
    sql_ft_posting_f_end = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f_end",
        conn_id="postgres-db",
        sql="sql_logs/ft_posting_f_end.sql"
    )

    sql_ft_balance_f_end = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f_end",
        conn_id="postgres-db",
        sql="sql_logs/ft_balance_f_end.sql"
    )
    sql_md_account_d_end = SQLExecuteQueryOperator(
        task_id="sql_md_account_d_end",
        conn_id="postgres-db",
        sql="sql_logs/md_account_d_end.sql"
    )

    sql_md_currency_d_end = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d_end",
        conn_id="postgres-db",
        sql="sql_logs/md_currency_d_end.sql"
    )

    sql_md_exchange_rate_d_end = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d_end",
        conn_id="postgres-db",
        sql="sql_logs/md_exchange_rate_d_end.sql"
    )

    sql_md_ledger_account_s_end = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s_end",
        conn_id="postgres-db",
        sql="sql_logs/md_ledger_account_s_end.sql"
    )

    start >> sql_ft_posting_f_start >> ft_posting_f >> sql_ft_posting_f_end >> end
    start >> sql_ft_balance_f_start >> ft_balance_f >> sql_ft_balance_f_end >> end
    start >> sql_md_account_d_start >> md_account_d >> sql_md_account_d_end >> end
    start >> sql_md_currency_d_start >> md_currency_d >> sql_md_currency_d_end >> end
    start >> sql_md_exchange_rate_d_start >> md_exchange_rate_d >> sql_md_exchange_rate_d_end >> end
    start >> sql_md_ledger_account_s_start >> md_ledger_account_s >> sql_md_ledger_account_s_end >> end
   # >>[ft_balance_f,ft_posting_f,md_account_d,md_currency_d,md_exchange_rate_d,md_ledger_account_s]
    # >>[[sql_ft_posting_f_start,ft_posting_f], [sql_ft_balance_f_start,ft_balance_f], [sql_md_account_d_start,md_account_d],[sql_md_currency_d_start,md_currency_d], [sql_md_exchange_rate_d_start,md_exchange_rate_d], [sql_md_ledger_account_s_start,md_ledger_account_s] ]
# for use SQL operators
#    >>split
#    >>[sql_ft_balance_f,sql_ft_posting_f]
#    >> end




