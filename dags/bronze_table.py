from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, and_, delete
from sqlalchemy.orm import sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


def extract_data():
    print("Extract da tabela bronze_produtos")


def delete_null_titles():
    hook = PostgresHook(postgres_conn_id="postgres_render")
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(bind=engine)
    tabela_origem = Table("bronze_produtos", metadata, autoload=True)
    delete_stmt = delete(tabela_origem).where(tabela_origem.c.titulo == None)
    engine.execute(delete_stmt)


def delete_negative_values():
    hook = PostgresHook(postgres_conn_id="postgres_render")
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData(bind=engine)
    tabela_origem = Table("bronze_produtos", metadata, autoload=True)
    delete_stmt = delete(tabela_origem).where(tabela_origem.c.preco < 0)
    engine.execute(delete_stmt)


def transformation_completed():
    print("Transformação realizada")


def transfer_and_delete_data():
    hook = PostgresHook(postgres_conn_id="postgres_render")
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData(bind=engine)
    tabela_origem = Table("bronze_produtos", metadata, autoload=True)
    tabela_destino = Table("produtos", metadata, autoload=True)

    query = tabela_origem.select()
    dados = session.execute(query).fetchall()

    for dado in dados:
        insercao = tabela_destino.insert().values(
            titulo=dado.titulo, descricao=dado.descricao, preco=dado.preco
        )
        session.execute(insercao)

    delete_stmt = delete(tabela_origem)
    session.execute(delete_stmt)

    session.commit()
    session.close()


def load_completed():
    print("Load finalizado na tabela produtos")


with DAG(
    "transfer_data_dag_v9",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_data_task", python_callable=extract_data
    )
    delete_null_titles_task = PythonOperator(
        task_id="delete_null_titles_task", python_callable=delete_null_titles
    )
    delete_negative_values_task = PythonOperator(
        task_id="delete_negative_values_task", python_callable=delete_negative_values
    )
    transformation_completed_task = PythonOperator(
        task_id="transformation_completed_task",
        python_callable=transformation_completed,
    )
    transfer_and_delete_task = PythonOperator(
        task_id="transfer_and_delete_task", python_callable=transfer_and_delete_data
    )
    load_completed_task = PythonOperator(
        task_id="load_completed_task", python_callable=load_completed
    )

    (
        extract_task
        >> delete_null_titles_task
        >> delete_negative_values_task
        >> transformation_completed_task
        >> transfer_and_delete_task
        >> load_completed_task
    )
