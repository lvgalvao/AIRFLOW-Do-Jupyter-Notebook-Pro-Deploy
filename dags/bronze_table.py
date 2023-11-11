from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, and_
from sqlalchemy.orm import sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


def transfer_data():
    # Utilizar o PostgresHook do Airflow para a conexão
    hook = PostgresHook(postgres_conn_id="postgres_render")
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData(bind=engine)

    tabela_origem = Table("bronze_produtos", metadata, autoload=True)
    tabela_destino = Table("produtos", metadata, autoload=True)

    # Filtrar dados: removendo registros com título Null e preço < 0
    dados = (
        session.query(tabela_origem)
        .filter(and_(tabela_origem.c.titulo != None, tabela_origem.c.preco >= 0))
        .all()
    )

    # Inserir dados filtrados na tabela de destino, sem especificar o ID
    for dado in dados:
        insercao = tabela_destino.insert().values(
            titulo=dado.titulo, descricao=dado.descricao, preco=dado.preco
        )
        session.execute(insercao)

    session.commit()
    session.close()


with DAG(
    "transfer_data_dag_v8",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # Executar a cada 5 minutos
    catchup=False,
) as dag:
    transfer_data_task = PythonOperator(
        task_id="transfer_data_task", python_callable=transfer_data
    )

    transfer_data_task
