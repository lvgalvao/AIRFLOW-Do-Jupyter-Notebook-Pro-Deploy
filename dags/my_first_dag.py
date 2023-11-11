# Importação de módulos do Airflow e de outras bibliotecas padrão Python
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Configuração dos argumentos padrão que serão aplicados a todas as tarefas da DAG
default_args = {
    "owner": "luciano",  # Define o proprietário da DAG, útil para fins de rastreamento e permissão.
    "start_date": "2023-11-02",  # Data de início da execução da DAG. As execuções serão agendadas a partir desta data.
    "retries": 1,  # Número de tentativas de reexecução de uma tarefa em caso de falha.
    "retry_delay": timedelta(
        minutes=1
    ),  # Intervalo de tempo entre as tentativas de reexecução.
}

# Definição da DAG, seu ID, descrição, intervalo de agendamento, argumentos padrão e política de recuperação
with DAG(
    dag_id="newdag",  # Identificador único para a DAG.
    description="My first DAG",  # Descrição textual da DAG.
    schedule_interval="0 0 * * *",  # Intervalo de agendamento (aqui, diariamente à meia-noite).
    default_args=default_args,  # Aplicação dos argumentos padrão definidos acima.
    catchup=False,  # Determina se o Airflow realiza ou não a execução de datas passadas que foram perdidas (catchup).
) as dag:
    # Definição de uma tarefa usando BashOperator
    task1 = BashOperator(
        task_id="task1",  # Identificador único da tarefa dentro da DAG.
        bash_command='echo "Hello World!"',  # Comando Bash que a tarefa vai executar.
    )

    # Neste ponto, você pode definir mais tarefas e suas dependências.
    # Por exemplo: task2 = BashOperator(...), seguido de task1 >> task2 para definir a ordem de execução.

# A DAG é automaticamente atribuída à variável 'dag' devido ao uso do 'with DAG(...) as dag'
