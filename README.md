##  Airflow

O Apache Airflow é uma plataforma projetada para criar, agendar e monitorar fluxos de trabalho de forma programática.

Quando os fluxos de trabalho são definidos como código, eles se tornam mais fáceis de manter, versionar, testar e colaborar.

Utilize o Airflow para compor fluxos de trabalho como grafos acíclicos dirigidos (DAGs) de tarefas. O agendador do Airflow executa suas tarefas em uma série de workers respeitando as dependências definidas. Ferramentas de linha de comando abrangentes facilitam a realização de operações complexas nos DAGs. A interface de usuário intuitiva permite visualizar facilmente os pipelines em execução, monitorar o progresso e resolver problemas quando necessário.

O Airflow é especialmente útil em contextos de engenharia de dados e ciência de dados, pois permite a automação e a orquestração de processos complexos de tratamento de dados, treinamento de modelos de machine learning, execução de ETLs e muito mais. Tudo isso contribui para uma gestão mais eficiente do ciclo de vida dos dados e dos modelos preditivos.

## User Interface

- **DAGs**: Visão deral do seu ambiente.

  ![DAGs](https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/img/dags.png)

- **Grid**: Visão de todas as execuções de um DAG.

  ![Grid](https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/img/grid.png)

- **Graph**: Visão de todas as tarefas de um DAG e suas dependências.

  ![Graph](https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/img/graph.png)

- **Task Duration**: Tempo de execução de cada tarefa de um DAG.

  ![Task Duration](https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/img/duration.png)

- **Gantt**: Duração de cada execução de um DAG.

  ![Gantt](https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/img/gantt.png)

- **Code**: Código de cada DAG.

  ![Code](https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/img/code.png)

### Instalação do Airflow em uma máquina EC2 AWS sem Docker

Este README fornece instruções passo a passo para instalar o Apache Airflow em uma instância EC2 da Amazon usando Ubuntu como sistema operacional. Certifique-se de ter as permissões adequadas e o acesso SSH à sua instância EC2 antes de começar.

Caso tenha dúvidas, visitar a [documentação](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

### Criar conta na AWS

1. Criar uma [conta na AWS](https://www.googleadservices.com/pagead/aclk?sa=L&ai=DChcSEwjAsan6qK2CAxWznVoFHQsWDoQYABAAGgJ2dQ&ase=2&gclid=EAIaIQobChMIwLGp-qitggMVs51aBR0LFg6EEAAYASAAEgKmIvD_BwE&ohost=www.google.com&cid=CAASJORoDeOFQkYZaTPYRdzliq9Zx_XBUo7PjMlTDdyGYQmvH8K9Gg&sig=AOD64_3UuKgxK2EF8RXgITpJ7PsabxFV0A&q&nis=4&adurl&ved=2ahUKEwjhmp_6qK2CAxU6rpUCHSP5D5QQ0Qx6BAgGEAE) e fazer o login.

- **Página inicial**: Página home sem login.

![Página inicial](assets/aws-registrar-conta.png)

- **Página inicial**: Página home após cadastro e logi

![Página inicial após cadastro e login](assets/painel-principal.png)

2. Criar uma instancia EC2.

A máquina EC2 é um servidor virtual na nuvem da Amazon Web Services (AWS) que fornece capacidade computacional escalável. Utilizamos no Airflow para hospedar e executar fluxos de trabalho de forma flexível e confiável, aproveitando a infraestrutura elástica da AWS.

3. Instalando o Airflow na nossa máquina

Fazer update, instalar sqlite e python na máquina virtual

```bash
sudo apt update

sudo apt install python3-pip

sudo apt install sqlite3

sudo apt install python3.10-venv

sudo apt-get install libpq-dev
```

Criar um ambiente virtual de Python

```bash
pip install apache-airflow[postgres]
pip install sqlalchemy
pip install psycopg2-binary
pip install apache-airflow-providers-postgres
```

Configurar o Home do Airflow

```bash
export AIRFLOW_HOME=~/airflow
```

Instalar o Airflow fazendo bruxaria

```bash
AIRFLOW_VERSION=2.7.2

PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Rodar o Airflow

```bash
airflow db migrate

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

airflow webserver &

airflow scheduler
```

4. Acessar o Airflow no seu IP na porta 8080

5. Acessando via SSH

No terminal digitar

```bash
ssh ubuntu@<Public IPv4 address>
```

Vai dar acesso negado

``` 
Permission denied (publickey).
```

Previsamos de uma chave para acessar a máquina EC2. Para isso, vamos conectar com a chave que criamnos anteriormente.

```bash
ssh -i <path-to-key> ubuntu@<Public IPv4 address>
```

Vai dar erro de permissão

```bash
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@         WARNING: UNPROTECTED PRIVATE KEY FILE!          @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Permissions 0644 for 'airflow-workshop.pem' are too open.
It is required that your private key files are NOT accessible by others.
This private key will be ignored.
```

```bash
chmod 0400 <path-to-key>
```

Tentar novamente e

```
ssh -i <path-to-key> ubuntu@<Public IPv4 address>
```

Tudo certo!

Obs: Caso use [Windows verificar esse tutorial aqui](https://www.youtube.com/watch?v=kzLRxVgos2M)


6) Acessar pelo VScode

## Acesso ao Airflow

Após iniciar o servidor web e o agendador, você pode acessar a interface do Airflow em seu navegador, usando o endereço IP da sua instância EC2 seguido pela porta `8080`. Por exemplo: `http://<EC2-IP-ADDRESS>:8080`.

Acessar pelo VScode

Certifique-se de que a porta `8080` está aberta no grupo de segurança associado à sua instância EC2.


## Instalando o Airflow em uma máquina EC2 AWS com Docker

### Criar um requiriments.txt

```bash
apache-airflow[postgres]
sqlalchemy
psycopg2-binary
``

Baixar o Docker-compose

https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

```bash
touch docker-compose.yaml
```

```bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

```bash
mkdig logs dags plugins
```

```
docker-compose up airflow-init
```

```bash
docker-compose up
```

```bash
``

### Criando nossa primeira DAG

```python
# Importação de módulos do Airflow e de outras bibliotecas padrão Python
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Configuração dos argumentos padrão que serão aplicados a todas as tarefas da DAG
default_args = {
    "owner": "luciano",  # Define o proprietário da DAG, útil para fins de rastreamento e permissão.
    "start_date": "2023-11-02",  # Data de início da execução da DAG. As execuções serão agendadas a partir desta data.
    "retries": 1,  # Número de tentativas de reexecução de uma tarefa em caso de falha.
    "retry_delay": timedelta(minutes=1),  # Intervalo de tempo entre as tentativas de reexecução.
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
        bash_command='echo "Hello World!"'  # Comando Bash que a tarefa vai executar.
    )

    # Neste ponto, você pode definir mais tarefas e suas dependências.
    # Por exemplo: task2 = BashOperator(...), seguido de task1 >> task2 para definir a ordem de execução.

# A DAG é automaticamente atribuída à variável 'dag' devido ao uso do 'with DAG(...) as dag'

```

### Como remover as DAGs originais

```yml

AIRFLOW__CORE__LOAD_EXAMPLES: 'false'

### Adicionar novos Itens após o deploy

INSERT INTO bronze_produtos (titulo, descricao, preco) VALUES 
('Carrinho de Controle Remoto', 'Azul', 300.00),
('Workshop', 'Workshop de deploy', -100.00);