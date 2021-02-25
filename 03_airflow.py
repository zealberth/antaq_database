import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Lib com o conjunto de scripts
from meus_scripts import download_bases, unzip_bases, criar_atracacao_fato, criar_carga_fato

# Definindo alguns argumentos básicos
default_args = {
   'owner': 'fiec',
   'start_date': datetime(2020, 2, 1),
   'retries': 0,
   'depends_on_past': True,
   }

# Criação do objeto DAG
with DAG(
   'etl_antaq',
   default_args=default_args
   ) as dag:

# Definições de DAGs

t01 = BashOperator(
    task_id= 'alertas',
    bash_command="./scripts/alerta_download.sh"
)

t02 = BashOperator(
    task_id= 'alertas',
    bash_command="./scripts/alerta_unzip.sh"
)

t03 = BashOperator(
    task_id= 'alertas',
    bash_command="./scripts/alerta_atracacao_fato.sh"
)

t04 = BashOperator(
    task_id= 'alertas',
    bash_command="./scripts/alerta_carga_fato.sh"
)

t1 = PythonOperator(task_id='download_dados',
                    python_callable=download_bases)

t2 = PythonOperator(task_id='tratamento_dados',
                    python_callable=unzip_bases)

t3 = PythonOperator(task_id='criar_atracacao_fato',
                    python_callable=criar_atracacao_fato)

t4 = PythonOperator(task_id='criar_carga_fato',
                    python_callable=criar_carga_fato)

# Cada operação informa ao script de alertas se a operação anterior ocorreu bem
# Em caso de erros, o script gera alertas pré definidos
t1 >> t01 >> t2 >> t02 >> t3 >> t03 >> t4 >> t04