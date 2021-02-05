import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Definindo alguns argumentos básicos
default_args = {
   'owner': 'fiec',
   'start_date': datetime(2020, 2, 1),
   'retries': 0,
   }

# Criação do objeto DAG
with DAG(
   'etl_antaq',
   default_args=default_args
   ) as dag:

# Definições de DAGs

t0 = BashOperator(
    task_id= 'alertas',
    bash_command="./scripts/alertas.sh",
    dag=dag
)

t1 = BashOperator(
    task_id= 'download_dados',
    bash_command="./scripts/download_dados.sh",
    dag=dag
) 

t2 = BashOperator(
    task_id= 'tratamento_dados',
    bash_command="./scripts/tratamento_dados.sh",
    dag=dag
) 

t3 = BashOperator(
    task_id= 'criar_atracacao_fato',
    bash_command="python /scripts/criar_atracacao_fato.py",
    dag=dag
) 

t4 = BashOperator(
    task_id= 'criar_carga_fato',
    bash_command="python /scripts/criar_carga_fato.py",
    dag=dag
) 

# Cada operação informa ao script de alertas se a operação anterior ocorreu bem
# Em caso de erros, o script gera alertas pré definidos
t1 >> t0 >> t2 >> t0 >> t3 >> t0 >> t4