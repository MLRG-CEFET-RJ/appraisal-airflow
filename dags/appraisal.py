from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


def loadDataset(ti, **kwargs):
    print('Dataset escolhido: %s' % kwargs['dataset'])
    ti.xcom_push(key='dataset', value=kwargs['dataset'])


def inputSimples(ti):
    print('INPUTAÇÃO SIMPLES')
    ti.xcom_push(key='input', value='input')


def agrupamento():
    print('AGRUPAMENTO')


def selecao():
    print('SELEÇÃO')


'''
JSON config

{
  "dataset":"iris",
  "pipeline":"1"
}

'''
with DAG(dag_id="appraisal",
         start_date=datetime(2022, 6, 23),
         schedule_interval="@hourly",
         catchup=False) as dag:

    inputingPlans = ['InputSimples', 'AgrupamentoImputação', 'SeleçãoInputação',
                     'AgrupamentoSeleçãoInputação', "SeleçãoAgrupamentoImputação"]

    readConfigTask = PythonOperator(
        task_id="readConfig",
        python_callable=loadDataset,
        op_kwargs={'dataset': "{{ dag_run.conf['dataset'] }}"}
    )
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: inputingPlans[int("{{ dag_run.conf['pipeline'] }}")],
    )
    joinTask = DummyOperator(
        task_id='end',
        # Executa caso nenhum falhe e ao menos 1 tenha sucesso
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    readConfigTask >> branching

    InputSimplesTask = DummyOperator(
        task_id="InputSimples"
    )
    InputTask = PythonOperator(
        task_id="Input",
        python_callable=inputSimples
    )
    branching >> InputSimplesTask >> InputTask >> joinTask

    agrupamentoImputacaoTask = DummyOperator(
        task_id='AgrupamentoImputação'
    )
    agrupamentoTask = PythonOperator(
        task_id='Agrupamento',
        python_callable=agrupamento
    )
    branching >> agrupamentoImputacaoTask >> agrupamentoTask >> InputTask >> joinTask

    selecaoInputacaoTask = DummyOperator(
        task_id='SeleçãoInputação'
    )
    selecaoTask = PythonOperator(
        task_id='Seleção',
        python_callable=selecao
    )
    branching >> selecaoInputacaoTask >> selecaoTask >> InputTask >> joinTask

    # agrupamentoSelecaoInputacaoTask = DummyOperator(
    #     task_id='AgrupamentoSeleçãoInputação'
    # )
    # branching >> agrupamentoSelecaoInputacaoTask >> agrupamentoTask >> selecaoTask >> InputTask >> joinTask

    selecaoAgrupamentoImputacao = DummyOperator(
        task_id='SeleçãoAgrupamentoImputação'
    )
    branching >> selecaoAgrupamentoImputacao >> selecaoTask >> agrupamentoTask >> InputTask >> joinTask

