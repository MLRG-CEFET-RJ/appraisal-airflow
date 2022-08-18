# Ref: https://benalexkeen.com/k-nearest-neighbours-classification-in-python/

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.preprocessing import MinMaxScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


def loadDataset(ti):
    print('Carregando o iris dataset')
    iris = load_iris()
    data = pd.DataFrame(data=np.c_[iris['data'], iris['target']],
                        columns=iris['feature_names'] + ['target'])
    data.to_csv('data.csv', sep=',')
    ti.xcom_push(key='dataset', value='data.csv')


def minMaxScaler(ti):
    print('Aplicando o MinMaxScaler')
    dataset = ti.xcom_pull(key='dataset', task_ids=['load'])
    data = pd.read_csv(dataset[0])
    X = data.drop(['target'], axis=1)
    X = MinMaxScaler().fit_transform(X).tolist()
    ti.xcom_push(key='X-iris', value=X)


def pca(ti):
    print('Aplicando PCA')
    X = ti.xcom_pull(key='X-iris', task_ids=['minmaxscaler'])[0]

    dataset = ti.xcom_pull(key='dataset', task_ids=['load'])
    y = pd.read_csv(dataset[0])['target']

    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

    pca_model = PCA(n_components=2)
    pca_model.fit(X_train)
    X_train = pca_model.transform(X_train)
    X_test = pca_model.transform(X_test)

    ti.xcom_push(key='X-train-iris', value=X_train.tolist())
    ti.xcom_push(key='y-train-iris', value=y_train.tolist())
    ti.xcom_push(key='X-test-iris', value=X_test.tolist())
    ti.xcom_push(key='y-test-iris', value=y_test.tolist())


def knn(ti):
    X_train = ti.xcom_pull(key='X-train-iris', task_ids=['pca'])[0]
    y_train = ti.xcom_pull(key='y-train-iris', task_ids=['pca'])[0]
    X_test = ti.xcom_pull(key='X-test-iris', task_ids=['pca'])[0]
    y_test = ti.xcom_pull(key='y-test-iris', task_ids=['pca'])[0]

    knn1 = KNeighborsClassifier(5)
    knn1.fit(X_train, y_train)

    y_predicted = knn1.predict(X_test).tolist()
    accuracy = str(accuracy_score(y_test, y_predicted) * 100) + '%'

    ti.xcom_push(key='predicted', value=y_predicted)
    ti.xcom_push(key='accuracy', value=accuracy)


with DAG(dag_id="pipeline_teste",
         start_date=datetime(2022, 6, 23),
         schedule_interval="@hourly",
         catchup=False) as dag:

    loadTask = PythonOperator(
        task_id="load",
        python_callable=loadDataset
    )

    MinMaxTask = PythonOperator(
        task_id="minmaxscaler",
        python_callable=minMaxScaler
    )

    pcaTask = PythonOperator(
        task_id="pca",
        python_callable=pca
    )

    knnTask = PythonOperator(
        task_id="knn",
        python_callable=knn
    )

    endTask = BashOperator(
        task_id='end',
        bash_command="echo TARGET: {{ ti.xcom_pull(key='y-test-iris', task_ids=['pca'])[0] }} && "
        "echo PREDICTED: {{ ti.xcom_pull(key='predicted', task_ids=['knn'])[0] }} && "
        "echo ACCURACY: {{ ti.xcom_pull(key='accuracy', task_ids=['knn'])[0] }}"
    )


loadTask >> MinMaxTask >> pcaTask >> knnTask >> endTask
