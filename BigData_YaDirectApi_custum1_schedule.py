import requests
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

# Даты для выгрузки поставленной на расписание.
# лаг в 1 день по требованию заказчика
# start_date = end_date - выгружем по одной дате за раз
start_date_v = str(datetime.date(datetime.today()) - timedelta(days=1))
end_date_v = str(datetime.date(datetime.today()) - timedelta(days=1))

# BASH команды которые толкают скрипты на стороне 08 хоста 
bash_clear_tmp = '/usr/bin/python3 /home/bdataadmin/airflowYaDirect/skripts/clear_custum1_txt.py'
bash_clear_hdfs = '/usr/bin/bash /home/bdataadmin/airflowYaDirect/skripts/сheck_hdfs_custom1_txt.sh '

bash_download_custum1_txt = f'/usr/bin/python3 /home/bdataadmin/airflowYaDirect/skripts/getYaDirect_custum1.py {start_date_v}'
bash_preprocessing_txt = '/usr/bin/python3 /home/bdataadmin/airflowYaDirect/skripts/checking_custum1_txt.py'
bash_hdfs_put_bigTxt = '/usr/bin/bash hdfs dfs -put /home/bdataadmin/airflowYaDirect/txt_final/YaDirect_custum1.txt /data/yaDirect/txt_custom_report_1 '

bash_start_pySpark_basic_orc = '/usr/bin/bash /home/bdataadmin/airflowYaDirect/skripts/start_pySpark_YaDirect_custom1.sh '

# DAG упал 🛑
# DAG отработал успешно ✅
def tlgrm(message):
    """
    Функция через телеграмм бот шлет в телеграмм беседу
    отчёты об успешности или не успешности выполнения DAG-а.
    Беседа называется: AFLT_BigDataReports
    """
    token = "BOT_ID:BOT_TOKEN"
    url = "https://api.telegram.org/bot"
    channel_id = "-100PRESS_YOUR_CHANNEL_ID"
    url += token
    method = url + "/sendMessage"

    try:
        r = requests.post(method,
                          data={
                              "chat_id": channel_id,
                              "text": message},
                          #                          proxies=proxy,
                          timeout=30)
        print(r.json())
    except requests.exceptions.ConnectionError:
        print("Искл_1")
        return False


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 25),
    'retries': 0
}


# Если на каком-то узле DAG-а что-то пошло не так - считаем что выгрузка неуспешна
# и для данной даты нужно начинать все сначала. 
# Существуют пары DAG-ов: 1)с выбором даты для выгрузки
# 2) и поставленном на расписание - котррый лучше не трогать.
with DAG(dag_id='BigData_YaDirect_custum1_schedule',
         tags=["ETL на расписании"],
         default_args=default_args,
         schedule_interval='15 21 * * * ',    # каждый день в 21:15 утра, но тк UTC +3 часа, поэтому в 00:15 утра
         start_date=days_ago(1)
         ) as dag:

    task_clear_tmp_1 = SSHOperator(
        task_id='task_clear_tmp_1',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_1 = SSHOperator(
        task_id='task_clear_hdfs_1',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    download_custum1 = SSHOperator(
        task_id='task_download_custum1',
        ssh_conn_id="ssh_08",
        command=bash_download_custum1_txt,
        dag=dag
    )

    task_preprocessing_txt = SSHOperator(
        task_id='task_preprocessing_txt',
        ssh_conn_id="ssh_08",
        command=bash_preprocessing_txt,
        dag=dag
    )

    task_move_bigTxt = SSHOperator(
        task_id='task_move_bigTxt',
        ssh_conn_id="ssh_08",
        command=bash_hdfs_put_bigTxt,
        dag=dag
    )

    task_start_pySpark_Basic_orc = SSHOperator(
        task_id='task_start_pySpark_Basic_orc',
        ssh_conn_id="ssh_08",
        command=bash_start_pySpark_basic_orc,
        dag=dag
    )

    task_clear_tmp_2 = SSHOperator(
        task_id='task_clear_tmp_2',
        ssh_conn_id="ssh_08",
        command=bash_clear_tmp,
        dag=dag
    )

    task_clear_hdfs_2 = SSHOperator(
        task_id='task_clear_hdfs_2',
        ssh_conn_id="ssh_08",
        command=bash_clear_hdfs,
        dag=dag
    )

    tlgrm_allSuccess = PythonOperator(
        task_id='tlgrm_allSuccess',
        python_callable=tlgrm,
        dag=dag,
        trigger_rule='all_success',
        op_kwargs={
            'message': f"Отработал успешно ✅ - DAG YaDirect_custum1_schedule_{start_date_v}_{end_date_v}"}
    )

    tlgrm_oneFailed = PythonOperator(
        task_id='tlgrm_oneFailed',
        python_callable=tlgrm,
        dag=dag,
        trigger_rule='one_failed',
        op_kwargs={
            'message': f"Упал 🛑 - DAG YaDirect_custum1_schedule_{start_date_v}_{end_date_v}"}
    )

(task_clear_tmp_1 >> task_clear_hdfs_1 >> download_custum1 >> task_preprocessing_txt >> task_move_bigTxt >> task_start_pySpark_Basic_orc >> task_clear_tmp_2 >> task_clear_hdfs_2 >> [tlgrm_allSuccess, tlgrm_oneFailed])
