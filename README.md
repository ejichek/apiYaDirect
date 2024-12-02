# apiYaDirect

ETL процесс на базе AirFlow, Python и PySpark по выгрузке таблицы, где "ReportType": "CUSTOM_REPORT". 
Источник данных - API Яндекс.Директ, получатель - HDFS, файлы хранятся в orc и партиционируются по полю date с "шагом" в 1 день. 
