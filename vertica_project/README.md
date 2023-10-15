## Diploma project

### Task description:
We need to prepare users transactions activity data for money transfer service analytics.\
Our sources are PostgreSQL tables with data of transactions and exchange rates.\
We need to upload data from PostgreSQL to Vertica then build the data mart of users activity in one defined currency and then make the dashboard.

### Project structure:
* [sql_scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/sql_scripts) - creating table scripts
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/dags) - airflow dags
* [img](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/img) - dashboard screenshots

### What was done:
1. DWH structure was designed. It contains STG and CDM layers.
2. Staging layer was created. Raw data from sources are stored here as is.\
It consists of tables transactions and currencies. The currencies table is sorted by date. The transactions table is segmented by hash-function of date and transaction id, moreover there was created a date-sorted projection of it.\
Tables creation scripts:
"[\sql_scripts\create_tables.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/sql_scripts/create_tables.sql)"
3. CDM layer was created. It consists of global_metrics data mart which is sorted by date.\
Tables creation scripts:
"[\sql_scripts\create_tables.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/sql_scripts/create_tables.sql)"
4. The ETL process of source connection was developed. It was created with Airflow using Python. DAG consists of tasks for current date data uploading from PostgreSQL to STG using PostgresHook. Current date is stored as the airflow variable.\
Used Python libraries list: vertica_python, pandas, logging, datetime, airflow.\
DAG code:
"[\dags\stg_one_day_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/dags/stg_one_day_dag.py)"
5. The data mart updating ETL process was developed. It was created with Airflow using Python. DAG consists of tasks for current date data uploading from STG to CDM. Current date is stored as the airflow variable.\
Used Python libraries list: vertica_python, logging, datetime, airflow.\
DAG code:
"[\dags\stg_to_mart_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/dags/stg_to_mart_dag.py)"\
SQL code is saved in txt file for easier formatting:
"[\dags\sql_mart.txt](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/dags/sql_mart.txt)"
6. The dashboard was created using Metabase. It shows:\
Company's turnover\
![Turnover](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.49.33.png)\
Transactions statistics\
![Transactions](https://github.com/PonomarevVladimir/de-project-final/blob/main/src/img/Screenshot%202023-06-20%20at%2011.49.39.png)\
Statistics of users and tables updates\
![Users](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.49.43.png)\
Filters of dates and currencies\
![Filtered turnover](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.50.30.png)\
![Filtered transactions](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.50.34.png)\
![Filtered users](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.50.38.png)

## Выпускная проектная работа 

### Описание задачи:
Необходимо подготовить данные по транзакционной активности пользователей сервиса валютных переводов для аналитики.\
Источники содержат информацию о транзакциях и курсах валют и расположены в базе PostgreSQL.\
Необходимо загрузить данные из PostgreSQL в Vertica, собрать витрину, показывающую ежедневную активность пользователей в одной валюте, а также построить дашборд. 

### Структура проекта:
* [sql_scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/sql_scripts) - скрипты создания таблиц
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/dags) - даги airflow
* [img](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/img) - скриншоты дашборда

### Что было сделано:
1. Спроектирована структура хранилища DWH. Оно состоит из Staging-слоя и CDM.
2. Реализован staging-слой. В него попадают данные из системы-источника в исходном формате данных.\
Он состоит из таблиц transactions и currencies. Таблица currencies отсортирована по дате. Таблица transactions сегментирована по хэш-функции от даты и идентификатора транзакции, также для неё создана отсортированная по дате проекция.\
Код создания таблиц: "[\sql_scripts\create_tables.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/sql_scripts/create_tables.sql)"
3. Реализован CDM-слой. В нём находится витрина global_metrics, которая также отсортирована по дате.\
Код создания таблиц: "[\sql_scripts\create_tables.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/sql_scripts/create_tables.sql)"
4. Реализован ETL-процесс, который подключается к системе-источнику при помощи PostgresHook. Процесс загрузки данных из источника в staging-слой разрабатываемой системы реализован с помощью DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи на соответствующую загрузку данных из источника за текущую дату, хранящуюся в переменной Airflow.\
Для реализации этого процесса были использованы следующие библиотеки Python: vertica_python, pandas, logging, datetime, airflow.\
Исходный код прилагается: "[\dags\stg_one_day_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/dags/stg_one_day_dag.py)"
5. Реализован ETL-процесс, который наполняет витрину. Процесс загрузки данных реализован с помощью DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи на соответствующую загрузку данных из STG-слоя за текущую дату, хранящуюся в переменной Airflow.\
Для реализации этого процесса были использованы следующие библиотеки Python: vertica_python, logging, datetime, airflow.\
Исходный код прилагается: "[\dags\stg_to_mart_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/dags/stg_to_mart_dag.py)"\
Запрос, производящий агрегацию, вынесен в отдельный файл формата txt для удобства форматирования: "[\dags\sql_mart.txt](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/dags/sql_mart.txt)"
6. Построен дашборд при помощи Metabase, который показывает:\
Общий оборот компании\
![Оборот](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.49.33.png)\
Статистику транзакций\
![Транзакции](https://github.com/PonomarevVladimir/de-project-final/blob/main/src/img/Screenshot%202023-06-20%20at%2011.49.39.png)\
Статистику пользователей и обновления таблиц\
![Пользователи](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.49.43.png)\
Также реализованы фильтры дат и валют:\
![Оборот с фильтрами](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.50.30.png)\
![Транзакции с фильтрами](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.50.34.png)\
![Пользователи с фильтрами](https://github.com/PonomarevVladimir/Portfolio/blob/main/vertica_project/img/Screenshot%202023-06-20%20at%2011.50.38.png)
