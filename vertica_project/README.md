## Выпускная проектная работа 

### Описание задачи:
Необходимо подготовить данные по транзакционной активности пользователей сервиса валютных переводов для аналитики.\
Источники содержат информацию о транзакциях и курсах валют и расположены в базе PostgreSQL.\
Необходимо загрузить данные из PostgreSQL в Vertica, собрать витрину, показывающую ежедневную активность пользователей в одной валюте, а также построить дашборд. 

### Структура проекта:
* [sql_scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/sql_scripts) - скрипты создания и наполнения таблиц, а также тесты
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/vertica_project/dags) - даги airflow

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
