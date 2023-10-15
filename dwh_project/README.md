## Multiple data sources DWH project

### Task description:
We need to build dwh for delivery service. It would contain staging layer (raw data downloaded from api), detail data store and common data marts layer.\
From data sources we get couriers and deliveries data, moreover there was developed dwh for restaurants data.\
Final result would be the couriers payments data mart with automatic data update.

### Project structure:
* [sql_scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/dwh_project/sql_scripts) - creating tables, uploading data and testing scripts
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/dwh_project/dags) - airflow dags

### What was done:
1. DWH structure was designed. It contains STG, DDS and CDM layers.
2. Staging layer was created. Raw data from sources are stored here as is.\
It consists of tables apisystem_couriers, apisystem_deliveries and auxiliary table loads_check containing results of soures' data uploads.\
Tables creation scripts:
"[\sql_scripts\create_stg.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_stg.sql)", "[\sql_scripts\create_stg_loads_check.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_stg_loads_check.sql)"
3. DDS layer was created. It contains data from the staging layer reduced to the data types required for the mart.\
It consists of tables dm_couriers, dm_timestamps и dm_deliveries. It's data schema is the star with dm_deliveries as the fact table.\
Tables creation scripts:
"[\sql_scripts\create_dds.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_dds.sql)"
4. CDM layer was created. It contains aggregated data from the DDS layer.\
It consists of monthly couriers payments mart dm_courier_ledger.\
Tables creation scripts:
 "[\sql_scripts\create_cdm.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_cdm.sql)"
5. The ETL process for sources api connection was developed. It was created with Airflow using Python. DAG consists of tasks for data uploading from sources to STG.\
Used Python libraries list: requests, pandas, json, logging, datetime, airflow.\
ETL python scripts:
"[\dags\couriers_get_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/couriers_get_dag.py)", "[\dags\deliveries_get_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/deliveries_get_dag.py)"
6. The ETL process for uploading data with reduced types from STG to DDS was developed. It was created with Airflow using Python. DAG consists of tasks for data uploading from STG to DDS.\
Used Python libraries list: logging, datetime, airflow.\
ETL python scripts:
"[\dags\stg_to_dds_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/stg_to_dds_dag.py)"\
SQL scripts:
"[\sql_scripts\insert_couriers.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_couriers.sql)", "[\sql_scripts\insert_timestamps.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_timestamps.sql)", "[\sql_scripts\insert_deliveries.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_deliveries.sql)"
7. The ETL for uploading aggregated data from DDS to CDM process was developed. It was created with Airflow using Python. DAG consists of tasks for data uploading from DDS to CDM.\
Used Python libraries list: logging, datetime, airflow.\
ETL python scripts:
"[\dags\dds_to_cdm_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/dds_to_cdm_dag.py)"\
SQL scripts:
"[\sql_scripts\insert_ledger.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_ledger.sql)"

#### Compliance with the requirements is confirmed by tests results: #
1. All positive tests for data quality of DDS and CDM layers were passed successfully.\
Tests scripts:
"[\sql_scripts\positive_tests.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/positive_tests.sql)"
2. All negative tests for lack of data from sources were passed successfully.\
Tests scripts:
"[\sql_scripts\negative_couriers_test.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/negative_couriers_test.sql)", "[\sql_scripts\negative_timestamps_test.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/negative_timestamps_test.sql)", "[\sql_scripts\negative_deliveries_test.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/negative_deliveries_test.sql)"

## Проектная работа по DWH для нескольких источников

### Описание задачи:
Нужно построить хранилище данных службы доставки, которое будет включать в себя слой сырых данных, получаемых по api, слой детальных исторических данных и слой витрин.\
Из источников мы получаем информацию о курьерах и доставках, также ранее было реализовано хранение данных о ресторанах.\
Конечная задача — реализовать автоматически обновляемую витрину с рассчётом месячных выплат курьерам.

### Структура проекта:
* [sql_scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/dwh_project/sql_scripts) - скрипты создания и наполнения таблиц, а также тесты
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/dwh_project/dags) - даги airflow

### Что было сделано:
1. Спроектирована структура хранилища DWH. Оно состоит из Staging-слоя, DDS и CDM.
2. Реализован staging-слой. В него попадают данные из системы-источника в исходном формате данных.\
Он состоит из таблиц apisystem_couriers, apisystem_deliveries, а также вспомогательной таблицы loads_check, хранящей результаты работы загрузок из источника.\
Код создания таблиц: "[\sql_scripts\create_stg.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_stg.sql)", "[\sql_scripts\create_stg_loads_check.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_stg_loads_check.sql)"
3. Реализован DDS-слой. В него попадают данные из staging-слоя, приведённые к необходимым для витрины типам данных.\
Он состоит из таблиц dm_couriers, dm_timestamps и dm_deliveries. Эти таблицы образуют модель звезда с таблицей фактов dm_deliveries.\
Код создания таблиц: "[\sql_scripts\create_dds.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_dds.sql)"
4. Реализован CDM-слой. В него попадают агрегированные данные из DDS-слоя.\
Он состоит из витрины dm_courier_ledger, содержащей данные по выплатам курьерам, разбитым по месяцам.\
Код создания таблиц: "[\sql_scripts\create_cdm.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/create_cdm.sql)"
5. Реализован ETL-процесс, который подключается к системе-источнику по api. Процесс загрузки данных из источника в staging-слой разрабатываемой системы реализован с помощью DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи на соответствующую загрузку данных из источника.\
Для реализации этого процесса были использованы следующие библиотеки Python: requests, pandas, json, logging, datetime, airflow.\
Исходный код прилагается: "[\dags\couriers_get_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/couriers_get_dag.py)", "[\dags\deliveries_get_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/deliveries_get_dag.py)"
6. Реализован ETL-процесс, который переносит данные из stging-слоя в dds-слой и приводит их к необходимым типам данных.\
Процесс загрузки данных из staging-слоя в dds-слой разрабатываемой системы реализован с помощью DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи на соответствующую загрузку данных.\
Для реализации этого процесса были использованы следующие библиотеки Python: logging, datetime, airflow.\
Исходный код прилагается: "[\dags\stg_to_dds_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/stg_to_dds_dag.py)"\
Код sql запросов хранится в файлах: "[\sql_scripts\insert_couriers.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_couriers.sql)", "[\sql_scripts\insert_timestamps.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_timestamps.sql)", "[\sql_scripts\insert_deliveries.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_deliveries.sql)"
7. Реализован ETL-процесс, который переносит данные из dds-слоя в cdm-слой и аггрегирует их.\
Процесс загрузки данных из dds-слоя в cdm-слой разрабатываемой системы реализован с помощью DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи на соответствующую загрузку данных.\
Для реализации этого процесса были использованы следующие библиотеки Python:  logging, datetime, airflow.\
Исходный код прилагается: "[\dags\dds_to_cdm_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/dags/dds_to_cdm_dag.py)"\
Код sql запросов хранится в файле: "[\sql_scripts\insert_ledger.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/insert_ledger.sql)"

#### Выполнение требований подтверждается результатами тестирования: #
1. Проведены позитивные тесты таблиц dds и cdm слоёв на отсутсвие дублей и пустых значений. Все тесты пройдены успешно.\
Код тестов хранится в файле: "[\sql_scripts\positive_tests.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/positive_tests.sql)"
2. Проведены негативные тесты на реагирование системы на отсутствие в данных системы источника как обязательных, так и необязательных полей. Все тесты пройдены успешно.\
Коды тестов хранится в файлах: "[\sql_scripts\negative_couriers_test.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/negative_couriers_test.sql)", "[\sql_scripts\negative_timestamps_test.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/negative_timestamps_test.sql)", "[\sql_scripts\negative_deliveries_test.sql](https://github.com/PonomarevVladimir/Portfolio/blob/main/dwh_project/sql_scripts/negative_deliveries_test.sql)"
