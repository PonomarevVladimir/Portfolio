## Data Lake project

### Task description:
We need to build three data marts in data lake.\
Our sources are data from hdfs and local csv file with cities coordinates.\
List of data marts:
* Users mart - contains data of user home city and travels
* Zones mart - contains users activity statistics grouped by cities
* Friends recomendations mart - contains pairs of users who never chated but live close to each other and have the same interests

### Project structure:
* [scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/spark_project/scripts) - python scripts
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/spark_project/dags) - airflow dags

### What was done:
1. Auxiliary functions for distances between cites calculations using their coordinates and reducing UTC time to local one were developed.
Python code: "[\scripts\project_functions.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/project_functions.py)"
2. ETL process for creating and updating the users data mart was developed.\
It was created with Airflow using Python. DAG consists of tasks wich start spark applications with defined parameters.\
Used Python libraries list: datetime, airflow, pyspark, math, sys.\
DAG code: "[\dags\ul_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/ul_dag.py)"\
PySpark code: "[\scripts\ul.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/ul.py)"
3. ETL process for creating and updating the zones data mart was developed.\
It was created with Airflow using Python. DAG consists of tasks wich start spark applications with defined parameters.\
Used Python libraries list: datetime, airflow, pyspark, math, sys.\
DAG code: "[\dags\zs_dag_init.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/zs_dag_init.py "Initial")", "[\dags\zs_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/zs_dag.py "Increment")"\
PySpark code: "[\scripts\ul.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/zs.py)"
4. ETL process for creating and updating the friends recomendations data mart was developed.\
It was created with Airflow using Python. DAG consists of tasks wich start spark applications with defined parameters.\
Used Python libraries list: datetime, airflow, pyspark, math, sys.\
DAG code: "[\dags\rec_dag_init.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/rec_dag_init.py "Initial")", "[\dags\rec_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/rec_dag.py "Increment")"\
PySpark code: "[\scripts\rec.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/rec.py)"

## Проектная работа по организации Data Lake

### Описание задачи:
Нужно реализовать рассчёт трёх витрин на данных, хранящихся в data lake.\
Источниками являются данные из hdfs, а также csv-таблица с координатами городов, хранящаяся локально.\
Необходиные витрины:
* Витрина пользователей - содержит информацию о родном городе и поездках пользователя
* Витрина зон - содержит статистику об активности пользователей по городам
* Витрина рекомендации друзей - содерждит пары пользователей, которые ранее не общались, живут рядом и имеют общие интересы

### Структура проекта:
* [scripts](https://github.com/PonomarevVladimir/Portfolio/tree/main/spark_project/scripts) - скрипты наполнения витрин
* [dags](https://github.com/PonomarevVladimir/Portfolio/tree/main/spark_project/dags) - даги airflow

### Что было сделано:
1. Реализованы вспомогательные функции для определения расстояния по координатам и приведения времени к местному.
Исходный код прилагается: "[\scripts\project_functions.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/project_functions.py)"
2. Реализован ETL-процесс, который создаёт и наполняет витрину пользователей.\
Процесс первоначального расчёта и инкрементального наполнения реализован при помощи DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи запуск spark-приложения с нужными параметрами.\
Для реализации этого процесса были использованы следующие библиотеки Python: datetime, airflow, pyspark, math, sys.\
Исходный код прилагается: "[\dags\ul_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/ul_dag.py)"\
Код spark-приложения: "[\scripts\ul.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/ul.py)"
3. Реализован ETL-процесс, который создаёт и наполняет витрину зон.\
Процесс первоначального расчёта и инкрементального наполнения реализован при помощи DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи запуск spark-приложения с нужными параметрами.\
Для реализации этого процесса были использованы следующие библиотеки Python: datetime, airflow, pyspark, math, sys.\
Исходный код прилагается: "[\dags\zs_dag_init.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/zs_dag_init.py "Первоначальный расчёт")", "[\dags\zs_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/zs_dag.py "Инкрементальное обновление")"\
Код spark-приложения: "[\scripts\ul.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/zs.py)"
4. Реализован ETL-процесс, который создаёт и наполняет витрину рекомендации друзей.\
Процесс первоначального расчёта и инкрементального наполнения реализован при помощи DAG-оркестратора Airflow на языке программирования Python. DAG содержит задачи запуск spark-приложения с нужными параметрами.\
Для реализации этого процесса были использованы следующие библиотеки Python: datetime, airflow, pyspark, math, sys.\
Исходный код прилагается: "[\dags\rec_dag_init.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/rec_dag_init.py "Первоначальный расчёт")", "[\dags\rec_dag.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/dags/rec_dag.py "Инкрементальное обновление")"\
Код spark-приложения: "[\scripts\rec.py](https://github.com/PonomarevVladimir/Portfolio/blob/main/spark_project/scripts/rec.py)"
