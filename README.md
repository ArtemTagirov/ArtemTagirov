# My DE portfolio

Здесь я собрал свои работы в виде портфолио.

-----

# ✅ Airflow, создание DAG и Operator
__Задача:__
- С помощью API (https://rickandmortyapi.com/documentation/#location) создать Operator для нахождения трех локации сериала
"Рик и Морти" с наибольшим количеством резидентов. 
- Создать DAG, который создает в  GreenPlum'е таблицу с названием "<название>" с полями id, name, type, dimension, resident_cnt и записывает значения соответствующих полей этих трёх локаций в таблицу.

__Результат:__ [Operator](https://github.com/ArtemTagirov/ArtemTagirov/blob/main/Airflow/plugins/atg_ram_top_n_locations_operator.py), [DAG](https://github.com/ArtemTagirov/ArtemTagirov/blob/main/Airflow/dags/atg_ram_top_locations.py)

---

# ✅ Создание витрины (HADOOP. YARN, MAPREDUCE)
__Задача:__ Написать map-reduce приложение, использующее скопированные на Object storage данные и вычисляющее отчет на каждый месяц года вида:

Payment type |  Month | Tips average amount
:------------|:------:|-------------------:
Cash | 2020-01 | 123.45

__Примечание:__ Датасет [NYC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) за 2020 год был 
предварительно скачан в созданный S3 бакет в Object Storage. Задача решалась в облачном кластере yandex.cloud

__Результат:__ [MapReduce приложение](https://github.com/ArtemTagirov/ArtemTagirov/tree/main/MapReduce)

# ✅ 
## ✅  
### ✅ 