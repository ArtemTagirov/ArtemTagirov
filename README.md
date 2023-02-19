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

# ✅ Hive, создание скриптов
__Задача:__
- Создать скрипты для создания таблиц-справочников согласно [описанию](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) формата данных.
- Создать скрипт для создания фактовой таблицы, партиционирование по дню начала поездки.
- Создать скрипт наливки данных в фактовую таблицу
- Создать скрипт для создания представления (view)
- Создать скрипт для создания витрины вида:

Payment type |    Date    | Tips average amount | Passengers total |
:------------|:----------:|:-------------------:|-----------------:|
Cash | 2020-01-01 | 123.45| 67

- Создать скрипт для наливки данных в витрину
- Создать скрипт для запуска всего перечисленного выше в правильном порядке

__Примечание:__ Датасет [NYC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) за 2020 год был 
предварительно скачан в созданный S3 бакет в Object Storage. Задача решалась в облачном кластере yandex.cloud

__Результат:__ На готовые скрипты можно посмотреть в данной [папке](https://github.com/ArtemTagirov/ArtemTagirov/tree/main/Hive)


# ✅ 
## ✅  
### ✅ 