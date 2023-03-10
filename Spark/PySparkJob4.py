"""
Для дашборда с отображением выполненных рейсов требуется собрать таблицу вида:
Колонка -------------------- Описание
AIRLINE_NAME --------------- Полное название авиакомпании
TAIL_NUMBER ---------------- Номер рейса
ORIGIN_COUNTRY ------------- Страна отправления
ORIGIN_AIRPORT_NAME -------- Полное название аэропорта отправления
ORIGIN_LATITUDE ------------ Широта аэропорта отправления
ORIGIN_LONGITUDE ----------- Долгота аэропорта отправления
DESTINATION_COUNTRY -------- Страна прибытия
DESTINATION_AIRPORT_NAME --- Полное название аэропорта прибытия
DESTINATION_LATITUDE ------- Широта аэропорта прибытия
DESTINATION_LONGITUDE ------ Долгота аэропорта прибытия
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """

    flight_df = spark.read.parquet(flights_path)
    airlines_df = spark.read.parquet(airlines_path)
    airports_df = spark.read.parquet(airports_path)

    airlines_df = airlines_df \
        .withColumnRenamed('IATA_CODE', 'IATA_CODE_A') \
        .withColumnRenamed('AIRLINE', 'AIRLINE_A')

    airports_df_destination = airports_df \
        .withColumnRenamed('IATA_CODE', 'IATA_CODE_D') \
        .withColumnRenamed('AIRPORT', 'DESTINATION_AIRPORT_NAME') \
        .withColumnRenamed('CITY', 'CITY_D') \
        .withColumnRenamed('STATE', 'STATE_D') \
        .withColumnRenamed('COUNTRY', 'DESTINATION_COUNTRY') \
        .withColumnRenamed('LATITUDE', 'DESTINATION_LATITUDE') \
        .withColumnRenamed('LONGITUDE', 'DESTINATION_LONGITUDE')

    result_df = flight_df \
        .join(other=airlines_df, on=airlines_df['IATA_CODE_A'] == F.col('AIRLINE'), how='inner') \
        .join(other=airports_df, on=airports_df['IATA_CODE'] == F.col('ORIGIN_AIRPORT'), how='inner') \
        .join(other=airports_df_destination, on=airports_df_destination['IATA_CODE_D'] == F.col('DESTINATION_AIRPORT'), how='inner') \
        .select(airlines_df['AIRLINE_A'].alias('AIRLINE_NAME'),
                F.col('TAIL_NUMBER'),
                airports_df['COUNTRY'].alias('ORIGIN_COUNTRY'),
                airports_df['AIRPORT'].alias('ORIGIN_AIRPORT_NAME'),
                airports_df['LATITUDE'].alias('ORIGIN_LATITUDE'),
                airports_df['LONGITUDE'].alias('ORIGIN_LONGITUDE'),
                airports_df_destination['DESTINATION_COUNTRY'],
                airports_df_destination['DESTINATION_AIRPORT_NAME'],
                airports_df_destination['DESTINATION_LATITUDE'],
                airports_df_destination['DESTINATION_LONGITUDE'])

    #result_df.write.mode('overwrite').parquet(result_path)
    result_df.show()

def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
