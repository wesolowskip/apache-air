
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import expr, from_json, col, concat, round, lit, year, month, dayofweek, hour
from pyspark.streaming import StreamingContext
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler

import os
print(os.getcwd(), 'PAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAtth')

spark = SparkSession \
    .builder \
    .appName("Spark Kafka Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
df_air = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "air-data") \
    .load()


df_weather = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-data") \
    .load()

schema_weather = StructType([
    StructField("coord_lon", DoubleType()),
    StructField("coord_lat", DoubleType()),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
    StructField("base", StringType()),
    StructField("main_temp", DoubleType()),
    StructField("main_feels_like", DoubleType()),
    StructField("main_temp_min", DoubleType()),
    StructField("main_temp_max", DoubleType()),
    StructField("main_pressure", IntegerType()),
    StructField("main_humidity", IntegerType()),
    StructField("visibility", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_deg", DoubleType()),
    StructField("wind_gust", DoubleType()),
    StructField("clouds_all", IntegerType()),
    StructField("dt_all", IntegerType()),
    StructField("sys_type", IntegerType()),
    StructField("sys_id", IntegerType()),
    StructField("sys_country", StringType()),
    StructField("sys_sunrise", IntegerType()),
    StructField("sys_sunset", IntegerType()),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType()),
    StructField("snow_1h", DoubleType()),
    StructField("rain_1h", DoubleType()),
])
schema_air = StructType([
    StructField("station_id", StringType()),
    StructField("station_name", StringType()),
    StructField("station_gegrLat", DoubleType()),
    StructField("station_gegrLon", DoubleType()),
    StructField("sensor_id", IntegerType()),
    StructField("sensor_param_id", IntegerType()),
    StructField("sensor_param_code", StringType()),
    StructField("sensor_param_formula", StringType()),
    StructField("sensor_param_name", StringType()),
    StructField("measurement_date", StringType()),
    StructField("measurement_value", DoubleType()),
])


value_df_weather = df_weather.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
json_df_weather = value_df_weather.select(from_json(col("value"),schema_weather).alias("value"), 'timestamp')
exploaded_df_weather = json_df_weather.select("value.*", "timestamp")

exploaded_df_weather = exploaded_df_weather.withColumn("lon", round("`coord_lon`", 4))
exploaded_df_weather = exploaded_df_weather.withColumn("lat", round("`coord_lat`", 4))
watermarked_weather = exploaded_df_weather.withWatermark("timestamp", "1 minute")

value_df_air = df_air.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
json_df_air = value_df_air.select(from_json(col("value"),schema_air).alias("value"), "timestamp")
exploaded_df_air = json_df_air.select("value.*", "timestamp")
exploaded_df_air = exploaded_df_air.withColumn("lon", round("`station_gegrLon`", 4))
exploaded_df_air = exploaded_df_air.withColumn("lat", round("`station_gegrLat`", 4))
watermarked_air = exploaded_df_air.withWatermark("timestamp", "1 minute")

joined_df = watermarked_air.alias("air").join(watermarked_weather.alias("weather"), expr("""
    air.lon = weather.lon AND
    air.lat = weather.lat
"""))

no2_table = joined_df.filter("`sensor_param_code` = 'NO2'").select("*", month("`measurement_date`").alias("month"),hour("`measurement_date`").alias("hour"), dayofweek("`measurement_date`").alias("weekday"))
o3_table = joined_df.filter("`sensor_param_code` = '03'").select("*", month("`measurement_date`").alias("month"),hour("`measurement_date`").alias("hour"), dayofweek("`measurement_date`").alias("weekday"))
pm25_table = joined_df.filter("`sensor_param_code` = 'PM10'").select("*", month("`measurement_date`").alias("month"),hour("`measurement_date`").alias("hour"), dayofweek("`measurement_date`").alias("weekday"))
pm10_table = joined_df.filter("`sensor_param_code` = 'PM25'").select("*", month("`measurement_date`").alias("month"),hour("`measurement_date`").alias("hour"), dayofweek("`measurement_date`").alias("weekday"))
# no2_table_out = no2_table \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
# no2_table_out.awaitTermination()
modeln02 = GBTRegressionModel.load("/home/base_models_learning/2022_12_19_15_14_46_NO2.model")
modelo3 = GBTRegressionModel.load("/home/base_models_learning/2022_12_19_15_14_46_O3.model")
modelpm10 = GBTRegressionModel.load("/home/base_models_learning/2022_12_19_15_14_46_PM10.model")
modelpm25 = GBTRegressionModel.load("/home/base_models_learning/2022_12_19_15_14_46_PM25.model")
print(modeln02.columns)

assembler = VectorAssembler(inputCols=["`measurement_value`", "`main_temp`", "`main_pressure`", "`main_humidity`", "`wind_speed`", "`wind_deg`", "`clouds_all`", "snow_1h", "rain1_h", "month", "hour", "weekday"], outputCol="features")
assembled_df_n02 = assembler.transform(no2_table)
assembled_df_o3 = assembler.transform(o3_table)
assembled_df_pm25 = assembler.transform(pm25_table)
assembled_df_pm10 = assembler.transform(pm10_table)


print(modeln02.predict(assembled_df_n02))
print(modelo3.predict(assembled_df_o3))
print(modelpm25.predict(assembled_df_pm25))
print(modelpm10.predict(assembled_df_pm10))
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master spark://spark-master:7077 /home/base_models_learning/program.py
