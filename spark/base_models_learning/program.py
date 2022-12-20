
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
    StructField("coord.lon", DoubleType()),
    StructField("coord.lat", DoubleType()),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
    StructField("base", StringType()),
    StructField("main.temp", DoubleType()),
    StructField("main.feels_like", DoubleType()),
    StructField("main.temp_min", DoubleType()),
    StructField("main.temp_max", DoubleType()),
    StructField("main.pressure", IntegerType()),
    StructField("main.humidity", IntegerType()),
    StructField("visibility", IntegerType()),
    StructField("wind.speed", DoubleType()),
    StructField("wind.deg", DoubleType()),
    StructField("wind.gust", DoubleType()),
    StructField("clouds.all", IntegerType()),
    StructField("dt.all", IntegerType()),
    StructField("sys.type", IntegerType()),
    StructField("sys.id", IntegerType()),
    StructField("sys.country", StringType()),
    StructField("sys.sunrise", IntegerType()),
    StructField("sys.sunset", IntegerType()),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType()),
])
schema_air = StructType([
    StructField("station.id", StringType()),
    StructField("station.name", StringType()),
    StructField("station.gegrLat", DoubleType()),
    StructField("station.gegrLon", DoubleType()),
    StructField("sensor.id", IntegerType()),
    StructField("sensor.param.id", IntegerType()),
    StructField("sensor.param.code", StringType()),
    StructField("sensor.param.formula", StringType()),
    StructField("sensor.param.name", StringType()),
    StructField("measurement.date", StringType()),
    StructField("measurement.value", DoubleType()),
])


value_df_weather = df_weather.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
json_df_weather = value_df_weather.select(from_json(col("value"),schema_weather).alias("value"), 'timestamp')
exploaded_df_weather = json_df_weather.select("value.*", "timestamp")

exploaded_df_weather = exploaded_df_weather.withColumn("lon", round("`coord.lon`", 4))
exploaded_df_weather = exploaded_df_weather.withColumn("lat", round("`coord.lat`", 4))
watermarked_weather = exploaded_df_weather.withWatermark("timestamp", "1 minute")

value_df_air = df_air.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
json_df_air = value_df_air.select(from_json(col("value"),schema_air).alias("value"), "timestamp")
exploaded_df_air = json_df_air.select("value.*", "timestamp")
exploaded_df_air = exploaded_df_air.withColumn("lon", round("`station.gegrLon`", 4))
exploaded_df_air = exploaded_df_air.withColumn("lat", round("`station.gegrLat`", 4))
watermarked_air = exploaded_df_air.withWatermark("timestamp", "1 minute")

joined_df = watermarked_air.alias("air").join(watermarked_weather.alias("weather"), expr("""
    air.lon = weather.lon AND
    air.lat = weather.lat
"""))

no2_table = joined_df.filter("`sensor.param.code` = 'NO2'").select("*", lit(0).alias("snow_1h"), lit(0).alias("rain_1h"), month("`measurement.date`").alias("month"),hour("`measurement.date`").alias("hour"), dayofweek("`measurement.date`").alias("weekday"))
o3_table = joined_df.filter("`sensor.param.code` = '03'").select("*", lit(0).alias("snow_1h"), lit(0).alias("rain_1h"), month("`measurement.date`").alias("month"),hour("`measurement.date`").alias("hour"), dayofweek("`measurement.date`").alias("weekday"))
pm25_table = joined_df.filter("`sensor.param.code` = 'PM10'").select("*", lit(0).alias("snow_1h"), lit(0).alias("rain_1h"), month("`measurement.date`").alias("month"),hour("`measurement.date`").alias("hour"), dayofweek("`measurement.date`").alias("weekday"))
pm10_table = joined_df.filter("`sensor.param.code` = 'PM25'").select("*", lit(0).alias("snow_1h"), lit(0).alias("rain_1h"), month("`measurement.date`").alias("month"),hour("`measurement.date`").alias("hour"), dayofweek("`measurement.date`").alias("weekday"))
# no2_table_out = no2_table \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
# no2_table_out.awaitTermination()
modeln02 = GBTRegressionModel.load("/home/base_models_learning/2022_12_17_03_58_15_NO2.model")
modelo3 = GBTRegressionModel.load("/home/base_models_learning/2022_12_17_03_58_15_O3.model")
modelpm10 = GBTRegressionModel.load("/home/base_models_learning/2022_12_17_03_58_15_PM10.model")
modelpm25 = GBTRegressionModel.load("/home/base_models_learning/2022_12_17_03_58_15_PM25.model")
print(modeln02.columns)

assembler = VectorAssembler(inputCols=["`measurement.value`", "`main.temp`", "`main.pressure`", "`main.humidity`", "`wind.speed`", "`wind.deg`", "`clouds.all`", "snow_1h", "rain1_h", "month", "hour", "weekday"], outputCol="features")
final_df_n02 = assembler.transform(no2_table)
final_df_o3 = assembler.transform(o3_table)
final_df_pm25 = assembler.transform(pm25_table)
final_df_pm10 = assembler.transform(pm10_table)


print(modeln02.predict(final_df_n02))
print(modelo3.predict(final_df_o3))
print(modelpm25.predict(final_df_pm25))
print(modelpm10.predict(final_df_pm10))
# ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master spark://spark-master:7077 /home/base_models_learning/program.py 10
