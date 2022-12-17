import pyspark.sql.functions as sf
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Window
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

import os
import datetime
particles = ('NO2', 'O3', 'PM25', 'PM10')
lower_threshold = 25
upper_threshold = 50

def union_all(dfs):
    df = dfs[0]
    for df_ in dfs[1:]:
        df = df.union(df_)
    return df


def train_models(df, particles=particles, models_dir='base_models_learning/models',
                 summary_dir='base_models_learning/summary', types=('RF', 'GB')):
    models = dict()
    summaries = []
    general_summaries = []
    for type_ in types:
        print(type_)
        for particle in particles:
            print(particle)
            summary, general_summary, model = train_model(
                df, particle, particles, type_)
            models[f'{type}_{particle}'] = model
            summaries.append(summary)
            general_summaries.append(general_summary)

    general_summary = union_all(general_summaries)

    w = Window.partitionBy(['particle'])
    general_summary = general_summary\
        .withColumn('min_MAE', sf.min('MAE').over(w))\
        .filter(sf.col('MAE') == sf.col('min_MAE'))\
        .drop('min_MAE')
    now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    save_summary(general_summary, os.path.join(
        summary_dir,  f'{now}_general_summary.csv'))
    general_summary_pd = general_summary.toPandas()
    best_models = dict(
        zip(general_summary_pd['particle'], general_summary_pd['model_type']))

    for particle, type_ in best_models.items():
        save_model(models[f'{type}_{particle}'],
                   os.path.join(models_dir, f'{now}_{particle}.model'))

    summary = union_all(summaries)
    save_summary(summary, os.path.join(summary_dir, f'{now}_summary.csv'))


def save_model(model, filename):
    if not os.path.exists(os.path.dirname(filename)):
        os.mkdir(os.path.dirname(filename))
    model.save(filename)


def save_summary(summary, filename):
    if not os.path.exists(os.path.dirname(filename)):
        os.mkdir(os.path.dirname(filename))
    summary.toPandas().to_csv(filename, index=False)


def train_model(df, particle, all_particles, type_):
    particles_to_drop = [
        particle_ for particle_ in all_particles if particle_ != particle]
    cols_to_drop = [f'{particle}_present' for particle in particles_to_drop] + \
        [f'{particle}_feature' for particle in particles_to_drop]
    df = df.drop(*cols_to_drop)
    df = df.dropna()

    assembler = VectorAssembler(inputCols=[col for col in df.columns if col != f'{particle}_future'],
                                outputCol="features")
    df = assembler.transform(df)

    train, test = df.randomSplit([0.7, 0.3])

    model_args = {'featuresCol': 'features', 'labelCol': f'{particle}_future'}
    model = GBTRegressor(
        **model_args) if type_ == 'GB' else RandomForestRegressor(**model_args)
    model = model.fit(train)
    preds = model.transform(test)
    summary = preds.select(
        sf.col(f'{particle}_future'),
        sf.col('prediction'),
        sf.abs((sf.col('prediction') -
                sf.col(f'{particle}_future'))/sf.col(f'{particle}_future')).alias('MAE'),
        sf.when(preds.prediction < lower_threshold, 0).when(
            upper_threshold <= preds.prediction, 2)
        .otherwise(1).alias('interval'))

    general_summary = summary\
        .dropna()\
        .select(
            sf.mean('MAE').alias('MAE')
        ).withColumn('model_type', sf.lit(type_))\
        .withColumn('particle', sf.lit(particle))

    summary = summary.dropna()\
        .groupBy('interval')\
        .agg(sf.mean('MAE').alias('MAE'))

    mean_mae = summary.select(sf.mean('MAE')).collect()[0][0]
    intervals = spark.range(3).withColumnRenamed('id', 'interval')

    summary = summary.join(intervals, summary.interval == intervals.interval, 'right')\
        .drop(summary.interval)\
        .fillna(mean_mae)\
        .withColumn('model_type', sf.lit(type_))\
        .withColumn('particle', sf.lit(particle))
    return summary, general_summary, model
