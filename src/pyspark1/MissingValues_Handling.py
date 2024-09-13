import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def MissingValues_Handling():
    matrix = [
        [1, 34.0, 'Cardiology'],
        [2, np.nan, 'Neurology'],
        [3, 50.0, 'Orthopedics'],
        [4, np.nan, str(np.nan)],
        [5, 15.0, 'Neurology'],
    ]

    columns = ['patient_id', 'age', 'department']


    # Créez une session Spark
    spark = SparkSession.builder \
        .appName("AppName") \
        .getOrCreate()

    spark_df = spark.createDataFrame(matrix, columns)


    without_nan_df = spark_df \
        .filter(
            F.col('age') != float('NaN')
            ) \
            .agg(
                F.mean('age').alias('mean')
                )

    result_df = spark_df \
        .fillna(
            {'age':without_nan_df.first()[0]}
            ) \
                .withColumn(
                    'department', 
                    F.regexp_replace('department', 'nan', 'Unknown')
                    )

    return result_df