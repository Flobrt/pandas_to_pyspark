from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def ConditionalCalculations_MedicalAge():
    matrix = [
        [1, 34, 'Cardiology'],
        [2, 70, 'Neurology'],
        [3, 50, 'Orthopedics'],
        [4, 20, 'Cardiology'],
        [5, 15, 'Neurology'],
    ]

    columns = ['patient_id', 'age', 'department']

    # Créez une session Spark
    spark = SparkSession.builder \
        .appName("AppName") \
        .getOrCreate()

    spark_df = spark.createDataFrame(matrix, columns)

    result_df = spark_df \
        .withColumn('age_category', F.when(
            F.col('age') > 60, 'senior') \
                .otherwise(
                    F.when(
                        F.col('age') > 18, 'adult') \
                            .otherwise('minor')
                            )
                        )
                    

    return result_df

