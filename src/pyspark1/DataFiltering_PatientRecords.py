from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def DataFiltering_PatientRecords():
    matrix = [
        [1, 34, 'Cardiology'],
        [2, 45, 'Neurology'],
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
    from pyspark.sql import functions as F

    result_df = spark_df \
        .filter(F.col('age') > 30)
                    
    # Afficher le résultat
    return result_df

