from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def Agg_Departments():
    matrix = [
        ['1', 34, 'Cardiology', 10],
        ['2', 45, 'Neurology', 12],
        ['3', 23, 'Cardiology', 5],
        ['4', 64, 'Orthopedics', 8],
        ['5', 52, 'Cardiology', 9],
    ]

    columns = ['patient_id', 'age', 'department', 'visit_count']

    # Créez une session Spark
    spark = SparkSession.builder \
        .appName("AppName") \
        .getOrCreate()

    spark_df = spark.createDataFrame(matrix, columns)
    from pyspark.sql import functions as F

    result_df = spark_df \
        .groupBy(F.col('department')) \
        .agg(
            F.sum('visit_count').alias('sum'),
            F.mean('age').alias('mean'),
            F.max('age').alias('max'),
        ).orderBy(F.col('department')) \
        

    # Afficher le résultat
    return result_df

