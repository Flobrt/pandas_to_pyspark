from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def Concat_MedicalInfo():
    matrix = [
        ["John Doe", "Diabetes"],
        ["Jane Smith", "Heart Disease"],
        ["Alice Brown", "Hypertension"],
    ]

    columns = ["patient_name", "diagnosis"]

    # Créez une session Spark
    spark = SparkSession.builder.appName("AppName").getOrCreate()

    spark_df = spark.createDataFrame(matrix, columns)

    result_df = spark_df.withColumn(
        "diagnosis_lower", F.lower(F.col("diagnosis"))
    ).withColumn(
        "full_info",
        F.concat(F.col("patient_name"), F.lit(" - "), F.col("diagnosis_lower")),
    )

    # Afficher le résultat
    return result_df
