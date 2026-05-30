import os
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from src.bronze import run_bronze
from src.silver import run_silver
from src.gold import run_gold
from pyspark.sql import SparkSession

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("MedallionPipeline")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

if __name__ == "__main__":
    print("Starting Medallion Pipeline...")
    spark = create_spark_session()
    print("Bronze layer...")
    run_bronze(spark)
    print("Silver layer...")
    run_silver(spark)
    print("Gold layer...")
    run_gold(spark)
    print("Pipeline complete!")
    spark.stop()