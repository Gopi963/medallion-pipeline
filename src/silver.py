from pyspark.sql.functions import col, when, upper, trim

BRONZE_PATH = "data/bronze/insurance"
SILVER_PATH = "data/silver/insurance"

def run_silver(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df_clean = (
        df
        .dropDuplicates()
        .dropna(subset=["age", "bmi", "charges"])
        .filter(col("age") > 0)
        .filter(col("bmi") > 0)
        .filter(col("charges") > 0)
        .withColumn("sex", upper(trim(col("sex"))))
        .withColumn("smoker", upper(trim(col("smoker"))))
        .withColumn("region", upper(trim(col("region"))))
        .withColumn("age_group",
            when(col("age") < 30, "Under 30")
            .when(col("age") < 45, "30-44")
            .when(col("age") < 60, "45-59")
            .otherwise("60+")
        )
        .withColumn("bmi_category",
            when(col("bmi") < 18.5, "Underweight")
            .when(col("bmi") < 25.0, "Normal")
            .when(col("bmi") < 30.0, "Overweight")
            .otherwise("Obese")
        )
    )
    print(f"  Silver: {df_clean.count()} rows after cleaning")
    df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"  Silver layer saved to {SILVER_PATH}")