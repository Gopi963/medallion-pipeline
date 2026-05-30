from pyspark.sql.functions import col, avg, count, round, max, min

SILVER_PATH = "data/silver/insurance"
GOLD_PATH_SUMMARY = "data/gold/charges_by_region"
GOLD_PATH_SMOKER = "data/gold/charges_by_smoker_agegroup"

def run_gold(spark):
    df = spark.read.format("delta").load(SILVER_PATH)

    summary = (
        df.groupBy("region")
        .agg(
            count("*").alias("total_customers"),
            round(avg("charges"), 2).alias("avg_charge"),
            round(min("charges"), 2).alias("min_charge"),
            round(max("charges"), 2).alias("max_charge")
        )
        .orderBy("avg_charge", ascending=False)
    )
    summary.show()
    summary.write.format("delta").mode("overwrite").save(GOLD_PATH_SUMMARY)
    print(f"  Gold summary saved to {GOLD_PATH_SUMMARY}")

    smoker_age = (
        df.groupBy("smoker", "age_group")
        .agg(
            count("*").alias("total_customers"),
            round(avg("charges"), 2).alias("avg_charge")
        )
        .orderBy("smoker", "age_group")
    )
    smoker_age.show()
    smoker_age.write.format("delta").mode("overwrite").save(GOLD_PATH_SMOKER)
    print(f"  Gold smoker/age group saved to {GOLD_PATH_SMOKER}")