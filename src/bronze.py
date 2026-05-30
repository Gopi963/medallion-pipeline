import os
from pyspark.sql import SparkSession

RAW_PATH = "data/raw/insurance.csv"
BRONZE_PATH = "data/bronze/insurance"

def create_data():
    if os.path.exists(RAW_PATH):
        print("  Raw data already exists, skipping.")
        return
    print("  Creating insurance dataset...")
    csv_data = """age,sex,bmi,children,smoker,region,charges
19,female,27.9,0,yes,southwest,16884.924
18,male,33.77,1,no,southeast,1725.5523
28,male,33.0,3,no,southeast,4449.462
33,male,22.705,0,no,northwest,21984.47061
32,male,28.88,0,no,northwest,3866.8552
31,female,25.74,0,no,southeast,3756.6216
46,female,33.44,1,no,southeast,8240.5896
37,female,27.74,3,no,northwest,7281.5056
37,male,29.83,2,no,northeast,6406.4107
60,female,25.84,0,no,northwest,28923.13692
25,male,26.22,0,no,northeast,2721.3208
62,female,26.29,0,yes,southeast,27808.7251
23,male,34.4,0,no,southwest,1826.843
56,female,39.82,0,no,southeast,11090.7178
27,male,42.13,0,yes,southeast,39611.7577
19,male,24.6,1,no,southwest,1837.237
52,female,30.78,1,no,northeast,10797.3362
23,female,23.845,0,no,northeast,2395.17155
56,male,40.3,0,no,southwest,10602.385
30,male,35.3,0,yes,southwest,36837.467
60,male,32.8,0,no,southwest,12228.2083
30,female,32.4,1,no,southwest,4149.736
18,male,34.1,0,no,southeast,1137.011
34,female,31.92,1,yes,northeast,37701.8768
37,male,28.025,2,no,northwest,6203.90175
59,female,27.72,3,no,southeast,14001.1338
63,female,23.085,0,no,northeast,14451.83515
55,female,32.775,2,no,northwest,12268.63225
23,male,17.385,1,no,northwest,2775.19215
31,male,36.3,2,yes,southwest,38711.0
22,female,35.6,0,no,southwest,2198.18985
18,male,26.315,0,no,northeast,1708.92575
19,female,28.6,5,no,southwest,4687.797
63,male,28.31,0,no,northwest,13770.0979
28,male,36.4,1,yes,southwest,51194.5591
19,female,20.425,0,no,northwest,1743.21425
62,male,32.005,0,yes,southeast,38709.176
26,male,20.8,0,no,southwest,2302.3
35,male,36.67,1,yes,northeast,39774.2763
60,female,39.9,0,no,southwest,12982.8754
24,female,26.6,0,no,northeast,3046.062
31,male,33.33,4,yes,southeast,46255.1125
41,male,26.41,0,no,northeast,6272.4772
37,male,29.44,2,no,northwest,6313.759
38,female,26.12,2,no,northeast,6079.6718
55,male,38.28,0,no,southeast,10226.2842
18,female,29.165,0,no,northeast,1744.46535
28,female,22.515,2,no,northeast,4719.52785
60,female,28.7,0,no,northwest,13224.693
36,male,20.52,1,yes,northwest,38792.6858
40,female,32.395,2,no,northwest,7986.47455
44,male,32.34,1,no,southeast,7740.337
26,female,24.0,0,no,northeast,2974.126
19,male,31.25,1,no,northwest,1909.5245
21,male,35.53,0,no,southeast,1534.3023
40,female,34.5,2,no,southwest,7281.5056
54,female,31.9,2,yes,southeast,36397.576
47,male,36.08,1,yes,southeast,46113.5112
42,female,36.195,2,no,northeast,8059.6791
29,male,27.0,0,yes,southeast,16297.846
32,female,29.26,2,no,northeast,6406.4107
57,male,38.1,2,no,southeast,11534.87265
29,female,28.05,3,no,northwest,5584.65475
46,male,31.35,2,no,northeast,9500.5725
29,male,28.05,2,no,northwest,4562.8421
43,male,28.025,0,no,northwest,6283.47475
26,female,22.0,0,no,southwest,2686.262
18,male,23.21,0,no,southeast,1121.8739
52,female,28.7,1,no,northwest,10959.6947
31,female,33.155,0,no,northeast,5616.74345
31,male,35.97,1,yes,southeast,42560.4303
37,female,26.4,0,no,northeast,6551.7501
52,male,30.78,2,yes,northeast,44501.3982
53,female,22.88,3,no,northeast,11264.5418
44,male,37.0,2,no,northeast,8068.185
50,male,25.3,0,no,northwest,8827.2101
18,female,31.35,4,no,southeast,2585.2696
19,female,31.0,1,no,southwest,1880.487
32,male,28.9,0,no,northwest,4518.8266
46,female,27.6,0,no,northwest,8342.90946
35,male,34.1,3,no,southeast,5765.099
64,male,26.41,0,no,northeast,14394.5579
60,female,36.005,0,no,northwest,13228.84695
31,female,28.4,1,no,southwest,4239.893
40,female,38.095,0,no,northwest,6652.5288
40,male,34.9,0,no,southwest,6356.271
34,female,26.41,1,no,northeast,5385.33695
45,female,28.0,2,no,southwest,8219.2039
41,female,31.35,0,no,northwest,6360.996
51,male,27.74,0,yes,northwest,58571.0745
48,female,28.88,0,no,northwest,8597.6576
45,male,29.925,4,no,northeast,9788.8659
49,female,27.17,0,yes,southeast,39047.2849
27,male,28.5,0,no,northwest,3782.5965
57,female,29.37,0,yes,northwest,32361.3218
51,female,30.21,0,no,northwest,9377.9047
42,male,30.0,0,yes,southeast,32787.4585
35,female,24.42,3,no,northeast,7227.3498
38,male,29.7,0,no,southwest,5765.099
44,female,26.235,0,no,northwest,7419.4779"""
    with open(RAW_PATH, "w") as f:
        f.write(csv_data)
    print("  Dataset created successfully.")

def run_bronze(spark: SparkSession):
    create_data()
    df = spark.read.option("header", True).option("inferSchema", True).csv(RAW_PATH)
    print(f"  Bronze: {df.count()} rows loaded")
    print(f"  Columns: {df.columns}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(BRONZE_PATH)
    print(f"  Bronze layer saved to {BRONZE_PATH}")