# Install pyspark if running in Colab/Local (skip if in IBM Lab)
!pip install pyspark findspark

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression

# 1. Create Spark Session
spark = SparkSession.builder.appName("Final Project").getOrCreate()

# 2. Load the CORRECT "dirty" Lab Dataset (NOT the UCI one)
# We use the URL provided in the PDF
!wget -O NASA_airfoil_noise_raw.csv https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-BD0231EN-Coursera/datasets/NASA_airfoil_noise_raw.csv

df = spark.read.csv("NASA_airfoil_noise_raw.csv", header=True, inferSchema=True)

# 3. Perform ETL (Cleaning) as per Lab Instructions
df = df.dropDuplicates()
df = df.dropna()
df = df.withColumnRenamed("SoundLevel", "SoundLevelDecibels")

# 4. Define Pipeline Stages
assembler = VectorAssembler(
    inputCols=['Frequency', 'AngleOfAttack', 'ChordLength', 'FreeStreamVelocity', 'SuctionSideDisplacement'],
    outputCol="features"
)
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
lr = LinearRegression(featuresCol="scaledFeatures", labelCol="SoundLevelDecibels")

pipeline = Pipeline(stages=[assembler, scaler, lr])

# 5. Split Data (CRITICAL: Must use seed=42 as per PDF)
(trainingData, testingData) = df.randomSplit([0.7, 0.3], seed=42)

# 6. Fit Model
pipelineModel = pipeline.fit(trainingData)
lrModel = pipelineModel.stages[-1]

# 7. Print Coefficients for Q16-Q20
# Matches the output format required by the quiz
input_columns = ['Frequency', 'AngleOfAttack', 'ChordLength', 'FreeStreamVelocity', 'SuctionSideDisplacement']
print("\n--- ANSWERS FOR Q16-Q20 ---")
for col_name, coef in zip(input_columns, lrModel.coefficients):
    print(f"Coefficient for {col_name}: {round(coef, 4)}")

# 8. Print Intercept (to verify against Q14)
print(f"Intercept: {round(lrModel.intercept, 2)}")
