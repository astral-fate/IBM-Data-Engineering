
# 1. Install Spark


!pip install pyspark findspark
# 2. Start Session
# Here is the  code using PySpark to complete the tasks in your assignment.

# 1. Install Spark


!pip install pyspark findspark
# 2. Start Session


import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SearchTermAnalysis").getOrCreate()
# 3. Download the Search Term Dataset


!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/Bigdata%20and%20Spark/searchterms.csv
# 4. Load the CSV into a Spark DataFrame


# Load the csv into a spark dataframe
df = spark.read.csv("searchterms.csv", header=True, inferSchema=True)
# 5. Print the Number of Rows and Columns


# Print the number of rows and columns
rows = df.count()
cols = len(df.columns)
print(f"Rows: {rows}, Columns: {cols}")
# 6. Print the Top 5 Rows


# Print the top 5 rows
df.show(5)
# 7. Find the Datatype of the Column searchterm


# Find out the datatype of the column searchterm
df.select("searchterm").printSchema()

# Alternative method to just print the type name:
print(df.schema["searchterm"].dataType)
# 8. How Many Times Was the Term gaming laptop Searched?


# How many times was the term `gaming laptop` searched?
count_gaming_laptop = df.filter(df['searchterm'] == 'gaming laptop').count()
print(f"The term 'gaming laptop' was searched {count_gaming_laptop} times.")
# 9. Print the Top 5 Most Frequently Used Search Terms


# Print the top 5 most frequently used search terms
from pyspark.sql.functions import desc

df.groupBy("searchterm") \
  .count() \
  .orderBy(desc("count")) \
  .show(5)
# 10. Download the Pretrained Sales Forecasting Model


!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/Bigdata%20and%20Spark/model.tar.gz
# Extract the model
!tar -xzf model.tar.gz
# 11. Load the Sales Forecast Model


# Load the sales forecast model
from pyspark.ml.regression import LinearRegressionModel

# Note: The extracted folder name is usually "sales_prediction_model" or similar based on this specific lab.
# If the folder name differs, check your file directory after extraction.
model = LinearRegressionModel.load('sales_prediction_model')
# 12. Predict the Sales for the Year 2023


# Using the sales forecast model, predict the sales for the year of 2023.
from pyspark.ml.feature import VectorAssembler

# Create a dataframe with the year 2023
data_2023 = spark.createDataFrame([(2023,)], ["year"])

# The model usually expects a vector column named "features". 
# We must assemble our "year" column into a vector.
assembler = VectorAssembler(inputCols=["year"], outputCol="features")
data_2023_vector = assembler.transform(data_2023)

# Make the prediction
predictions = model.transform(data_2023_vector)
predictions.select("prediction").show()
Next Step: Would you like me to explain how the VectorAssembler works in the final step, or help you debug if the model path name is different after extraction?

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SearchTermAnalysis").getOrCreate()
# 3. Download the Search Term Dataset


!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/Bigdata%20and%20Spark/searchterms.csv
# 4. Load the CSV into a Spark DataFrame


# Load the csv into a spark dataframe
df = spark.read.csv("searchterms.csv", header=True, inferSchema=True)
# 5. Print the Number of Rows and Columns


# Print the number of rows and columns
rows = df.count()
cols = len(df.columns)
print(f"Rows: {rows}, Columns: {cols}")
# 6. Print the Top 5 Rows


# Print the top 5 rows
df.show(5)
# 7. Find the Datatype of the Column searchterm


# Find out the datatype of the column searchterm
df.select("searchterm").printSchema()

# Alternative method to just print the type name:
print(df.schema["searchterm"].dataType)
# 8. How Many Times Was the Term gaming laptop Searched?


# How many times was the term `gaming laptop` searched?
count_gaming_laptop = df.filter(df['searchterm'] == 'gaming laptop').count()
print(f"The term 'gaming laptop' was searched {count_gaming_laptop} times.")
# 9. Print the Top 5 Most Frequently Used Search Terms


# Print the top 5 most frequently used search terms
from pyspark.sql.functions import desc

df.groupBy("searchterm") \
  .count() \
  .orderBy(desc("count")) \
  .show(5)
# 10. Download the Pretrained Sales Forecasting Model


!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/Bigdata%20and%20Spark/model.tar.gz
# Extract the model
!tar -xzf model.tar.gz
# 11. Load the Sales Forecast Model


# Load the sales forecast model
from pyspark.ml.regression import LinearRegressionModel

# Note: The extracted folder name is usually "sales_prediction_model" or similar based on this specific lab.
# If the folder name differs, check your file directory after extraction.
model = LinearRegressionModel.load('sales_prediction_model')
# 12. Predict the Sales for the Year 2023


# Using the sales forecast model, predict the sales for the year of 2023.
from pyspark.ml.feature import VectorAssembler

# Create a dataframe with the year 2023
data_2023 = spark.createDataFrame([(2023,)], ["year"])

# The model usually expects a vector column named "features". 
# We must assemble our "year" column into a vector.
assembler = VectorAssembler(inputCols=["year"], outputCol="features")
data_2023_vector = assembler.transform(data_2023)

# Make the prediction
predictions = model.transform(data_2023_vector)
predictions.select("prediction").show()
