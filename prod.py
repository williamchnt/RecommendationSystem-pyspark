# Databricks notebook source
def read_movies_data(data):
#     credits = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/credits.csv" 
#     keywords = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/keywords.csv" 
#     links = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/links.csv"
#     links_small = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/links_small.csv"
#     movies_metadata = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/movies_metadata.csv"
#     ratings = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/ratings.csv"
#     ratings_small = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/ratings_small.csv"
    
    df=spark.read.option("header",True).csv(f"dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/{data}.csv")
    return df
    
# display(read_movies_data("keywords"))
df_keywords = read_movies_data("keywords")
df_keywords = df_keywords.withColumnRenamed("id", "id_keywords")
df_keywords.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType
df_keywords = df_keywords.withColumn("id_keywords", df_keywords["id_keywords"].cast(IntegerType()))
df_keywords.printSchema()

def countRows(data_movie):
  rows = data_movie.count()
  return print(f"DataFrame Rows count : {rows}")

countRows(df_keywords)

# COMMAND ----------

from pyspark.sql.functions import col,isnan,when,count

def EmptyRows(data_movie):
  PysparkDF = data_movie.select([count(when(col(c).contains('None') | \
                            col(c).contains('"[') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            (col(c) == '[]' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                      for c in data_movie.columns])
  return PysparkDF

EmptyRows(df_keywords).show()

# COMMAND ----------

from pyspark.sql.functions import when
df_keywords=df_keywords.withColumn('keywords',when(col('keywords')=='[]',"[{'id': 0, 'name': 'Unknown'}]").otherwise(col('keywords')))
df_keywords.show()
EmptyRows(df_keywords).show()

# COMMAND ----------

from pyspark.sql.functions import *

df_keywords = df_keywords.filter(~df_keywords.keywords.like("%}"))
df_keywords.show()

# COMMAND ----------

EmptyRows(df_keywords).show()
rows = df_keywords.count()
print(f"DataFrame Rows count : {rows}")

# COMMAND ----------

df_keywords.printSchema()
df_keywords.show()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Define the schema for the struct type
json_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_keywords = df_keywords.withColumn("keywords", from_json(col("keywords"), json_schema))
df_keywords.printSchema()
df_keywords.show()

# COMMAND ----------

from pyspark.sql.functions import explode

# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_keywords = df_keywords.selectExpr("*", "explode(keywords) as e").selectExpr("*", "e.*")
columns_to_drop = ["keywords", "e", "id"]
df_keywords = df_keywords.select([c for c in df_keywords.columns if c not in columns_to_drop])
df_keywords.show()

# COMMAND ----------

EmptyRows(df_keywords).show()
rows = df_keywords.count()
print(f"DataFrame Rows count : {rows}")

# COMMAND ----------

import matplotlib.pyplot as plt
# Group the DataFrame by ID and count the number of keywords for each ID
df_grouped = df_keywords.groupBy("id_keywords").count()

# Sort the grouped DataFrame in descending order of count
df_sorted = df_grouped.sort(col("count").desc())

# Get the first row of the sorted DataFrame (which will contain the ID with the most keywords)
# most_keywords_id = df_sorted.first()["id_keywords"]
df_head = df_sorted.limit(5).collect()
# df_head = df_sorted.head(10)
# Print the ID with the most keywords
df_head = [(row['id_keywords'], row['count']) for row in df_head]
# Plot the pie chart
labels, values = zip(*df_head)
plt.pie(values, labels=labels, autopct='%1.1f%%')
plt.title("Top 5 IDs with most keywords")
plt.show()

# COMMAND ----------

rows = df_sorted.count()
print(f"DataFrame Rows count : {rows}")
df_sorted.show(10)

# COMMAND ----------

df_credits = read_movies_data("credits")
display(df_credits)

# COMMAND ----------

df_links_small = read_movies_data("links_small")
display(df_links_small)

# COMMAND ----------

df_ratings_small = read_movies_data("ratings_small")
df_ratings_small.printSchema()
countRows(df_ratings_small)
df_ratings_small.show()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# Define the columns to convert
cols_to_convert = ["userId", "movieId", "timestamp"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df_ratings_small = df_ratings_small.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_ratings_small.columns])
df_ratings_small = df_ratings_small.withColumn("rating", df_ratings_small["rating"].cast(FloatType()))
df_ratings_small.printSchema()
df_ratings_small.show()

# COMMAND ----------

EmptyRows(df_ratings_small).show()

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

df_ratings_small = df_ratings_small.withColumn("timestamp", from_unixtime(df_ratings_small["timestamp"]))
df_ratings_small.printSchema()
df_ratings_small.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_ratings_small = df_ratings_small.withColumn("timestamp", to_timestamp(df_ratings_small["timestamp"], "yyyy-MM-dd HH:mm:ss"))
df_ratings_small.printSchema()
df_ratings_small.show()

# COMMAND ----------

df_movies_metadata = read_movies_data("movies_metadata")
countRows(df_movies_metadata)
display(df_movies_metadata)

# COMMAND ----------

df_movies_metadata = df_movies_metadata.drop("video","adult","belongs_to_collection","homepage", "original_language", "overview", "poster_path", "production_companies", "production_countries", "spoken_languages", "status", "tagline", "release_date", "revenue", "title", "genres")
df_movies_metadata.printSchema()
display(df_movies_metadata)

# COMMAND ----------

from pyspark.sql.functions import *
df_movies_metadata = df_movies_metadata.withColumn("imdb_id", regexp_replace(col("imdb_id"), "tt", ""))
display(df_movies_metadata)

# COMMAND ----------



# COMMAND ----------


