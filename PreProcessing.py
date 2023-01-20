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

# COMMAND ----------

def countRows(data_movie):
  rows = data_movie.count()
  return print(f"DataFrame Rows count : {rows}")


# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC # KEYWORDS

# COMMAND ----------

df_keywords = read_movies_data("keywords")
df_keywords = df_keywords.withColumnRenamed("id", "id_global")
display(df_keywords)

# COMMAND ----------

from pyspark.sql.functions import *
df_keywords=df_keywords.withColumn('keywords',when(col('keywords')=='[]',"[{'id': 0, 'name': 'Unknown'}]").otherwise(col('keywords')))
df_keywords = df_keywords.filter(~df_keywords.keywords.like("%}"))

df_keywords.show()
display(df_keywords)
countRows(df_keywords)
EmptyRows(df_keywords).show()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import explode

# Define the schema for the struct type
json_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_keywords = df_keywords.withColumn("keywords", from_json(col("keywords"), json_schema))

# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_keywords = df_keywords.selectExpr("*", "explode(keywords) as e").selectExpr("*", "e.*")
columns_to_drop = ["keywords", "e"]
df_keywords = df_keywords.select([c for c in df_keywords.columns if c not in columns_to_drop])
df_keywords = df_keywords.withColumnRenamed("name", "keywords_name")
df_keywords = df_keywords.withColumnRenamed("id", "keywords_id")
display(df_keywords)
countRows(df_keywords)
EmptyRows(df_keywords).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Links_small

# COMMAND ----------

df_links_small = read_movies_data("links_small")
display(df_links_small)
df_links_small = df_links_small.dropna()
countRows(df_links_small)
EmptyRows(df_links_small).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Rating_small

# COMMAND ----------

df_ratings_small = read_movies_data("ratings_small")
df_ratings_small = df_ratings_small.drop("timestamp")
df_ratings_small.printSchema()
countRows(df_ratings_small)
EmptyRows(df_ratings_small).show()
display(df_ratings_small)

# COMMAND ----------

df_movies_metadata = read_movies_data("movies_metadata")
df_movies_metadata = df_movies_metadata.withColumnRenamed("id", "id_global")
countRows(df_movies_metadata)
display(df_movies_metadata)

# COMMAND ----------

df_movies_metadata = df_movies_metadata.drop("adult", "belongs_to_collection", "homepage", "original_language", "overview", "poster_path", "production_companies", "production_countries", "spoken_languages", "status", "tagline", "title", "video", "release_date")
from pyspark.sql.functions import *
df_movies_metadata = df_movies_metadata.withColumn("imdb_id", regexp_replace(col("imdb_id"), "tt", ""))
df_movies_metadata.show()

# COMMAND ----------

from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Define the schema for the struct type
json_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_movies_metadata = df_movies_metadata.withColumn("genres", from_json(col("genres"), json_schema))


# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_movies_metadata = df_movies_metadata.selectExpr("*", "explode(genres) as f").selectExpr("*", "f.*")
columns_to_drop = ["genres", "f"]
df_movies_metadata = df_movies_metadata.select([c for c in df_movies_metadata.columns if c not in columns_to_drop])
df_movies_metadata = df_movies_metadata.withColumnRenamed("name", "gender_name")
df_movies_metadata = df_movies_metadata.withColumnRenamed("id", "gender_id")
df_movies_metadata = df_movies_metadata.withColumnRenamed("imdb_id", "imdbid")
df_movies_metadata.printSchema()
df_movies_metadata.show()

# COMMAND ----------

# Define the columns to convert
cols_to_convert = ["budget", "id_global", "vote_count", "id", "revenue"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df_movies_metadata = df_movies_metadata.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_movies_metadata.columns])
df_movies_metadata.show()

# COMMAND ----------

# Define the columns to convert
cols_to_convert = ["popularity", "runtime", "vote_average"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("double")

# Convert the columns to string format
df_movies_metadata = df_movies_metadata.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_movies_metadata.columns])
df_movies_metadata.show()

# COMMAND ----------

display(df_movies_metadata)

# COMMAND ----------

# from pyspark.sql.functions import to_timestamp

# df_movies_metadata = df_movies_metadata.withColumn("release_date", to_timestamp(df_movies_metadata["release_date"], "yyyy-MM-dd"))
# df_movies_metadata.show()

# COMMAND ----------

countRows(df_movies_metadata)
EmptyRows(df_movies_metadata).show()

# COMMAND ----------

PysparkDF = df_movies_metadata.select([count(when(
                            col(c).isNull(), c 
                           )).alias(c)
                      for c in df_movies_metadata.columns])

PysparkDF.show()

# COMMAND ----------

df_movies_metadata = df_movies_metadata.dropna(subset=["id_global", "budget"])
countRows(df_movies_metadata)

# COMMAND ----------

PysparkDF = df_movies_metadata.select([count(when(
                            col(c).isNull(), c 
                           )).alias(c)
                      for c in df_movies_metadata.columns])

PysparkDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN

# COMMAND ----------

df_movies_metadata.show()

# COMMAND ----------

df_ratings_small.show()

# COMMAND ----------

df_links_small.show()

# COMMAND ----------

# Join the DataFrames on the "id" column
joined_df = df_links_small.join(df_ratings_small, "movieId")

# Show the joined DataFrame
joined_df.show()

# COMMAND ----------

final = joined_df.join(df_movies_metadata, "imdbId")

# Show the joined DataFrame
final.show()

# COMMAND ----------

countRows(final)
EmptyRows(final).show()

# COMMAND ----------

# Select the distinct values of the "column_name" column
distinct_values = final.select(col("popularity").alias("distinct_values")).distinct()

# Show the distinct values
distinct_values.show()

# COMMAND ----------

mean_value = distinct_values.select(mean('distinct_values')).collect()[0][0]
mean_value

# COMMAND ----------

final = final.na.fill({'popularity': mean_value})

# COMMAND ----------

# MAGIC %md
# MAGIC # 1

# COMMAND ----------

# Select the distinct values of the "column_name" column
distinct_values = final.select(col("revenue").alias("distinct_values")).distinct()

mean_value = distinct_values.select(mean('distinct_values')).collect()[0][0]
mean_value

# COMMAND ----------

final = final.na.fill({'revenue': mean_value})

# COMMAND ----------

# MAGIC %md
# MAGIC # 2

# COMMAND ----------

# Select the distinct values of the "column_name" column
distinct_values = final.select(col("runtime").alias("distinct_values")).distinct()

mean_value = distinct_values.select(mean('distinct_values')).collect()[0][0]
mean_value

# COMMAND ----------

final = final.na.fill({'runtime': mean_value})

# COMMAND ----------

# MAGIC %md
# MAGIC # 3

# COMMAND ----------

# Select the distinct values of the "column_name" column
distinct_values = final.select(col("vote_average").alias("distinct_values")).distinct()

mean_value = distinct_values.select(mean('distinct_values')).collect()[0][0]
mean_value

# COMMAND ----------

final = final.na.fill({'vote_average': mean_value})

# COMMAND ----------

# MAGIC %md
# MAGIC # 4

# COMMAND ----------

# Select the distinct values of the "column_name" column
distinct_values = final.select(col("vote_count").alias("distinct_values")).distinct()

mean_value = distinct_values.select(mean('distinct_values')).collect()[0][0]
mean_value

# COMMAND ----------

final = final.na.fill({'vote_count': mean_value})

# COMMAND ----------

final.show()

# COMMAND ----------

PysparkDF = final.select([count(when(col(c).contains('None'), c 
                           )).alias(c)
                      for c in final.columns])
PysparkDF.show()

# COMMAND ----------

final = final.withColumn("original_title", when(col("original_title").contains("None"), "Unknown").otherwise(col("original_title")))
PysparkDF = final.select([count(when(col(c).contains('None'), c 
                           )).alias(c)
                      for c in final.columns])
PysparkDF.show()

# COMMAND ----------

countRows(final)
EmptyRows(final).show()

# COMMAND ----------

# Drop multiple columns 'A', 'C'
final = final.drop("imdbId","tmdbId", "id_global")

# COMMAND ----------

display(final)
