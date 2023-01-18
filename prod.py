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
df_keywords = df_keywords.withColumnRenamed("id", "id_global")
df_keywords.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType
df_keywords = df_keywords.withColumn("id_global", df_keywords["id_global"].cast(IntegerType()))
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
columns_to_drop = ["keywords", "e"]
df_keywords = df_keywords.select([c for c in df_keywords.columns if c not in columns_to_drop])
df_keywords = df_keywords.withColumnRenamed("name", "keywords_name")
df_keywords = df_keywords.withColumnRenamed("id", "keywords_id")
df_keywords.show()

# COMMAND ----------

EmptyRows(df_keywords).show()
rows = df_keywords.count()
print(f"DataFrame Rows count : {rows}")

# COMMAND ----------

import matplotlib.pyplot as plt
# Group the DataFrame by ID and count the number of keywords for each ID
df_grouped = df_keywords.groupBy("id_global").count()

# Sort the grouped DataFrame in descending order of count
df_sorted = df_grouped.sort(col("count").desc())

# Get the first row of the sorted DataFrame (which will contain the ID with the most keywords)
# most_keywords_id = df_sorted.first()["id_keywords"]
df_head = df_sorted.limit(5).collect()
# df_head = df_sorted.head(10)
# Print the ID with the most keywords
df_head = [(row['id_global'], row['count']) for row in df_head]
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

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Define the schema for the struct type
json_schema = ArrayType(StructType([
#     StructField("character", StringType()),
    StructField("name", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_credits = df_credits.withColumn("cast", from_json(col("cast"), json_schema))
df_credits.printSchema()
df_credits.show()

# COMMAND ----------

from pyspark.sql.functions import explode

# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_credits = df_credits.selectExpr("*", "explode(cast) as e").selectExpr("*", "e.*")
columns_to_drop = ["cast", "e"]
df_credits = df_credits.select([c for c in df_credits.columns if c not in columns_to_drop])
# df_credits = df_credits.withColumnRenamed("character", "cast_character")
df_credits = df_credits.withColumnRenamed("name", "cast_namer")
df_credits.show()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Define the schema for the struct type
json_schema = ArrayType(StructType([
    StructField("job", StringType()),
    StructField("name", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_credits = df_credits.withColumn("crew", from_json(col("crew"), json_schema))
df_credits.printSchema()
df_credits.show()

# COMMAND ----------

from pyspark.sql.functions import explode

# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_credits = df_credits.selectExpr("*", "explode(crew) as e").selectExpr("*", "e.*")
columns_to_drop = ["crew", "e"]
df_credits = df_credits.select([c for c in df_credits.columns if c not in columns_to_drop])
df_credits = df_credits.withColumnRenamed("job", "crew_job")
df_credits = df_credits.withColumnRenamed("name", "crew_namer")
df_credits = df_credits.withColumnRenamed("id", "id_global")
df_credits = df_credits.withColumn("id_global", df_credits["id_global"].cast(IntegerType()))
df_credits.show()

# COMMAND ----------

display(df_credits)

# COMMAND ----------

from pyspark.sql.functions import *

# Assume your DataFrame is named "df"
# df_credits = df_credits.groupBy("id_global","crew_namer","crew_job", "cast_namer").agg(concat_ws(",",collect_list("crew_namer")).alias("crew_namer"))
df_credits = df_credits.groupBy("id_global","crew_job", "cast_namer", "crew_namer").pivot("crew_job").agg(first("crew_namer"))

# COMMAND ----------

display(df_credits)

# COMMAND ----------

df_credits = df_credits.drop("crew_namer", "crew_job")
display(df_credits)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



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
df_ratings_small = df_ratings_small.drop("timestamp")
df_ratings_small.printSchema()
df_ratings_small.show()


# COMMAND ----------

# df_mean = df_ratings_small.drop("userId")
# df_mean.show()

# COMMAND ----------

from pyspark.sql.functions import mean

df_ratings_small = df_ratings_small.groupBy("userId", "movieId").agg(mean("rating").alias("average_rating"))
df_ratings_small.show()

# COMMAND ----------

# from pyspark.sql.functions import mean

# # group the dataframe by movieID and calculate the mean of the ratings
# df_mean = df_ratings_small.groupBy("movieId").mean("rating")

# # # rename the mean column to "avg_rating"
# df_mean = df_mean.withColumnRenamed("avg(rating)", "avg_rating")
# df_mean.show()
# # # join the original dataframe with the mean dataframe on movieID
# # df_ratings_small = df_ratings_small.drop("rating")
# # df_ratings_small = df_ratings_small.join(df_mean, "movieID")
# # df_ratings_small.show()

# COMMAND ----------

df_movies_metadata = read_movies_data("movies_metadata")
df_movies_metadata = df_movies_metadata.withColumnRenamed("id", "id_global")
countRows(df_movies_metadata)
display(df_movies_metadata)

# COMMAND ----------

df_movies_metadata = df_movies_metadata.drop("video", "belongs_to_collection", "adult","homepage", "original_language", "overview", "poster_path", "production_companies", "production_countries", "spoken_languages", "status", "tagline", "release_date", "title")
df_movies_metadata.printSchema()
display(df_movies_metadata)

# COMMAND ----------

from pyspark.sql.functions import *
df_movies_metadata = df_movies_metadata.withColumn("imdb_id", regexp_replace(col("imdb_id"), "tt", ""))
display(df_movies_metadata)

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
df_movies_metadata = df_movies_metadata.withColumn("genres", from_json(col("genres"), json_schema))
df_movies_metadata.printSchema()
df_movies_metadata.show()

# COMMAND ----------

from pyspark.sql.functions import explode

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
cols_to_convert = ["budget", "id_global", "imdb_id", "vote_count", "id", "revenue"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df_movies_metadata = df_movies_metadata.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_movies_metadata.columns])
df_movies_metadata.printSchema()
df_movies_metadata.show()

# COMMAND ----------

# Define the columns to convert
cols_to_convert = ["popularity", "runtime", "vote_average"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("double")

# Convert the columns to string format
df_movies_metadata = df_movies_metadata.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_movies_metadata.columns])
df_movies_metadata.printSchema()
df_movies_metadata.show()

# COMMAND ----------

EmptyRows(df_movies_metadata).show()
rows = df_movies_metadata.count()
print(f"DataFrame Rows count : {rows}")

# COMMAND ----------

movielens = df_movies_metadata.join(df_keywords,["id_global"],"left")
movielens.show()

# COMMAND ----------

display(movielens)

# COMMAND ----------

movielens = df_movies_metadata.join(df_links_small,["imdbid"])
movielens.show()

# COMMAND ----------

movielens2 = movielens.join(df_ratings_small,["movieId"])

# COMMAND ----------

display(movielens2)

# COMMAND ----------

movielens3 = movielens2.join(df_credits,["id_global"])

# COMMAND ----------

# movielens = movielens.join(df_mean,["movieId"])
# movielens.show()

# COMMAND ----------

display(movielens3)

# COMMAND ----------



# COMMAND ----------

# movielens.write.csv("dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/movies_data_final.csv", header=True)
pdf = movielens3.toPandas()

# Write the Pandas DataFrame to a CSV file
pdf.to_csv("/dbfs/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/tp/movies_final.csv", index=False)

# COMMAND ----------

# # df_ratings_small.write.csv("dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/rating_final.csv", header=True)
# # movielens.write.csv("dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/movies_data_final.csv", header=True)
# pdf = df_ratings_small.toPandas()

# # Write the Pandas DataFrame to a CSV file
# pdf.to_csv("/dbfs/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/tp/ratings_small_final.csv", index=False)
