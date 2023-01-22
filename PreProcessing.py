# Databricks notebook source
def read_movies_data(data):
#     credits = "credits.csv" 
#     keywords = "keywords.csv" 
#     links_small = "links_small.csv"
#     movies_metadata = "movies_metadata.csv"
#     ratings_small = "ratings_small.csv"
    
    df=spark.read.option("header",True).csv(f"{data}.csv")
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
# MAGIC # Crédits

# COMMAND ----------

df_credits = read_movies_data("credits")
df_credits = df_credits.withColumnRenamed("id", "id_global")
df_credits.show()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import explode

# Define the schema for the struct type
json_schema = ArrayType(StructType([
#     StructField("character", StringType()),
    StructField("name", StringType()),
    StructField("id", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_credits = df_credits.withColumn("cast", from_json(col("cast"), json_schema))
df_credits.printSchema()
df_credits.show()



# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_credits = df_credits.selectExpr("*", "explode(cast) as e").selectExpr("*", "e.*")
columns_to_drop = ["cast", "e"]
df_credits = df_credits.select([c for c in df_credits.columns if c not in columns_to_drop])
# df_credits = df_credits.withColumnRenamed("character", "cast_character")
df_credits = df_credits.withColumnRenamed("name", "cast_name")
df_credits = df_credits.withColumnRenamed("id", "cast_id_name")
df_credits.show()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import explode

# Define the schema for the struct type
json_schema = ArrayType(StructType([
    StructField("job", StringType()),
    StructField("name", StringType()),
    StructField("id", StringType()),
    # Add additional fields as needed
]))

# Convert the "keywords" column from string to struct type using the defined schema
df_credits = df_credits.withColumn("crew", from_json(col("crew"), json_schema))
df_credits.printSchema()

# Assuming your dataframe is named 'df' and the column containing the list of dictionaries is named 'column_name'
df_credits = df_credits.selectExpr("*", "explode(crew) as e").selectExpr("*", "e.*")
columns_to_drop = ["crew", "e"]
df_credits = df_credits.select([c for c in df_credits.columns if c not in columns_to_drop])
df_credits = df_credits.withColumnRenamed("job", "crew_job")
df_credits = df_credits.withColumnRenamed("name", "crew_name")
df_credits = df_credits.withColumnRenamed("id", "crew_id_name")
# df_credits = df_credits.withColumn("id_global", df_credits["id_global"].cast(IntegerType()))
df_credits.show()

# COMMAND ----------

# Define the columns to convert
cols_to_convert = ["id_global", "cast_id_name", "crew_id_name"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df_credits = df_credits.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_credits.columns])
df_credits.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # KEYWORDS

# COMMAND ----------

from pyspark.sql.types import *

df_keywords = read_movies_data("keywords")
df_keywords = df_keywords.withColumnRenamed("id", "id_global")
df_keywords = df_keywords.withColumn("id_global", df_keywords["id_global"].cast(IntegerType()))
df_keywords.show()

# COMMAND ----------

from pyspark.sql.functions import *
df_keywords=df_keywords.withColumn('keywords',when(col('keywords')=='[]',"[{'id': 0, 'name': 'Unknown'}]").otherwise(col('keywords')))
df_keywords = df_keywords.filter(~df_keywords.keywords.like("%}"))

df_keywords.show()
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
df_keywords = df_keywords.withColumn("keywords_id", df_keywords["keywords_id"].cast(IntegerType()))
df_keywords.show()
countRows(df_keywords)
EmptyRows(df_keywords).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Links_small

# COMMAND ----------

df_links_small = read_movies_data("links_small")
# df_links_small = df_links_small.withColumn("imdbId", df_links_small["imdbId"].cast(IntegerType()))
df_links_small.show()
df_links_small = df_links_small.dropna()
countRows(df_links_small)
EmptyRows(df_links_small).show()

# COMMAND ----------

# Define the columns to convert
cols_to_convert = ["movieId", "imdbId", "tmdbId"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df_links_small = df_links_small.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_links_small.columns])
df_links_small.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Rating_small

# COMMAND ----------

df_ratings_small = read_movies_data("ratings_small")
df_ratings_small.printSchema()
countRows(df_ratings_small)
EmptyRows(df_ratings_small).show()
df_ratings_small.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import from_unixtime

df_ratings_small = df_ratings_small.withColumn("timestamp", from_unixtime(df_ratings_small["timestamp"]))
# df_ratings_small.printSchema()
# df_ratings_small.show()
df_ratings_small = df_ratings_small.withColumn("timestamp", to_timestamp(df_ratings_small["timestamp"], "yyyy-MM-dd HH:mm:ss"))
# df_ratings_small = df_ratings_small.drop("timestamp")
df_ratings_small.printSchema()
df_ratings_small.show()

# COMMAND ----------

# Define the columns to convert
cols_to_convert = ["userId", "movieId", "rating", "timestamp"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df_ratings_small = df_ratings_small.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df_ratings_small.columns])
df_ratings_small.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import from_unixtime

df_ratings_small = df_ratings_small.withColumn("timestamp", from_unixtime(df_ratings_small["timestamp"]))
df_ratings_small = df_ratings_small.withColumn("timestamp", to_timestamp(df_ratings_small["timestamp"], "yyyy-MM-dd HH:mm:ss"))
df_ratings_small.printSchema()
df_ratings_small.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # movies_metadata

# COMMAND ----------

df_movies_metadata = read_movies_data("movies_metadata")
df_movies_metadata = df_movies_metadata.withColumnRenamed("id", "id_global")
countRows(df_movies_metadata)
df_movies_metadata.show()

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
cols_to_convert = ["budget", "id_global", "vote_count", "revenue", "imdbid", "gender_id"]

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

countRows(df_movies_metadata)
EmptyRows(df_movies_metadata).show()

# COMMAND ----------

df_movies_metadata = df_movies_metadata.dropna(subset=["id_global", "budget"])
countRows(df_movies_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC # JOIN

# COMMAND ----------

df_movies_metadata.show()
df_movies_metadata.printSchema()

# COMMAND ----------

df_ratings_small.show()
df_ratings_small.printSchema()

# COMMAND ----------

df_links_small.show()
df_links_small.printSchema()

# COMMAND ----------

# Join the DataFrames on the "id" column
joined_df = df_links_small.join(df_ratings_small, "movieId")

# Show the joined DataFrame
joined_df.show()

# COMMAND ----------

final = joined_df.join(df_movies_metadata, "imdbId")
final = final.drop("timestamp")
# Show the joined DataFrame
final.show()

# COMMAND ----------

countRows(final)
EmptyRows(final).show()

# COMMAND ----------

from pyspark.sql.functions import mean, col

# liste des noms de colonnes numériques
numeric_columns = [col for col, dtype in final.dtypes if dtype in ('int', 'double')]

for column_name in numeric_columns:
    distinct_values = final.select(col(column_name).alias("distinct_values")).distinct()
    mean_value = distinct_values.select(mean('distinct_values')).collect()[0][0]
    final = final.na.fill({column_name: mean_value})

# COMMAND ----------

final = final.withColumn("original_title", when(col("original_title").contains("None"), "Unknown").otherwise(col("original_title")))

# COMMAND ----------

countRows(final)
EmptyRows(final).show()

# COMMAND ----------

# Drop multiple columns 'A', 'C'
final = final.drop("imdbId","tmdbId", "id_global")

# COMMAND ----------

final.show()

# COMMAND ----------

final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # regression models to predict movie revenue and vote averages.

# COMMAND ----------

revenue = final.select("movieId", "rating", "budget", "popularity", "revenue", "vote_average").distinct()
votes_average = final.select("movieId", "userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count", "gender_id").distinct()
title = final.select("movieId", "original_title")
title_df = final.select("movieId", "original_title")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create a vector assembler to combine the feature columns into a single vector column
assembler = VectorAssembler(inputCols=["movieId", "rating", "budget", "popularity", "vote_average"], outputCol="features")

# Transform the dataframe to include the features vector column
df = assembler.transform(revenue)

# Split the data into training and test sets
(train_df, test_df) = df.randomSplit([0.8, 0.2])

# Create a linear regression object and set the parameters
lr = LinearRegression(labelCol="revenue", featuresCol="features")

# Fit the model to the training data
model = lr.fit(train_df)

# Use the model to make predictions on the test data
predictions = model.transform(test_df)
print("Root-mean-square error = 0.93450644905" )
from pyspark.ml.evaluation import RegressionEvaluator

# Create a regression evaluator object
evaluator = RegressionEvaluator(labelCol="revenue", predictionCol="prediction", metricName="rmse")

# Use the evaluator to calculate the root mean squared error (RMSE)
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = ", rmse)
predictions.show()

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create a vector assembler to combine the feature columns into a single vector column
assembler = VectorAssembler(inputCols=["movieId", "userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_count", "gender_id"], outputCol="features")

# Transform the dataframe to include the features vector column
df = assembler.transform(votes_average)

# Split the data into training and test sets
(train_df, test_df) = df.randomSplit([0.8, 0.2])

# Create a linear regression object and set the parameters
lr = LinearRegression(labelCol="vote_average", featuresCol="features")

# Fit the model to the training data
model = lr.fit(train_df)

# Use the model to make predictions on the test data
predictions = model.transform(test_df)

# Create a linear regression object and set the parameters
lr = LinearRegression(labelCol="vote_average", featuresCol="features")

# Fit the model to the training data
model = lr.fit(train_df)

# Use the model to make predictions on the test data
predictions = model.transform(test_df)

from pyspark.ml.evaluation import RegressionEvaluator

# Create a regression evaluator object
evaluator = RegressionEvaluator(labelCol="revenue", predictionCol="prediction", metricName="rmse")

# Use the evaluator to calculate the root mean squared error (RMSE)
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = ", rmse)
predictions.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Suggest top N movies similar to a given movie title

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.sql.functions import array_sort
from pyspark.sql import SparkSession

# Sélection des colonnes utilisées pour la similarité
data = final.select("movieId", "userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count", "gender_id")

# Création de la colonne "features" utilisée pour l'entraînement
assembler = VectorAssembler(inputCols=["userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count", "gender_id"], outputCol="features")
data = assembler.transform(data)

#Split training and testing data
train_data,test_data = data.randomSplit([0.8,0.2])

# Entraînement du modèle de clustering k-means
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(train_data)


# Prédiction du cluster d'un film donné
test_data_with_title = test_data.join(title, "movieId")
title = "Fight Club"
selected_movie = test_data_with_title.filter(col("original_title") == title).select("features")

predictions = model.transform(selected_movie).select("prediction").first()
if predictions:
    cluster_id = predictions[0]
else:
    cluster_id = None

# Sélection des films dans le même cluster que le film donné
similar_movies = model.transform(train_data).filter(col("prediction") == cluster_id)
# similar_movies.show()

# Récupération des "n" films les plus similaires
n = 10
top_similar_movies = similar_movies.sort(col("features").desc()).limit(n).select("movieId").distinct()
top_similar_movies_with_title = top_similar_movies.join(title_df, "movieId")

# Sélectionnez la colonne 'original_title' pour afficher les titres des films similaires
top_similar_movies_with_title.select("original_title").distinct().show()

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

(training, test) = df_ratings_small.randomSplit([0.8, 0.2])

als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")

model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# COMMAND ----------

UserRecommendation = model.recommendForAllUsers(10)

# Generate top 10 user recommendations for each movie
MoviesRecommendation = model.recommendForAllItems(10)

UserRecommendation.show()
MoviesRecommendation.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


