# Databricks notebook source
# dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/tp/recommendation.csv
df=spark.read.option("header",True).csv("dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/tp/model.csv")
display(df)

# COMMAND ----------

df.show()
df.printSchema()

# COMMAND ----------

rows = df.count()
print(f"DataFrame Rows count : {rows}")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, IntegerType

# Define the columns to convert
cols_to_convert = ["movieId", "userId", "budget", "revenue", "vote_count", "gender_id"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("integer")

# Convert the columns to string format
df = df.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df.columns])
df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, IntegerType

# Define the columns to convert
cols_to_convert = ["rating", "popularity", "runtime", "vote_average"]

# Define the function to convert to string format
convert_to_string = lambda x: x.cast("float")

# Convert the columns to string format
df = df.select([convert_to_string(col(c)).alias(c) if c in cols_to_convert else c for c in df.columns])
df.printSchema()


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

data = df.select("movieId", "userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count", "gender_id")

# Prepare data for model
assembler = VectorAssembler(inputCols=["userId", "rating", "budget", "popularity", "movieId", "runtime", "vote_average", "vote_count", "gender_id"], outputCol="features")
data = assembler.transform(data)

# Split data into training and test sets
(trainingData, testData) = data.randomSplit([0.8, 0.2])

# Create linear regression model
lr = LinearRegression(featuresCol="features", labelCol="revenue")

# Train model on training data
model = lr.fit(trainingData)

# Use model to predict on test data
predictions = model.transform(testData)
predictions.show()

# COMMAND ----------

print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

# Create evaluator
evaluator = RegressionEvaluator(labelCol="revenue", predictionCol="prediction", metricName="rmse")

# Compute RMSE
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.sql.functions import array_sort
from pyspark.sql import SparkSession

# Sélection des colonnes utilisées pour la similarité
data = df.select("movieId", "userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count", "gender_id")

# Création de la colonne "features" utilisée pour l'entraînement
assembler = VectorAssembler(inputCols=["userId", "rating", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count", "gender_id"], outputCol="features")
data = assembler.transform(data)

#Split training and testing data
train_data,test_data = data.randomSplit([0.8,0.2])

# Entraînement du modèle de clustering k-means
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(train_data)

# Prédiction du cluster d'un film donné
movieId = 2403
selected_movie = test_data.filter(col("movieId") == movieId).select("features")
# selected_movie.show()

predictions = model.transform(selected_movie).select("prediction").first()
if predictions:
    cluster_id = predictions[0]
else:
    cluster_id = None
# cluster_id = model.transform(selected_movie).select("prediction").collect()[0][0]

# Sélection des films dans le même cluster que le film donné
similar_movies = model.transform(train_data).filter(col("prediction") == cluster_id)
# similar_movies.show()

# Récupération des N films les plus similaires
n = 10
top_similar_movies = similar_movies.sort(col("features").desc()).limit(n).select("movieId").distinct()

# Afficher les films similaires
top_similar_movies.show()

# COMMAND ----------

# Compute RMSE
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# COMMAND ----------

from pyspark.ml.clustering import KMeans

# Load data
# data = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

# # Train model
# kmeans = KMeans().setK(2).setSeed(1)
# model = kmeans.fit(data)

# Calculate cost
cost = model.computeCost(train_data)
print("Within Set Sum of Squared Errors = " + str(cost))

# COMMAND ----------

from pyspark.ml.evaluation import ClusteringEvaluator

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.sql.functions import array_sort
from pyspark.sql import SparkSession

# Sélection des colonnes utilisées pour la similarité
data = df.select("budget", "popularity", "vote_average", "movieId")

# Création de la colonne "features" utilisée pour l'entraînement
assembler = VectorAssembler(inputCols=["budget", "popularity", "vote_average"], outputCol="features")
data = assembler.transform(data)

#Split training and testing data
train_data,test_data = data.randomSplit([0.8,0.2])

# Entraînement du modèle de clustering k-means
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(train_data)

# COMMAND ----------

# Prédiction du cluster d'un film donné
movieId = 26
selected_movie = test_data.filter(col("movieId") == movieId).select("features")
selected_movie.show()

# COMMAND ----------

predictions = model.transform(selected_movie).select("prediction").first()
if predictions:
    cluster_id = predictions[0]
else:
    cluster_id = None
# cluster_id = model.transform(selected_movie).select("prediction").collect()[0][0]

# Sélection des films dans le même cluster que le film donné
similar_movies = model.transform(train_data).filter(col("prediction") == cluster_id)
similar_movies.show()

# COMMAND ----------

# Récupération des N films les plus similaires
n = 10
top_similar_movies = similar_movies.sort(col("features").desc()).limit(n).select("movieId").distinct()

# Afficher les films similaires
top_similar_movies.show()

# COMMAND ----------

# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.regression import LinearRegression

# #Input all the features in one vector column
# assembler = VectorAssembler(inputCols=['id_global', 'imdbid', 'budget', 'popularity', 'revenue', 'runtime', 'vote_average', 'vote_count', 'gender_id', 'userId', 'average_rating'], outputCol = 'Attributes')
# output = assembler.transform(df)
# output

# COMMAND ----------

# #Input vs Output
# finalized_data = output.select("Attributes","movieId")
# finalized_data.show()

# COMMAND ----------

# #Split training and testing data
# train_data,test_data = finalized_data.randomSplit([0.8,0.2])

# COMMAND ----------

# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import ClusteringEvaluator

# # Trains a k-means model.
# kmeans = KMeans(featuresCol='Attributes').setK(10).setSeed(1)
# model = kmeans.fit(finalized_data)

# # Make predictions
# predictions = model.transform(finalized_data)

# predictions.show()

# COMMAND ----------

# display(predictions)

# COMMAND ----------

# import pandas as pd
# from sklearn.metrics.pairwise import cosine_similarity
# from sklearn.cluster import KMeans

# # Chargement et nettoyage des données
# ratings = pd.read_csv("movie_ratings.csv")

# # Entraînement du modèle de clustering k-means
# kmeans = KMeans(n_clusters=10)
# kmeans.fit(ratings)

# # Calcul de la similarité entre les films
# similarity = cosine_similarity(ratings.T)

# # Prédiction du cluster d'un film donné
# title = "The Shawshank Redemption"
# selected_movie_index = ratings.columns.get_loc(title)
# cluster_id = kmeans.predict(ratings.iloc[:,selected_movie_index].values.reshape(1, -1))

# Sélection des films dans le même cluster que le film donné
# similar_movies = ratings.columns[kmeans.labels_ == cluster_id]

# Récupération des N films les plus similaires
# n = 10
# top_similar_movies = similar_movies[similarity[selected_movie_index][kmeans.labels_ == cluster_id].argsort()[-n-1:-1][::-1]]

# Afficher les films similaires
# print(top_similar_movies)
