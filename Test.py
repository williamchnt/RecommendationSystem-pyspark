# Databricks notebook source
import pandas as pd
import numpy as np

def read_movies_data():
    credits = "https://chenutfamily.freeboxos.fr:45883/share/z5zYVK5G0HSU5Elt/credits.csv"
    keywords = "https://chenutfamily.freeboxos.fr:45883/share/w7VuOdc5yL7_OooW/keywords.csv" 
    links = "https://chenutfamily.freeboxos.fr:45883/share/Gmz7QCOWH6H0GqsX/links.csv"
    linksSmall = "https://chenutfamily.freeboxos.fr:45883/share/63uzaFuqxgU0758p/links_small.csv"
    moviesMetadata = "https://chenutfamily.freeboxos.fr:45883/share/vA7JOk9UHZRZxIwK/movies_metadata.csv"
    ratings = "https://chenutfamily.freeboxos.fr:45883/share/Q6XXQ-ScvAXlsjeY/ratings.csv"
    ratingsSmall = "https://chenutfamily.freeboxos.fr:45883/share/ZZxgfteOX4i2C0Q4/ratings_small.csv"

    credits = pd.read_csv(credits,low_memory=False,dtype={'id': 'int32', 'cast': 'str', 'crew': 'str'})
    keywords = pd.read_csv(keywords,low_memory=False,dtype={'id': 'int32', 'keywords': 'str'})
    links = pd.read_csv(links,low_memory=False,dtype={'movieId': 'int32', 'imdbId': 'int32', 'tmdbId': 'int32'})
    linksSmall = pd.read_csv(linksSmall,low_memory=False,dtype={'movieId': 'int32', 'imdbId': 'int32', 'tmdbId': 'int32'})
    moviesMetadata = pd.read_csv(moviesMetadata,low_memory=False,dtype={'adult': 'str', 'belongs_to_collection': 'str', 'budget': 'int32', 'genres': 'str', 'homepage': 'str', 'id': 'int32', 'imdb_id': 'str', 'original_language': 'str', 'original_title': 'str', 'overview': 'str', 'popularity': 'float32', 'poster_path': 'str', 'production_companies': 'str', 'production_countries': 'str', 'release_date': 'str', 'revenue': 'int32', 'runtime': 'float32', 'spoken_languages': 'str', 'status': 'str', 'tagline': 'str', 'title': 'str', 'video': 'str', 'vote_average': 'float32', 'vote_count': 'int32'})
    ratings = pd.read_csv(ratings,low_memory=False,dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32', 'timestamp': 'int32'})
    ratingsSmall = pd.read_csv(ratingsSmall,low_memory=False,dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32', 'timestamp': 'int32'})

    return credits, keywords, links, linksSmall, moviesMetadata, ratings, ratingsSmall

# Reduce dataframe memory usage
def reduce_mem_usage(df):
    """ iterate through all the columns of a dataframe and modify the data type
        to reduce memory usage.        
    """
    start_mem = df.memory_usage().sum() / 1024**2
    print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
    
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
        else:
            df[col] = df[col].astype('category')
    
    end_mem = df.memory_usage().sum() / 1024**2
    print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
    print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    
    return df

# COMMAND ----------

credits = "dbfs:/FileStore/shared_uploads/mohamed.zenati@securitasdirect.fr/credits.csv" 

df_credits=spark.read.csv(credits)



# COMMAND ----------

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
df_keywords.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType
# df_keywords.describe()
df_keywords = df_keywords.withColumn("id", df_keywords["id"].cast(IntegerType()))
df_keywords.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, isnull

# Get a list of all the column names
column_names = df_keywords.columns

# Initialize a dictionary to store the counts of empty values
empty_values_counts = {}

# Iterate through the list of column names
for column_name in column_names:
    # Count the number of empty values in each column
    empty_values_count = df_keywords.filter(isnull(col(column_name))).count()
    # Add the count of empty values for each column to the dictionary
    empty_values_counts[column_name] = empty_values_count
    
# Iterate through the key-value pairs in the dictionary
for column_name, empty_values_count in empty_values_counts.items():
    # Print the column name and count of empty values
    print(f"Column '{column_name}' has {empty_values_count} empty values")

# COMMAND ----------

# # df_keywords = df_keywords.dropna(how = 'any')
# from pyspark.sql.functions import col
# df_keywords = df_keywords.filter(df_keywords["id"].isNotNull())

# COMMAND ----------

# df_keywords = df_keywords.filter(col("keywords") != "[]")
from pyspark.sql.functions import when
df_keywords=df_keywords.withColumn('keywords',when(col('keywords')=='[]',"[{'id': 0, 'name': 'Unknown'}]").otherwise(col('keywords')))

# COMMAND ----------

# from pyspark.sql.functions import regexp_replace
# # Remplacer les guillemets par des espaces vides dans la colonne 'value'
# df_keywords = df_keywords.withColumn('keywords', regexp_replace('keywords', '"[', ''))
# # df_keywords = df_keywords.withColumn('keywords', regexp_replace(df_keywords.keywords, '"[', '')
from pyspark.sql.functions import *
df_keywords = df_keywords.filter(~df_keywords.keywords.like("%}"))

# COMMAND ----------

df_keywords.printSchema()
df_keywords.show()

# COMMAND ----------

# df = df_keywords.withColumn("keywords", split(df_keywords["keywords"], ","))

# COMMAND ----------

# df = df_keywords.withColumn("dict", explode("keywords")).withColumn("dict", from_json("dict", "map<string,string>"))

# COMMAND ----------

# df.show()

# COMMAND ----------

# df_keywords = df_keywords.withColumn("id", df_keywords["id"].cast(StringType()))

# COMMAND ----------

# from pyspark.sql.functions import explode, map

# # Create a new DataFrame with two columns, "key" and "value", by
# # exploding the "azerty" column and mapping each dictionary to its
# # key-value pairs
# df_keys_values = df.withColumn("key_value", explode(map(lambda x: x.items(), df["azerty"])))

# # Split the "key_value" column into two columns: "key" and "value"
# df_keys_values = df_keys_values.select("key_value.*")


# COMMAND ----------

# from pyspark.sql.functions import pandas_udf, PandasUDFType

# # Define a function that takes in a pandas DataFrame and returns a
# # new DataFrame with the keys and values from the "azerty" column
# @pandas_udf("key string, value string", PandasUDFType.GROUPED_MAP)
# def extract_keys_values(df):
#     return df["azerty"].apply(pd.Series).stack().reset_index(level=1, drop=True)

# # Use the "extract_keys_values" function to create a new DataFrame
# # with the keys and values from the "azerty" column
# df_keys_values = df.groupby().apply(extract_keys_values)


# COMMAND ----------

from pyspark.sql.types import StructType,StringType,IntegerType,MapType,ArrayType,StructField


schema = ArrayType(StructType([
        StructField('id', IntegerType(), nullable=False), 
        StructField('name', StringType(), nullable=False)]))

convertUDF = udf(lambda s: ','.join(map(str, s)),StringType())

df=df_keywords.withColumn("name",convertUDF(from_json(df_keywords.keywords,schema).getField("name"))).withColumn("id_name",convertUDF(from_json(df_keywords.keywords,schema).getField("id")))

df.select("name","id_name").show(10,False)

# COMMAND ----------

df.printSchema()
df.show()

# COMMAND ----------

df = df.drop(col("keywords"))
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("name", split(df["name"], ","))

# COMMAND ----------

# add a new column to count the number of words
df = df.withColumn("word_count", size(df.name))
df.show()
# filter the rows that contain the most words
# df = df.filter(df.word_count == df.select(max(df.word_count)).first()[0])

# group the rows by the number of words
# grouped_df = df.groupBy(df.word_count).count()

# convert the PySpark DataFrame to a pandas DataFrame
# pandas_df = grouped_df.toPandas()

# create the graph using the pandas plot method
# pandas_df.plot(kind='bar', x='word_count', y='count', rot=0)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# import pyspark
# split_col = pyspark.sql.functions.split(df['name'], ',')
# df = df.withColumn('NAME1', split_col.getItem(0))
# df = df.withColumn('NAME2', split_col.getItem(1))

# COMMAND ----------

# df.show()

# COMMAND ----------

# from pyspark.sql.functions import split

# df = df.withColumn("name", split("name", ",")).withColumn("id_name", split("id_name", ","))
# df.show()

# COMMAND ----------

# df_keywordsCOLLECT = df_keywords.collect()

# COMMAND ----------

# # looping thorough each row of the dataframe
# for row in df_keywordsCOLLECT:
#     # while looping through each
#     # row printing the data of Id, Name and City
#     print(row["keywords"])

# COMMAND ----------

df_credits = read_movies_data("credits")
display(df_credits)

# COMMAND ----------

df_links_small = read_movies_data("links_small")
display(df_links_small)

# COMMAND ----------

df_ratings_small = read_movies_data("ratings_small")
display(df_ratings_small)

# COMMAND ----------

df_movies_metadata = read_movies_data("movies_metadata")
display(df_movies_metadata)

# COMMAND ----------



# COMMAND ----------


