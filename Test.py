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
    
display(read_movies_data("movies_metadata"))
