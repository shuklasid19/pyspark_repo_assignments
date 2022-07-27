
from pyspark.sql import *


import os
location = r'C:\Users\sid\Downloads\diggibyte\pyspark_assignment\pyspark123\spark_1\comvine\data/'

def spark_driver():
    """it will start the session of our application
    """
    spark = SparkSession.builder.master('local[2]').appName('assignment').getOrCreate()
    return spark



def read_transac(spark):
    """it will read the dataset csv file transaction.csv
    """
    return spark.read.option('inferSchema', 'True').option('header', 'True').csv(location + 'transaction.csv')


def read_user(spark):
    """it will read the dataset csv file user.csv
    """
    return spark.read.option('inferSchema', 'True').option('header', 'True').csv(location + 'user.csv')



def change_column(data_file, col, columns):
    """ it will change the column name from userid to user_id in transac dataset
    """
    return data_file.withColumnRenamed(col, columns)


def combined_csv(spark, new_transac, user):
    """it will combine 2 data sets csv 
    """
    return new_transac.join(user, on=['user_id'], how='inner')


#count of unique location where each product is sold
def unique_location(combined_df):
    """it will do groupby operation and then count them and show it in terminal
    """
    return combined_df.groupBy('location ', 'product_description').count()


#find out products bought by each user.
def product_bought(combined_df):
    "it will do groupby operation and then show the product bought by each user"
    return combined_df.groupBy('user_id' ,'product_description').count()

#Total spending done by each user on each product. 
def product_expenses(combined_df):
    """it will do groupby operation and the total sum of product of each user"
    """
    return combined_df.groupBy('user_id', 'product_description').sum('price')