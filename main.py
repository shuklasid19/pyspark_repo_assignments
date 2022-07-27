from pyspark.sql import *
from pyspark import *
from utils import change_column, read_transac, spark_driver , read_user, combined_csv, unique_location, product_expenses , product_bought


#import os
#location = r'C:\Users\sid\Downloads\diggibyte\pyspark_assignment\pyspark123\spark_1/'

#calling the driver
spark = spark_driver()
print(spark)

#reading and showing the csv file transactions.csv
transc = read_transac(spark)
transc.show()

#reading and showing the csv
user = read_user(spark)
user.show()


#new variable for saving changed column name 
new_transac = change_column(transc, 'userid', 'user_id')
new_transac.show()


#combined the old and new csv file 
combined_df = combined_csv(spark, new_transac, user)
combined_df.show()

#
unique_loc = unique_location(combined_df)
unique_loc.show()

product_boght = product_bought(combined_df)
product_boght.show()


product_exp = product_expenses(combined_df)
product_exp.show()





