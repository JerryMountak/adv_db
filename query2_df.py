from dataframe import create_dataset
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, count, udf

spark = SparkSession \
    .builder \
    .appName("Query 2 DataFrame Implementation") \
    .getOrCreate()

def determine_part_of_day(time_occ):
    time_occ = int(time_occ)

    if 500 <= time_occ <= 1159:
        return 'Morning'
    elif 1200 <= time_occ <= 1659:
        return 'Afternoon'
    elif 1700 <= time_occ <= 2059:
        return 'Evening'
    else:
        # night ends at 0400 ??
        return 'Night'
    
# Register the UDF
determine_part_of_day_udf = udf(determine_part_of_day, StringType())

# Record the start time
start_time = time.time()

crimes_df = create_dataset()

# The implementation of the query consists of the following steps:
# Filter the original dataset keeping only crimes that took place in STREET 
# Use udf method to determine the time of day each crime happened and create part of day column
# Group dataset records by part of day measuring the crime number for each time of day
# Order the dataset based on crime count in descending order
# Finally select the columns part of day and crime total for the result
result_df = crimes_df.filter(col('Premis Desc') == 'STREET') \
                     .withColumn('part of day', determine_part_of_day_udf(col('TIME OCC'))) \
                     .groupby('part of day').agg(count("*").alias('crime_total')) \
                     .orderBy(col('crime_total').desc()) \
                     .select('part of day','crime_total')

result_df.show()

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")