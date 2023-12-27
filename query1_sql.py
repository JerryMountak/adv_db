from dataframe import create_dataset
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col

spark = SparkSession \
    .builder \
    .appName("Query 1 Implementation") \
    .getOrCreate()

# Record the start time
start_time = time.time()

# Create dataset and extract month and year from DATE OCC field (DATE OCC is the date when each crime occured)
crimes_df = create_dataset()
crimes_df = crimes_df.withColumn('month', month(col('DATE OCC'))) \
                     .withColumn('year', year(col('DATE OCC')))
crimes_df.createOrReplaceTempView("crimes_table")

# Group crimes by year and month with count as an aggregation method
crime_total_query = "SELECT crimes_table.year as year, crimes_table.month as month, COUNT(*) as crime_total \
            FROM crimes_table \
            GROUP BY year, month"

# Execute the query and register the results 
crime_total_result = spark.sql(crime_total_query)
crime_total_result.registerTempTable("grouped_table")

# Create a partitioning by year and order by crime_total descending and add a rank column within each year based on crime_total
partition_query = "SELECT year, month, crime_total, ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) as rank \
            FROM grouped_table"

# Execute the query and register the results
partition_query_result = spark.sql(partition_query)
partition_query_result.registerTempTable("partition_table")

# Order the dataframe by year ascending and crime_total descending after keeping top 3 months of each year
final_query = "SELECT year, month, crime_total, rank \
            FROM partition_table \
            WHERE rank <= 3 \
            ORDER BY year ASC, crime_total DESC"

final_query_result = spark.sql(final_query)
final_query_result.show(50,truncate=False)

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")