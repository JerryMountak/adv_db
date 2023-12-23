from dataframe import create_dataset
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, row_number
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Query 1 Implementation") \
    .getOrCreate()

# Create dataset and extract month and year from DATE OCC field (DATE OCC is the date when each crime occured)
crimes_df = create_dataset()
crimes_df = crimes_df.withColumn('month', month(col('DATE OCC'))) \
                     .withColumn('year', year(col('DATE OCC')))

# Record the start time
start_time = time.time()

# Group crimes by year and month with count as an aggregation method
grouped_df = crimes_df.groupby('year', 'month').agg(count("*").alias("crime_total"))

# Create a window specification to partition by year and order by crime_total descending
window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

# Add a rank column within each year based on crime_total and keep top three ranked months
grouped_df = grouped_df.withColumn("#", row_number().over(window_spec))
filtered_df = grouped_df.filter(col("#") <= 3)

# Order the dataframe by year ascending and crime_total descending
result_df = filtered_df.orderBy(col("year").asc(), col("crime_total").desc())
result_df.show(50,truncate=False)

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")