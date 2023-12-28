from dataframe import create_dataset
import time

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Query 2 SQL Implementation") \
    .getOrCreate()

# Record the start time
start_time = time.time()

crimes_df = create_dataset()

# Register the DataFrame as a temporary SQL table
crimes_df.createOrReplaceTempView("crimes_table")

# SQL query 
query = """
    SELECT
        CASE
            WHEN CAST(`TIME OCC` AS INT) BETWEEN 500 AND 1159 THEN 'Morning'
            WHEN CAST(`TIME OCC` AS INT) BETWEEN 1200 AND 1659 THEN 'Afternoon'
            WHEN CAST(`TIME OCC` AS INT) BETWEEN 1700 AND 2059 THEN 'Evening'
            ELSE 'Night'
        END AS part_of_day,
        COUNT(*) AS crime_total
    FROM crimes_table
    WHERE `Premis Desc` = 'STREET'
    GROUP BY part_of_day
    ORDER BY crime_total DESC
"""

result_df = spark.sql(query)

result_df.show()

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")