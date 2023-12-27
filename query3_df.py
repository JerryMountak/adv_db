from dataframe import create_dataset, create_householdIncome_dataset, create_revgecoding_dataset
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, count, udf,year

spark = SparkSession \
    .builder \
    .appName("Query 3 DataFrame Implementation") \
    .getOrCreate()

# Match victim descent code to detailed description
def victim_descent(descent_code):
    descent_map = {'A': 'Other Asian', 'B': 'Black', 'C': 'Chinese',
                   'D': 'Cambodian', 'F': 'Filipino', 'G': 'Guamanian',
                   'H': 'Hispanic/Latin/Mexican', 'I': 'American Indian/Alaskan Native',
                   'J': 'Japanese', 'K': 'Korean', 'L': 'Laotian',
                   'O': 'Other', 'P': 'Pacific Islander', 'S': 'Samoan',
                   'U': 'Hawaiian', 'V': 'Vietnamese', 'W': 'White', 
                   'X': 'Unkown', 'Z': 'Asian indian'}

    return descent_map[descent_code]

# Register the UDF
victim_descent_udf = udf(victim_descent, StringType())

# Record the start time
start_time = time.time()

# Dataset loading
income_df = create_householdIncome_dataset()
revgecoding_df = create_revgecoding_dataset()
crimes_df = create_dataset()

# Determine 3 highest and 3 lowest income areas
highest_income_codes = income_df.filter(col("community").like("%Los Angeles%")) \
                                .orderBy(col('Estimated Median Income').desc()) \
                                .limit(3) \
                                .select('Zip Code')

lowest_income_codes = income_df.filter(col("community").like("%Los Angeles%")) \
                                .orderBy(col('Estimated Median Income').asc()) \
                                .limit(3) \
                                .select('Zip Code')

# Filter crime dataset to keep only crimes commited on 2015 and have valid victim descriptions
crimes_df = crimes_df.withColumn('year', year(col('DATE OCC'))) \
                     .filter((col('year') == 2015) & (col('Vict Descent') != '')) \
                     .withColumn('Vict Descent', victim_descent_udf(col('Vict Descent')))

# Join crimes and gecoding datasets on coordinates columns
joined_df = crimes_df.join(revgecoding_df, on = ["LAT","LON"])

# Highest Income Areas Calculation
highest_join_df = joined_df.join(highest_income_codes, on = "Zip Code")

highest_result_df = highest_join_df.groupby('Vict Descent').agg(count("*").alias('#')) \
                                   .orderBy(col('#').desc()) \
                                   .select('Vict Descent','#')

# Lowest Income Areas Calculation
lowest_join_df = joined_df.join(lowest_income_codes, on = "Zip Code")

lowest_result_df = lowest_join_df.groupby('Vict Descent').agg(count("*").alias('#')) \
                                   .orderBy(col('#').desc()) \
                                   .select('Vict Descent','#')


highest_result_df.show(truncate=False)
lowest_result_df.show(truncate=False)

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")