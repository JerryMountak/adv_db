from dataframe import create_dataset, create_householdIncome_dataset, create_revgecoding_dataset
import time

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Query 3 SQL Implementation") \
    .getOrCreate()

# Record the start time
start_time = time.time()


income_df = create_householdIncome_dataset()
revgecoding_df = create_revgecoding_dataset()
crimes_df = create_dataset()

# Register DataFrames as temporary SQL tables
income_df.createOrReplaceTempView("income_table")
revgecoding_df.createOrReplaceTempView("revgecoding_table")
crimes_df.createOrReplaceTempView("crimes_table")

# SQL query to determine 3 highest income areas
highest_income_query = """
    SELECT
        `Zip Code`
    FROM income_table
    WHERE community LIKE '%Los Angeles%'
    ORDER BY `Estimated Median Income` DESC
    LIMIT 3
"""

# SQL query to determine 3 lowest income areas
lowest_income_query = """
    SELECT
        `Zip Code`
    FROM income_table
    WHERE community LIKE '%Los Angeles%'
    ORDER BY `Estimated Median Income` ASC
    LIMIT 3
"""

# SQL query to filter crime dataset
filtered_crimes_query = """
    SELECT
        *,
        YEAR(`DATE OCC`) AS year
    FROM crimes_table
    WHERE YEAR(`DATE OCC`) = 2015
      AND `Vict Descent` != ''
"""

highest_income_zipcode = spark.sql(highest_income_query)
lowest_income_zipcode = spark.sql(lowest_income_query)
filtered_crimes = spark.sql(filtered_crimes_query)

highest_income_zipcode.registerTempTable("highest_zipcode_table")
lowest_income_zipcode.registerTempTable("lowest_zipcode_table")
filtered_crimes.registerTempTable("filtered_crimes_table")

# SQL query to join crimes and geocoding datasets on coordinates columns
joined_query = """
    SELECT *
    FROM filtered_crimes_table c
    JOIN revgecoding_table r ON c.LAT = r.LAT AND c.LON = r.LON
"""
joined_crimes = spark.sql(joined_query)
joined_crimes.registerTempTable("joined_crimes_table")


# SQL query for Highest Income Areas Result Calculation
highest_result_query = """
    SELECT
        c.`Vict Descent`,
        COUNT(*) AS `#`
    FROM joined_crimes_table c
    JOIN highest_zipcode_table h ON c.`Zip Code` = h.`Zip Code`
    GROUP BY c.`Vict Descent`
    ORDER BY `#` DESC
"""

# SQL query for Lowest Income Areas Result Calculation
lowest_result_query = """
    SELECT
        c.`Vict Descent`,
        COUNT(*) AS `#`
    FROM joined_crimes_table c
    JOIN lowest_zipcode_table l ON c.`Zip Code` = l.`Zip Code`
    GROUP BY c.`Vict Descent`
    ORDER BY `#` DESC
"""


highest_result_df = spark.sql(highest_result_query)
lowest_result_df = spark.sql(lowest_result_query)

print('Results for the three highest median income areas')
highest_result_df.show()

print('Results for the three lowest median income areas')
lowest_result_df.show()

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")