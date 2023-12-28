from dataframe import create_dataset, create_police_stations_dataset

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Query 4 SQL Implementation") \
    .getOrCreate()

# Assuming 'create_dataset' and 'create_police_stations_dataset' return DataFrames
crimes_df = create_dataset()
police_df = create_police_stations_dataset()

# Register DataFrames as temporary SQL tables
crimes_df.createOrReplaceTempView("crimes_table")
police_df.createOrReplaceTempView("police_table")


# SQL query to remove records where coordinates point to Null Island or weapon is not a firearm
filtered_crimes_query = """
    SELECT *
    FROM crimes_table
    WHERE (LAT != 0.0 AND LON != 0.0) AND `Weapon Used Cd` LIKE '1%'
"""

filtered_crimes_df = spark.sql(filtered_crimes_query)
filtered_crimes_df.registerTempTable("filtered_crimes_table")

# SQL query to join crime dataset with police stations dataset on AREA == PREC
# and calculate distance of crime scene from the police station which took the case
joined_query = """
    SELECT c.*,
           p.*,
           6371 * ACOS(SIN(RADIANS(c.LAT)) * SIN(RADIANS(p.Y)) +
                COS(RADIANS(c.LAT)) * COS(RADIANS(p.Y)) *
                COS(RADIANS(c.LON - p.X))) AS distance
    FROM filtered_crimes_table c
    JOIN police_table p ON c.AREA = p.PREC
"""


joined_df = spark.sql(joined_query)
joined_df.registerTempTable("joined_table")

# SQL query to calculate average distance and total crimes per year
crimes_per_year_query = """
    SELECT YEAR(`DATE OCC`) AS year,
           ROUND(AVG(distance), 3) AS average_distance,
           COUNT(*) AS `#`
    FROM joined_table
    GROUP BY year
    ORDER BY year ASC
"""

crimes_per_year_df = spark.sql(crimes_per_year_query)


# SQL query to calculate average distance and total crimes per division
crimes_per_pd_query = """
    SELECT DIVISION,
           ROUND(AVG(distance), 3) AS average_distance,
           COUNT(*) AS `#`
    FROM joined_table
    GROUP BY DIVISION
    ORDER BY `#` DESC
"""

crimes_per_pd_df = spark.sql(crimes_per_pd_query)

print('Results for crimes per year')
crimes_per_year_df.show(truncate=False)

print('Results for crimes per division')
crimes_per_pd_df.show(21,truncate=False)






# ========== PART 2 ==========

# calculate the distance using SQL functions
distance_query = """
    SELECT
        c.*,
        p.*,
        -- Calculate distance using SQL functions
        6371 * ACOS(SIN(RADIANS(c.LAT)) * SIN(RADIANS(p.Y)) +
                    COS(RADIANS(c.LAT)) * COS(RADIANS(p.Y)) *
                    COS(RADIANS(c.LON - p.X))) AS distance
    FROM filtered_crimes_table c
    JOIN police_table p
"""


# Execute the distance calculation SQL query
joined2_df = spark.sql(distance_query)
joined2_df.registerTempTable("joined2_table")

# Add a column with the minimum distance for each crime
min_distance_query = """
    SELECT *,
           MIN(distance) OVER (PARTITION BY DR_NO) AS min_distance
    FROM joined2_table
"""

joined2_df = spark.sql(min_distance_query)
joined2_df.registerTempTable("min_distance_table")

# Filter the dataset keeping only rows where the distance from the PD is minimum
filtered_joined2_query = """
    SELECT *
    FROM min_distance_table
    WHERE distance = min_distance
"""


filtered_joined2_df = spark.sql(filtered_joined2_query)
filtered_joined2_df.registerTempTable("filtered_joined2_table")


# SQL query to calculate average distance and total crimes per year
crimes_per_year2_query = """
    SELECT YEAR(`DATE OCC`) AS year,
           ROUND(AVG(distance), 3) AS average_distance,
           COUNT(*) AS `#`
    FROM filtered_joined2_table
    GROUP BY year
    ORDER BY year ASC
"""

# SQL query to calculate average distance and total crimes per division
crimes_per_pd2_query = """
    SELECT DIVISION,
           ROUND(AVG(distance), 3) AS average_distance,
           COUNT(*) AS `#`
    FROM filtered_joined2_table
    GROUP BY DIVISION
    ORDER BY `#` DESC
"""


crimes_per_year2_df = spark.sql(crimes_per_year2_query)
crimes_per_pd2_df = spark.sql(crimes_per_pd2_query)

print('Results for crimes per year')
crimes_per_year2_df.show(truncate=False)

print('Results for crimes per division')
crimes_per_pd2_df.show(21,truncate=False)