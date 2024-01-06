from dataframe import create_dataset, create_police_stations_dataset
from math import radians, sin, cos, sqrt, atan2

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, count, udf, year, avg, round, min
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Query 4 DataFrame Implementation") \
    .getOrCreate()

# calculate the distance between two points [lat1, long1] , [lat2, long2] in km
def get_distance(lat1, long1, lat2, long2):
    
    # Radius of the Earth in kilometers
    radius = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(long1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(long2)

    # Calculate differences between latitudes and longitudes
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Calculate distance using Haversine formula
    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = radius * c
    return distance


# Register the UDF
get_distance_udf = udf(get_distance, DoubleType())


# Dataset creation
crimes_df = create_dataset()
police_df = create_police_stations_dataset()

# Remove records where coordinates point to Null island or weapon is not firearm
crimes_df_firearms = crimes_df.filter(((col('LAT') != 0.0) & (col('LON') != 0.0)) & (col('Weapon Used Cd').like("1%")))

# Join crime dataset with police stations dataset on AREA == PREC
# Calculate distance of crime scene from the police station which took the case
joined_df = crimes_df_firearms.join(police_df, crimes_df_firearms['AREA'] == police_df['PREC']) \
                     .withColumn('distance', get_distance_udf(col('LAT'),col('LON'),col('Y'),col('X')))

# Group by year, find average distance and total crimes
# Order the result by year in ascending order
crimes_per_year_df = joined_df.withColumn('year', year(col('DATE OCC'))) \
                     .groupby('year').agg(avg('distance').alias("average_distance"),count("*").alias('#')) \
                     .withColumn('average_distance', round(col('average_distance'), 3)) \
                     .orderBy(col('year').asc())

print('Results for crimes per year')
crimes_per_year_df.show(50,truncate=False)


# Remove records where coordinates point to Null island or weapon code is empty
crimes_df_all = crimes_df.filter(((col('LAT') != 0.0) & (col('LON') != 0.0)) & (col('Weapon Used Cd') != ''))

# Join crime dataset with police stations dataset on AREA == PREC
# Calculate distance of crime scene from the police station which took the case
joined_df2 = crimes_df_all.join(police_df, crimes_df_all['AREA'] == police_df['PREC']) \
                     .withColumn('distance', get_distance_udf(col('LAT'),col('LON'),col('Y'),col('X')))

# Group by division, find average distance and total crimes
# Order the result by crime total in descending order
crimes_per_pd_df = joined_df2.groupby('DIVISION').agg(avg('distance').alias("average_distance"),count("*").alias('#')) \
                     .withColumn('average_distance', round(col('average_distance'), 3)) \
                     .orderBy(col('#').desc())

print('Results for crimes per division')
crimes_per_pd_df.show(50,truncate=False)


# Query4 part2

# Cartesian join of crime dataset (firearms only) with police stations dataset
# Calculate distance of crime scene from all the police stations
joined2_df = crimes_df_firearms.join(police_df) \
                      .withColumn('distance', get_distance_udf(col('LAT'),col('LON'),col('Y'),col('X')))


# Create a window specification to partition by crime record number
window_spec = Window.partitionBy('DR_NO')

# Add a column with the minimum distance for each crime
joined2_df = joined2_df.withColumn("min_distance", min('distance').over(window_spec))

# Filter the dataset keeping only rows where the distance from the pd is minimum
joined2_df = joined2_df.filter(col("distance") == col("min_distance")) \
                       .drop("min_distance")


# Group by year, find average distance and total crimes
# Order the result by year in ascending order
crimes_per_year2_df = joined2_df.withColumn('year', year(col('DATE OCC'))) \
                     .groupby('year').agg(avg('distance').alias("average_distance"),count("*").alias('#')) \
                     .withColumn('average_distance', round(col('average_distance'), 3)) \
                     .orderBy(col('year').asc())

print('Results for crimes per year')
crimes_per_year2_df.show(50,truncate=False)


# Cartesian join of crime dataset (any weapon) with police stations dataset
# Calculate distance of crime scene from all the police stations
joined2_df2 = crimes_df_all.join(police_df) \
                      .withColumn('distance', get_distance_udf(col('LAT'),col('LON'),col('Y'),col('X')))


# Create a window specification to partition by crime record number
window_spec = Window.partitionBy('DR_NO')

# Add a column with the minimum distance for each crime
joined2_df2 = joined2_df2.withColumn("min_distance", min('distance').over(window_spec))

# Filter the dataset keeping only rows where the distance from the pd is minimum
joined2_df2 = joined2_df2.filter(col("distance") == col("min_distance")) \
                       .drop("min_distance")

# Group by division, find average distance and total crimes
# Order the result by crime total in descending order
crimes_per_pd2_df = joined2_df2.groupby('DIVISION').agg(avg('distance').alias("average_distance"),count("*").alias('#')) \
                     .withColumn('average_distance', round(col('average_distance'), 3)) \
                     .orderBy(col('#').desc())

print('Results for crimes per division')
crimes_per_pd2_df.show(50,truncate=False)