from pyspark.sql import SparkSession
from csv import reader
from io import StringIO
import time

spark = SparkSession \
    .builder \
    .appName("Query 2 RDD Implementation") \
    .getOrCreate() \
    .sparkContext

def determine_part_of_day(row):
    time_occ = int(row[3])

    if 500 <= time_occ <= 1159:
        return ('Morning', 1)
    elif 1200 <= time_occ <= 1659:
        return ('Afternoon', 1)
    elif 1700 <= time_occ <= 2059:
        return ('Evening', 1)
    else:
        # night ends at 0400 ??
        return ('Night', 1)


# Function to parse a CSV line
def parse_csv_line(line):
    csv_reader = reader(StringIO(line))
    return next(csv_reader)

# Record the start time
start_time = time.time()


crime1 = spark.textFile("hdfs://okeanos-master:54310/dataset/crimes10_19.csv") \
              .map(lambda x: parse_csv_line(x))
header1 = crime1.first()
crime1 = crime1.filter(lambda row: row != header1)


crime2 = spark.textFile("hdfs://okeanos-master:54310/dataset/crimes20_present.csv") \
              .map(lambda x: parse_csv_line(x))
header2 = crime2.first()
crime2 = crime2.filter(lambda row: row != header2)

crimes = crime1.union(crime2)

# Filter the original dataset keeping only crimes that took place in STREET 
crimes_street = crimes.map(lambda x: x if (x[15] == 'STREET') else None) \
                      .filter(lambda x: x != None)

# Count dataset records by measuring the rows containing each part of day
crimes_street = crimes_street.map(lambda x: determine_part_of_day(x)) \
                             .reduceByKey(lambda x,y: x+y)

# Cache the reduction result which will be used for sorting
crimes_street.persist()

# Order the dataset in descending order
sorted_rdd = crimes_street.sortBy(lambda x: x[1],ascending=False)
print(sorted_rdd.collect())

crimes_street.unpersist()

# Record the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time:.3f} seconds\n")
