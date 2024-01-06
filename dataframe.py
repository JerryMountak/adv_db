from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.functions import to_date, col, to_timestamp, regexp_replace

def create_dataset():
    spark = SparkSession \
        .builder \
        .appName("Dataframe creation") \
        .getOrCreate()

    crimes_schema = StructType([
        StructField("DR_NO", StringType()),
        StructField("Date Rptd", StringType()),
        StructField("DATE OCC", StringType()),
        StructField("TIME OCC", StringType()),
        StructField("AREA", IntegerType()),
        StructField("AREA NAME", StringType()),
        StructField("Rpd Dist No", StringType()),
        StructField("Part 1-2", IntegerType()),
        StructField("Crm Cd", IntegerType()),
        StructField("Crm Cd Desc", StringType()),
        StructField("Mocodes", StringType()),
        StructField("Vict Age", IntegerType()),
        StructField("Vict Sex", StringType()),
        StructField("Vict Descent", StringType()),
        StructField("Premis Cd", IntegerType()),
        StructField("Premis Desc", StringType()),
        StructField("Weapon Used Cd", StringType()),
        StructField("Weapon Desc", StringType()),
        StructField("Status", StringType()),
        StructField("Status Desc", StringType()),
        StructField("Crm Cd 1", IntegerType()),
        StructField("Crm Cd 2", IntegerType()),
        StructField("Crm Cd 3", IntegerType()),
        StructField("Crm Cd 4", IntegerType()),
        StructField("LOCATION", StringType()),
        StructField("Cross Street", StringType()),
        StructField("LAT", DoubleType()),
        StructField("LON", DoubleType()),
    ])

    crimes10_19_df = spark.read.csv("hdfs://master:54310/dataset/crimes10_19.csv", header=True, schema=crimes_schema)
    crimes20_pr_df = spark.read.csv("hdfs://master:54310/dataset/crimes20_present.csv", header=True, schema=crimes_schema)
    crimes_df = crimes10_19_df.union(crimes20_pr_df)
    crimes_df = crimes_df.withColumn("Date Rptd", to_date(to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))) \
                         .withColumn("DATE OCC", to_date(to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")))


    return crimes_df


def create_householdIncome_dataset():
    
    spark = SparkSession \
        .builder \
        .appName("Income Dataframe creation") \
        .getOrCreate()
    
    income_schema = StructType([
        StructField("Zip Code", StringType()),
        StructField("Community", StringType()),
        StructField("Estimated Median Income", StringType()),       
    ])

    income_df = spark.read.csv("hdfs://master:54310/dataset/LA_income_2015.csv", header=True, schema=income_schema)
    income_df = income_df.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[^0-9]", "").cast("int"))
    return income_df


def create_revgecoding_dataset():
    spark = SparkSession \
        .builder \
        .appName("Reverse GeCoding Dataframe creation") \
        .getOrCreate()

    revgecoding_schema = StructType([
        StructField("LAT", DoubleType()),
        StructField("LON", DoubleType()),
        StructField("Zip Code", StringType())
    ])

    revgecoding_df = spark.read.csv("hdfs://master:54310/dataset/revgecoding.csv", header=True, schema=revgecoding_schema)
    return revgecoding_df

def create_police_stations_dataset():
    spark = SparkSession \
        .builder \
        .appName("Police Stations Dataframe creation") \
        .getOrCreate()

    stations_schema = StructType([
        StructField("X", DoubleType()),
        StructField("Y", DoubleType()),
        StructField("FID", IntegerType()),
        StructField("DIVISION", StringType()),
        StructField("LOCATION", StringType()),
        StructField("PREC", IntegerType()),
    ])

    stations_df = spark.read.csv("hdfs://master:54310/dataset/police_stations.csv", header=True, schema=stations_schema)
    return stations_df

def main():
    crimes_df = create_dataset()
    crimes_df.printSchema()
    crimes_df.show(5)


    print(f"The number of rows of the dataframe is {crimes_df.count()}")
    print(f"The number of columns of the dataframe is {len(crimes_df.columns)}")

    
    for c in crimes_df.dtypes:
        print(c)


if __name__ == "__main__":
    main()