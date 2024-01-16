# Advanced Databases Project

In this project we create a cluster of two VMs setting up Hadoop dfs and Apache Spark environment.
In this cluster we will store in a distributed way a dataset about criminal activity in Los Angeles executing queries in order to extract information.

# Frameworks
* Java
* Hadoop DFS
* YARN Resource Manager
* Spark
* Python/Pyspark

# Starting up the Services
Before executing queries we have to activate hdfs, YARN and Spark History Server using the following commands

  ```
  start-all.sh
  $SPARK_HOME/sbin/start-history-server.sh
```

# About the Queries
For each of the requested queries we present the Dataframe implementation in the `queryX_df.py` file and the SQL implementation in `queryX_sql.py`

# Running Queries
In order to run the queries we use the dataframe.py file which contains utility methods to create and format the datasets in a desired way
We execute a query using the following command:
```
spark-submit --num-executors x --py-files dataframe.py query_name.py
```

# About Us
* Gerasimos Mountakis
* Petros Maratos
