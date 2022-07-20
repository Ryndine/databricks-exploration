# Databricks notebook source
from pyspark.sql import *

# Departments dataframe
department1 = Row(id="123456", name="Computer Science")
department2 = Row(id="789012", name="Mechanical Engineering")
department3 = Row(id="345678", name="Theater and Drama")
department4 = Row(id="901234", name="Indoor Recreation")

# Employees dateframe
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
employee2 = Employee("mayu", "asahina", "no-reply@standford.edu", 120000)
employee3 = Employee("ryan", "mangeno", "no-reply@waterloo.edu", 140000)
employee4 = Employee(None, "cecil", "no-reply@berkeley.edu", 160000)
employee5 = Employee("zach", "howe", "no-reply@neverla.nd", 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

# COMMAND ----------

# Datframes from lists of rows

departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)

df1.show(truncate=False)

departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)

df2.show(truncate=False)

# COMMAND ----------

# Union dataframes

unionDF = df1.union(df2)
unionDF.show(truncate=False)

# COMMAND ----------

# Write dataframe to Parquet file

# Remove the file if it exists
dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)
unionDF.write.format("parquet").save("/tmp/databricks-df-example.parquet")

# COMMAND ----------

# Read parquet files

parquetDF = spark.read.format("parquet").load("/tmp/databricks-df-example.parquet")
parquetDF.show(truncate=False)

# COMMAND ----------

# Exploding first and last name

from pyspark.sql.functions import explode

explodeDF = unionDF.select(explode("employees").alias("e"))
flattenDF = explodeDF.selectExpr("e.firstName", "e.lastName", "e.email", "e.salary")

flattenDF.show(truncate=False)

# COMMAND ----------

# Filtering

filterDF = flattenDF.filter(flattenDF.firstName == "mayu").sort(flattenDF.lastName)
filterDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, asc

# Use `|` instead of `or`
filterDF = flattenDF.filter((col("firstName") == "mayu") | (col("firstName") == "ryan")).sort(asc("lastName"))
filterDF.show(truncate=False)

# COMMAND ----------

# Where and filter

whereDF = flattenDF.where((col("firstName") == "mayu") | (col("firstName") == "ryan")).sort(asc("lastName"))
whereDF.show(truncate=False)

# COMMAND ----------

nonNullDF = flattenDF.fillna("--")
nonNullDF.show(truncate=False)

# COMMAND ----------

filterNonNullDF = flattenDF.filter(col("firstName").isNull() | col("lastName").isNull()).sort("email")
filterNonNullDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

countDistinctDF = nonNullDF.select("firstName", "lastName") \
  .groupBy("firstName") \
  .agg(countDistinct("lastName").alias("distinct_last_names"))

countDistinctDF.show()

# COMMAND ----------

countDistinctDF.explain()

# COMMAND ----------

# Register the DataFrame as a temporary view so that we can query it by using SQL.
nonNullDF.createOrReplaceTempView("databricks_df_example")

# Perform the same query as the preceding DataFrame and then display its physical plan.
countDistinctDF_sql = spark.sql('''
  SELECT firstName, count(distinct lastName) AS distinct_last_names
  FROM databricks_df_example
  GROUP BY firstName
''')

countDistinctDF_sql.explain()

# COMMAND ----------

salarySumDF = nonNullDF.agg({"salary" : "sum"})
salarySumDF.show()

# COMMAND ----------

match = 'salary'

for key, value in nonNullDF.dtypes:
  if key == match:
    print(f"Data type of '{match}' is '{value}'.")

# COMMAND ----------

nonNullDF.describe("salary").show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
plt.clf()
pdDF = nonNullDF.toPandas()
pdDF.plot(x='firstName', y='salary', kind='bar', rot=45)
display()

# COMMAND ----------

dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Build an example DataFrame dataset to work with.
dbutils.fs.rm("/tmp/dataframe_sample.csv", True)
dbutils.fs.put("/tmp/dataframe_sample.csv", """id|end_date|start_date|location
1|2015-10-14 00:00:00|2015-09-14 00:00:00|CA-SF
2|2015-10-15 01:00:20|2015-08-14 00:00:00|CA-SD
3|2015-10-16 02:30:00|2015-01-14 00:00:00|NY-NY
4|2015-10-17 03:00:20|2015-02-14 00:00:00|NY-NY
5|2015-10-18 04:30:00|2014-04-14 00:00:00|CA-SD
""", True)

df = spark.read.format("csv").options(header='true', delimiter = '|').load("/tmp/dataframe_sample.csv")
df.printSchema()

# COMMAND ----------

# Instead of registering a UDF, call the builtin functions to perform operations on the columns.
# This will provide a performance improvement as the builtins compile and run in the platform's JVM.

# Convert to a Date type
df = df.withColumn('date', F.to_date(df.end_date))

# Parse out the date only
df = df.withColumn('date_only', F.regexp_replace(df.end_date,' (\d+)[:](\d+)[:](\d+).*$', ''))

# Split a string and index a field
df = df.withColumn('city', F.split(df.location, '-')[1])

# Perform a date diff function
df = df.withColumn('date_diff', F.datediff(F.to_date(df.end_date), F.to_date(df.start_date)))

# COMMAND ----------

df.createOrReplaceTempView("sample_df")
display(sql("select * from sample_df"))

# COMMAND ----------

from pyspark.sql import functions as F

add_n = udf(lambda x, y: x + y, IntegerType())

# We register a UDF that adds a column to the DataFrame, and we cast the id column to an Integer type.
df = df.withColumn('id_offset', add_n(F.lit(1000), df.id.cast(IntegerType())))

# COMMAND ----------

df = df.withColumn('test_col', add_n(F.lit(100), df.id.cast(IntegerType())))

# COMMAND ----------

display(df)

# COMMAND ----------

# any constants used by UDF will automatically pass through to workers
N = 90
last_n_days = udf(lambda x: x < N, BooleanType())

df_filtered = df.filter(last_n_days(df.date_diff))
display(df_filtered)

# COMMAND ----------

# Both return DataFrame types
df_1 = table("sample_df")
df_2 = spark.sql("select * from sample_df")

# COMMAND ----------

# Provide the min, count, and avg and groupBy the location column. Diplay the results
agg_df = df.groupBy("location").agg(F.min("id"), F.count("id"), F.avg("date_diff"))
display(agg_df)

# COMMAND ----------

df = df.withColumn('end_month', F.month('end_date'))
df = df.withColumn('end_year', F.year('end_date'))
df.write.partitionBy("end_year", "end_month").format("parquet").load("/tmp/sample_table")
display(dbutils.fs.ls("/tmp/sample_table"))

# COMMAND ----------

null_item_schema = StructType([StructField("col1", StringType(), True),
                               StructField("col2", IntegerType(), True)])
null_df = spark.createDataFrame([("test", 1), (None, 2)], null_item_schema)
display(null_df.filter("col1 IS NOT NULL"))

# COMMAND ----------

adult_df = spark.read.\
    format("com.spark.csv").\
    option("header", "false").\
    option("inferSchema", "true").load("dbfs:/databricks-datasets/adult/adult.data")
adult_df.printSchema()

# COMMAND ----------


