# Databricks notebook source
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import sum, count, avg, min, max, mean, col, lit

# Create SparkConf object with application name
conf = SparkConf().setAppName("mysparkprojects")

# Create a SparkContext object using the configuration
sc = SparkContext.getOrCreate(conf=conf)

# Read a CSV file into sparkdf
df = spark.read.csv("/FileStore/tables/OfficeData.csv", header=True, inferSchema=True)
df.show()
df.printSchema()

# Total number of employees
tot_emp = df.agg(count('employee_name'))
tot_emp.show()

# Total no of departments
tot_depts = df.select('department').distinct().count()
tot_depts

# Distinct departments
df.select('department').distinct().show()

# Num of employees in each department
df.groupBy('department').count().show()

# No of employees in each state
df.groupBy('state').count().show()

# Min salary by dept
df.groupBy('department').agg(min('salary')).show()

# Salary by dept - sorted desc
df.groupBy('department').agg(min('salary').alias('max_salary')).orderBy('max_salary', ascending =False).show()

# Name of finance employee in NY whose bonus > average NY employees' bonus
df0 = df.filter(df.state == 'NY').filter(df.department == 'Finance')
df0.show()


df1 = df.filter(df.state == 'NY').agg((mean('bonus')))
df1.show()


df0.select('employee_name').filter(df0.bonus > 17000).show()

# Give a $500 raise to sales employees in CA and a $300 raise those in NY finance 
def get_raise(state, department, salary):
    if state == 'CA' and department == 'Sales':
        return salary + 500
    elif state == 'NY' and department == 'Finance':
        return salary + 300
    

salaryudf = udf(lambda x, y, z: get_raise(x, y, z), IntegerType())

df.withColumn('raise', salaryudf(df.state, df.department, df.salary)).show()
