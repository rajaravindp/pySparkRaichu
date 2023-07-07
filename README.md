# pySparkRaichu
PySpark Project: Demonstrating Data Analysis and Processing using Apache Spark DataFrames - Part II

The project leverages PySpark, a powerful distributed data processing framework, to efficiently process large-scale data. It provides code snippets and examples for performing common data manipulation and analysis tasks.
The purpose of the code is to demonstrate data manipulation and analysis using Spark DataFrames in PySpark. It performs various operations such as reading a CSV file, calculating counts and aggregations, filtering data, and applying user-defined functions.

## Advantages of using Spark DataFrames over RDDs (Resilient Distributed Datasets) include:
- Higher-level API: Spark DataFrames provide a higher-level, more expressive API compared to RDDs. DataFrames allow for more concise and readable code, thanks to built-in functions and optimized query execution.
- Schema enforcement: DataFrames have a schema, which means they provide a structured way to organize and work with data. This allows for better data quality control, as the schema enforces data types and helps catch errors early.
- Optimized execution: Spark DataFrames leverage Catalyst optimizer, which optimizes the execution plan for data processing operations. Catalyst optimizes the code by performing predicate pushdown, column pruning, and other optimizations to improve performance.
- Efficient memory management: DataFrames use a technique called "Tungsten" that improves memory management and serialization. It enables Spark to efficiently store data in memory using a binary format, resulting in faster processing speeds.
- Integration with SQL and other libraries: DataFrames seamlessly integrate with Spark SQL, enabling SQL queries to be executed directly on DataFrames. This allows users familiar with SQL to leverage their skills and easily transition to Spark. Spark DataFrames also integrate well with other Spark libraries, such as MLlib for machine learning and GraphX for graph processing. This allows for a unified and cohesive data processing pipeline.

## Key features of the code base:
- Data Reading: The code reads a CSV file named "OfficeData.csv" into a DataFrame using the spark.read.csv() function. It includes options such as header=True to indicate the presence of a header row and inferSchema=True to automatically infer the schema of the DataFrame.
- Data Manipulation: The code performs various data manipulation operations on the DataFrame. It includes operations like filtering, aggregation, grouping, and column selection using functions such as filter(), groupBy(), agg(), select(), and orderBy().
- User-Defined Function (UDF): The code defines a user-defined function (get_raise()) using the udf() function. This UDF is then applied to a column in the DataFrame using withColumn() to calculate the raise amount based on the employee's state, department, and salary.
- Spark SQL Functions: The code utilizes Spark SQL functions such as count(), avg(), min(), max(), mean(), and distinct() to perform various calculations and aggregations on the DataFrame.
- DataFrame Operations: The code demonstrates common DataFrame operations such as show() to display the contents of a DataFrame, printSchema() to print the inferred schema, and orderBy() to sort the data based on a column.

Feel free to explore the codebase, adapt it to your requirements, or use it as a reference to enhance your PySpark skills and tackle similar data analysis tasks.
