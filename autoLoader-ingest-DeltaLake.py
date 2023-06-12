# import functions
from pyspark.sql.functions import input_file_name, current_timestamp
# Default variable used in code below
file_path="/databricks-datasets/structured-streaming/events"
username=spark.sql("SELECT regexp_replace(current_user(),'[^a-zA-Z0-9])','_')").first()[0]
table_name=f"{username}_etl_quickstart"
checkpoint_path=f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbtils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.schemaLocation",checkpoint_path)
    .load(file_path)
    .select("*", input_file_name().alias("source_file"),current_timestamp().alias("processing_time"))
    .writeStream
    .option('checkpointLocation',checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name)
 
 )


# To display the table
display(df)

#Create a new visualization
sparkDF=spark.read.csv("/databricks-datasets/bikeSharing/data-001/day.csv",header="true", inferSchema="true")
display(sparkDF)


# Create DataFrame with PySpark
import pandas as pd
data=[ [1,"Elia"],[2,"Teo"],[3,"Fang"]]

pdf=pd.DataFrame(data,columns=["id","name"])


df1=spark.createDataFrame(pdf)
df2=spark.createDataFrame(data,schema="id LONG,name STRING")
display(df1)
# Read a table into a DataFrame
df3=spark.read.table=("<catalog-name>.<schema_name>.<table-name>")
display(df3)

# Load data into dataFrame from file

df = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")
)
display(df4)

# Transformation of DataFrame using Spark, the below is inner join by DEFAULT
joined_df=df1.join(df2,how="inner",on="id")

union_df=df1.union(df2)

# Filter Rows for DataFrame
filtered_df=df1.where("id > 1")
filtered_df=df1.filter("id > 1")


# Select columns from a DataFrame
select_df=df1.select( "name")
display(select_df)


subset_df = df1.filter("id > 1").select("name")
display(subset_df)


# Save a DataFrame to table
df1.write.saveAsTable("<table-name")
df1.write.format("json").save("/tmp/json_data")



