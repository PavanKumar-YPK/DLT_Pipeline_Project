from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from utilities import utils

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("title", StringType())
    ]
)

geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema)
    ]
)

schema = ArrayType(feature_schema)

#.option("cloudFiles.schemaLocation","/Volumes/dlt_pipeline_project/silver/checkpointLocation/")\
                
@dp.view(name="earthquake_data_vw")

def earthquake_data():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format","json")\
                .load("/Volumes/dlt_pipeline_project/bronze/earthquake_data/")\
                .withColumn("_load_timestamp", current_timestamp()) 

    df = df.withColumn("parsed_data", from_json(col('features'),schema))
    df = df.select(explode(col("parsed_data")).alias("features"),col("_load_timestamp"))
    df = df.select(col("features.properties.*"),col("features.id"),col("features.geometry.coordinates")[0].alias("latitude"),col("features.geometry.coordinates")[1].alias("longitude"),col("features.geometry.coordinates")[2].alias("depth"),col("_load_timestamp"))
    df = df.withColumn("time",from_unixtime(col("time")/1000).cast("timestamp"))\
            .withColumn("mag", col("mag").cast("double"))\
            .withColumn("nst", col("nst").cast("double"))\
            .withColumn("tsunami", col("tsunami").cast("double"))\
            .withColumn("sig", col("sig").cast("double"))\
            .withColumn("felt", col("felt").cast("double"))   

    return df

dp.create_streaming_table(name="earthquake_data_final")

dp.apply_changes(
    target="earthquake_data_final",
    source="earthquake_data_vw",
    keys=["id"],
    sequence_by=col("_load_timestamp"),
    stored_as_scd_type='1'
)
