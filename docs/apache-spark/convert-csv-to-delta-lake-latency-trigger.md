---
title: "Convert streaming CSV data to Delta Lake with different latency requirements"
date: "2022-06-04"
categories: 
  - "apache-spark"
---

# Convert streaming CSV data to Delta Lake with different latency requirements

This blog post explains how to incrementally convert streaming CSV data into Delta Lake with different latency requirements. A streaming CSV data source is used because it's easy to demo, but the lessons covered in this post also apply to streaming event platforms like Kafka or Kinesis.

This post will show you three ways to convert CSV to Delta Lake, with different latency and cost implications. In general, lower latency solutions cost more to run in production.

Here are the different options we'll cover:

- Structured Streaming & Trigger Once: low cost & high latency
- Structured Streaming & microbatch processing: higher cost & lower latency
- Reading streaming data directly: lowest latency

All of the code covered in this post is organized in [this notebook](https://github.com/MrPowers/delta-examples/blob/master/notebooks/csv-to-delta.ipynb) if you'd like to run these computations on your local machine.

## Project and data setup

We're going to setup a Structured Streaming process to watch a directory that'll have data that's added incrementally. The CSV data files will look like this:

```
student_name,graduation_year,major
someXXperson,2023,math
liXXyao,2025,physics
```

The data files have three columns: `student_name`, `graduation_year`, and `major`. As you can see the `student_name` column contains both the `first_name` and `last_name`, separated by `XX`. We'll want to properly split `first_name` and `last_name` into separate columns before writing to the Delta Lake.

Here's the function that'll normalize the `student_name` column:

```python
def with_normalized_names(df):
    split_col = pyspark.sql.functions.split(df["student_name"], "XX")
    return (
        df.withColumn("first_name", split_col.getItem(0))
        .withColumn("last_name", split_col.getItem(1))
        .drop("student_name")
    )
```

When you're reading data with Structured Streaming, you also need to specify the schema as follows:

```python
schema = (
    StructType()
    .add("student_name", "string")
    .add("graduation_year", "string")
    .add("major", "string")
)
```

Now let's look at how to initialize a PySpark SparkSession with Delta Lake so you can run these examples in a localhost notebook.

## Creating the PySpark SparkSession

Here's how to create the PySpark SparkSession when you're using Delta Lake:

```python
import pyspark
from delta import *
from pyspark.sql.types import StructType

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

See this blog post on [installing PySpark and Delta Lake with conda](https://mungingdata.com/pyspark/install-delta-lake-jupyter-conda-mac/) if you haven't installed all the dependencies in your localhost environment yet.

Now let's look at performing incremental updates with Structured Streaming and Trigger Once.

## Option 1: Structured Streaming and Trigger Once

We have `students1.csv`, `students2.csv` and `students3.csv` files. We'll manually move these files into the `tmp_students_incremental` directory throughout this example to simulate a directory that's being incrementally updated with CSV data in a streaming manner.

Let's start by moving the `students1.csv` file into the `tmp_students_incremental` directory:

```
mkdir data/tmp_students_incremental
cp data/students/students1.csv data/tmp_students_incremental
```

Let's read this data into a streaming DataFrame:

```python
df = (
    spark.readStream.schema(schema)
    .option("header", True)
    .csv("data/tmp_students_incremental")
)
```

We'd now like to apply the `with_normalized_names` transformation and write the data to a Delta Lake. Let's wrap this trigger once invocation in a function:

```python
def perform_trigger_once_update():
    checkpointPath = "data/tmp_students_checkpoint/"
    deltaPath = "data/tmp_students_delta"
    return df.transform(lambda df: with_normalized_names(df)).writeStream.trigger(
        once=True
    ).format("delta").option("checkpointLocation", checkpointPath).start(deltaPath)
```

You need a `checkpointLocation` to track the files that have already been processed. When the trigger once function is invoked, it'll will look at all the CSV files in the streaming directory, check the files that have already been processed in the `checkpointLocation` directory, and only process the new files.

Run the `perform_trigger_once_update()` function and then observe the contents of the Delta Lake.

```python
perform_trigger_once_update()

spark.read.format("delta").load(deltaPath).show()
```

```
+---------------+-------+----------+---------+
|graduation_year|  major|first_name|last_name|
+---------------+-------+----------+---------+
|           2023|   math|      some|   person|
|           2025|physics|        li|      yao|
+---------------+-------+----------+---------+
```

The CSV data was cleaned with the `with_normalized_names` function and is properly written to the Delta Lake.

Now copy over the `students2.csv` file to the `tmp_students_incremental` folder with `cp data/students/students2.csv data/tmp_students_incremental`, perform another trigger once update, and observe the contents of the Delta Lake.

```python
perform_trigger_once_update()

spark.read.format("delta").load(deltaPath).show()
```

```
+---------------+-------+----------+---------+
|graduation_year|  major|first_name|last_name|
+---------------+-------+----------+---------+
|           2022|    bio|    sophia|     raul|
|           2025|physics|      fred|       li|
|           2023|   math|      some|   person|
|           2025|physics|        li|      yao|
+---------------+-------+----------+---------+
```

Spark correctly updated the Delta Lake with the data in `students2.csv`.

Perform the final incremental update by copying `students3.csv` to `tmp_students_incremental`:

```
cp data/students/students3.csv data/tmp_students_incremental
```

Perform another incremental update and view the contents of the Delta Lake.

```python
perform_trigger_once_update()

spark.read.format("delta").load(deltaPath).show()
```

```
+---------------+-------+----------+---------+
|graduation_year|  major|first_name|last_name|
+---------------+-------+----------+---------+
|           2025|    bio|     chris|     borg|
|           2026|physics|     david|    cross|
|           2022|    bio|    sophia|     raul|
|           2025|physics|      fred|       li|
|           2023|   math|      some|   person|
|           2025|physics|        li|      yao|
+---------------+-------+----------+---------+
```

You're able to incrementally update the Delta Lake by simply invoking the `perform_trigger_once_update()` function. Spark is intelligent enough to only process the new data for each invocation.

You can invoke the `perform_trigger_once_update()` function as frequently or as seldom as you'd like. You can invoke the function every 3 hours, every day, or every week. It depends on the latency requirements of the Delta Lake that's being updated.

Suppose your Delta Lake is queried by a business user on a daily basis every morning. The Delta Lake is only queried once per day, so you only need to perform daily updates. In this case, you can setup a cron job to run `perform_trigger_once_update()` every morning at 8AM, so the Delta Lake is updated by 9AM for the business user.

Trigger once updates are less costly than constant updates that require a cluster to be continuously running. If you only need periodic updates, it's more economical to kick off an incremental update with cron, perform the update, and then shut down the cluster when the update finishes.

Now let's look at a different situation where you need to build a system that updates the Delta Lake every 2 seconds. This needs to be architected differently to account for the different latency requirement.

## Option 2: Structured Streaming & microbatch processing

This section shows how to use a Structured Streaming cluster to update a Delta Lake with streaming data every two seconds. This cluster needs to be kept running at all times.

As before, let's read the CSV data with `readStream`:

```python
df = (
    spark.readStream.schema(schema)
    .option("header", True)
    .csv("data/tmp_students_incremental")
)
```

Let's write out any new data to the Delta Lake every two seconds.

```python
checkpointPath = "data/tmp_students_checkpoint/"
deltaPath = "data/tmp_students_delta"

df.transform(lambda df: with_normalized_names(df)).writeStream.trigger(
    processingTime="2 seconds"
).format("delta").option("checkpointLocation", checkpointPath).start(deltaPath)
```

Copy over the `students1.csv` data file with `cp data/students/students1.csv data/tmp_students_incremental`, wait two seconds, and then the Delta Lake will be automatically updated.

Check the contents of the Delta Lake:

```python
spark.read.format("delta").load(deltaPath).show()

+---------------+-------+----------+---------+
|graduation_year|  major|first_name|last_name|
+---------------+-------+----------+---------+
|           2023|   math|      some|   person|
|           2025|physics|        li|      yao|
+---------------+-------+----------+---------+
```

Now copy over the `students2.csv` file, wait two seconds, and check that the Delta Lake has been automatically updated.

```
cp data/students/students2.csv data/tmp_students_incremental

spark.read.format("delta").load(deltaPath).show()

+---------------+-------+----------+---------+
|graduation_year|  major|first_name|last_name|
+---------------+-------+----------+---------+
|           2023|   math|      some|   person|
|           2025|physics|        li|      yao|
|           2022|    bio|    sophia|     raul|
|           2025|physics|      fred|       li|
+---------------+-------+----------+---------+
```

With trigger once, we needed to invoke a function every time we wanted the update to run. When the `trigger` is set to `processingTime="2 seconds"`, you don't need to invoke a function to perform the update - it happens automatically every two seconds.

Finally copy over `students3.csv`, wait two seconds, and again check that the Delta Lake was updated.

```
cp data/students/students3.csv data/tmp_students_incremental

spark.read.format("delta").load(deltaPath).show()

+---------------+-------+----------+---------+
|graduation_year|  major|first_name|last_name|
+---------------+-------+----------+---------+
|           2025|    bio|     chris|     borg|
|           2026|physics|     david|    cross|
|           2023|   math|      some|   person|
|           2025|physics|        li|      yao|
|           2022|    bio|    sophia|     raul|
|           2025|physics|      fred|       li|
+---------------+-------+----------+---------+
```

You can just keep the cluster running and Spark will automatically detect any new streaming data and write it to the Delta Lake every two seconds. This of course means you need to keep the cluster running 24/7, which is more expensive than running a periodic job with trigger once. This approach offers lower latency with higher cost. It's a great option when you're building a system that needs to be updated with low latency.

## Option 3: Reading streaming data directly

You can also read the streaming CSV data directly, which will have the lowest latency.

Here's how to continuously stream data to the console:

```python
df.writeStream \
  .format("console") \
  .trigger(continuous='1 second') \
  .start()
```

See the [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for more details.

Continuous streaming is still experimental, but is a promising option for applications that have extremely low latency requirements.

## Conclusion

This post shows a variety of ways to incrementally update a Delta Lake, depending on the latency requirements of your application.

Make sure to investigate the latency requirements of the end users in detail before building an ETL pipeline. Sometimes users will casually mention that they'd like a realtime dashboard and upon further digging, you'll find that they actually will only be looking at the dashboard a couple of times a day. You don't need realtime updates if you're not going to change decision making based on the last couple of seconds of data.

An ETL pipeline that's consumed by an automated process for a high-frequency trading system, on the other hand, might actually need to be realtime.

"Realtime" is a loaded term in the streaming space. A pipeline with 5 seconds of latency is way easier to build than something that's truly "realtime".

It's best to work backwards, determine the latency requirements of the system, and then architect a pipeline that meets the needs of the end users. There is no sense in building a pipeline with 2 second latency, and incurring the costs of a cluster that's constantly running, if the end users only need hourly updates.

The lower the latency of the system, the higher the probability that the pipeline will generate lots of small files. Delta Lake is great at performing backwards compatible small file compaction. Make sure you've setup auto optimization if your pipeline will generate lots of small files.

This post covered a simple example, but your streaming pipeline may be more complicated:

- you may have multiple streaming data sources that need to be joined before writing to the Delta Lake
- you may want to perform stateful aggregations before writing to the Delta Lake
- writing to multiple data sources

Future blog posts will explain these different scenarios in detail. Luckily for you, Structured Streaming makes it easy to build pipelines that are incrementally updated for all of these situations. Enjoy the beautiful user interface!
