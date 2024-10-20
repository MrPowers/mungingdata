---
title: "Incrementally Updating Extracts with Spark"
date: "2018-07-07"
categories: 
  - "apache-spark"
---

Spark Structured Streaming and `Trigger.Once` can be used to incrementally update Spark extracts with ease.

An extract that updates incrementally will take the same amount of time as a normal extract for the initial run, but subsequent runs will execute much faster.

I recently used this technology to refactor a batch job that took 11.5 hours to execute with an incrementally updating extract that only takes 8 minutes to run (on a cluster that's three times smaller)!

Incremental updates save a lot of time and money!

## Batch Extracts

Here's how to extract all rows with an age less than 18 from a data lake.

We'll use the [spark-daria](https://github.com/MrPowers/spark-daria) `EtlDefinition` object to wire up the extract, [as described in this blog post](https://medium.com/@mrpowers/how-to-write-spark-etl-processes-df01b0c1bec9).

```
val lakeDF = spark.read.parquet("s3a://some-bucket/my-data-lake")

def filterMinors()(df: DataFrame): DataFrame = {
  df
    .filter(col("age") < 18)
    .repartition(2000)
}

def exampleWriter()(df: DataFrame): Unit = {
  val path = "s3a://some-bucket/extracts/adults"
  df.write.mode(SaveMode.Overwrite).parquet(path)
}

val etl = new EtlDefinition(
  sourceDF = lakeDF,
  transform = filterMinors(),
  write = exampleWriter()
)

etl.process()
```

In batch mode, you always need to manually repartition an extract after filtering from the data lake, as described [in this blog post](https://hackernoon.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4).

It's hard to determine the optimal number of partitions to use for each extract. You can estimate the number of optimal partitions by measuring the size of your entire data lake and assuming each row contains the same amount of data.

Let's say your data lake contains 10 billion rows and 2,000 GB of data. Let's say you run an extract that contains 1 billion rows and you'd like each partition of the extract to contain 100 MB (0.1 GB) of data. You can assume the extract will contain 200 GB of data (because the extract is only 10% the size of the entire lake) and you'll need 2,000 partitions for each partition to contain 100 MB of data.

Manually specifying the number of partitions is annoying and requires a bit of trial and error to get right. You're chasing a moving target as the data lake grows and the number of optimal partitions increases.

Whenever the dataset is filtered, repartitioning is critical or your extract will contain a lot of files with no data. Empty partitions cause a lot of unnecessary network traffic and cause Spark to run slowly.

Thankfully incremental update technology removes the need to manually specify the number of partitions.

## Incrementally Updating Extracts

Spark Structured Streaming coupled with `Trigger.Once` can be used to create extracts that incrementally update, as described [in this blog post](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html).

The Structured Streaming API is similar to the batch API, so we only need to make minor refactorings to our code.

```
val schema = StructType(
  List(
    StructField("first_name", StringType, true),
    StructField("age", IntegerType, true)
  )
)

val streamingLakeDF = spark
  .readStream
  .format("parquet")
  .schema(schema)
  .load("s3a://some-bucket/my-data-lake")

def filterMinors()(df: DataFrame): DataFrame = {
  df
    .filter(col("age") < 18)
}

def parquetStreamWriter(dataPath: String, checkpointPath: String)(df: DataFrame): Unit = {
  df
    .writeStream
    .trigger(Trigger.Once)
    .format("parquet")
    .option("checkpointLocation", checkpointPath)
    .start(dataPath)
}

val etl = new EtlDefinition(
  sourceDF = streamingLakeDF,
  transform = filterMinors(),
  write = parquetStreamWriter(
    "s3a://some-bucket/incremental_extracts/data/adults",
    "s3a://some-bucket/incremental_extracts/checkpoints/adults"
  )
)

etl.process()
```

Key differences with the new code:

- We need to explicitly specify the schema of our data lake (the [spark-daria](https://github.com/MrPowers/spark-daria) `printSchemaInCodeFormat` method makes this easy)
- We don't need to specify how the extract will be repartioned, Spark Structured Streaming does this for us automatically
- We need to specify a checkpoint directory when writing out the data

The initial run of an incremental extract needs to be run on a big cluster (initial runs need clusters that are the same as clusters for batch runs). On subsequent runs, the checkpoint directory will keep track of the data lake files that have already been analyzed. The subsequent runs will analyse files that were added to the data lake since the last run. That's why subsequent runs can be executed on clusters that are much smaller and will take less times to complete.

## Full refresh of an incremental extract

Whenever the transformation logic is modified, you'll need to do a full refresh of the incremental extract. For example, if the transformation is changed from an age of 18 to 16, then a full refresh is required.

```
def filterMinors()(df: DataFrame): DataFrame = {
  df
    .filter(col(age) < 16)
}
```

You can simply delete the data folder and the checkpoint folder and run the exact same code to do a full refresh.

Here are the AWS CLI commands to delete those folders.

```
aws s3 rm s3a://some-bucket/incremental_extracts/data/adults --recursive
aws s3 rm s3a://some-bucket/incremental_extracts/checkpoints/adults --recursive
```

Remember that you'll need a bigger cluster for full refreshes than incremental updates.

## Incremental Complications

It's hard to incrementally update analyses that perform aggregations Spark supports [watermarking](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking), but I haven't figured out how to use it yet. I'll publish another blog when I figure it out ;)

## Next Steps

Incrementally updating analyses are necessary to keep processing times low and control costs.

Data extracts are a great place to add Structured Streaming and `Trigger.Once` to your ETL stack so you can avoid the complications of watermarking.

Incrementally updating extracts will save you a ton of time and money!
