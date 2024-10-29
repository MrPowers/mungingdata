---
title: "Working with Spark MapType Columns"
date: "2020-01-15"
categories: 
  - "apache-spark"
---

# Working with Spark MapType Columns

Spark DataFrame columns support maps, which are great for key / value pairs with an arbitrary length.

This blog post describes how to create MapType columns, demonstrates built-in functions to manipulate MapType columns, and explain when to use maps in your analyses.

Make sure to read [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) for a detailed overview of how to use MapType columns in production applications.

## Scala maps

Let's begin with a little refresher on Scala maps.

Create a Scala map that connects some English and Spanish words.

```scala
val wordMapping = Map("one" -> "uno", "dog" -> "perro")
```

Fetch the value associated with the `dog` key:

```scala
wordMapping("dog") // "perro"
```

## Creating MapType columns

Let's create a DataFrame with a MapType column.

```scala
val singersDF = spark.createDF(
  List(
    ("sublime", Map(
      "good_song" -> "santeria",
      "bad_song" -> "doesn't exist")
    ),
    ("prince_royce", Map(
      "good_song" -> "darte un beso",
      "bad_song" -> "back it up")
    )
  ), List(
    ("name", StringType, true),
    ("songs", MapType(StringType, StringType, true), true)
  )
)
```

```
singersDF.show(false)

+------------+----------------------------------------------------+
|name        |songs                                               |
+------------+----------------------------------------------------+
|sublime     |[good_song -> santeria, bad_song -> doesn't exist]  |
|prince_royce|[good_song -> darte un beso, bad_song -> back it up]|
+------------+----------------------------------------------------+
```

Let's examine the DataFrame schema and verify that the `songs` column has a `MapType`:

```
singersDF.printSchema()

root
 |-- name: string (nullable = true)
 |-- songs: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
```

We can see that `songs` is a MapType column.

Let's explore some built-in Spark methods that make it easy to work with MapType columns.

## Fetching values from maps with element\_at()

Let's use the `singersDF` DataFrame and append `song_to_love` as a column.

```scala
singersDF
  .withColumn("song_to_love", element_at(col("songs"), "good_song"))
  .show(false)
```

```
+------------+----------------------------------------------------+-------------+
|name        |songs                                               |song_to_love |
+------------+----------------------------------------------------+-------------+
|sublime     |[good_song -> santeria, bad_song -> doesn't exist]  |santeria     |
|prince_royce|[good_song -> darte un beso, bad_song -> back it up]|darte un beso|
+------------+----------------------------------------------------+-------------+
```

The `element_at()` function fetches a value from a MapType column.

## Appending MapType columns

We can use the `map()` method defined in `org.apache.spark.sql.functions` to append a `MapType` column to a DataFrame.

```scala
val countriesDF = spark.createDF(
  List(
    ("costa_rica", "sloth"),
    ("nepal", "red_panda")
  ), List(
    ("country_name", StringType, true),
    ("cute_animal", StringType, true)
  )
).withColumn(
  "some_map",
  map(col("country_name"), col("cute_animal"))
)
```

```
countriesDF.show(false)

+------------+-----------+---------------------+
|country_name|cute_animal|some_map             |
+------------+-----------+---------------------+
|costa_rica  |sloth      |[costa_rica -> sloth]|
|nepal       |red_panda  |[nepal -> red_panda] |
+------------+-----------+---------------------+
```

Let's verify that `some_map` is a `MapType` column:

```
countriesDF.printSchema()

root
 |-- country_name: string (nullable = true)
 |-- cute_animal: string (nullable = true)
 |-- some_map: map (nullable = false)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
```

## Creating MapType columns from two ArrayType columns

We can create a `MapType` column from two `ArrayType` columns.

```scala
val df = spark.createDF(
  List(
    (Array("a", "b"), Array(1, 2)),
    (Array("x", "y"), Array(33, 44))
  ), List(
    ("letters", ArrayType(StringType, true), true),
    ("numbers", ArrayType(IntegerType, true), true)
  )
).withColumn(
  "strange_map",
  map_from_arrays(col("letters"), col("numbers"))
)
```

```
df.show(false)

+-------+--------+------------------+
|letters|numbers |strange_map       |
+-------+--------+------------------+
|[a, b] |[1, 2]  |[a -> 1, b -> 2]  |
|[x, y] |[33, 44]|[x -> 33, y -> 44]|
+-------+--------+------------------+
```

Let's take a look at the `df` schema and verify `strange_map` is a `MapType` column:

```
df.printSchema()

 |-- letters: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- numbers: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- strange_map: map (nullable = true)
 |    |-- key: string
 |    |-- value: integer (valueContainsNull = true)
```

The Spark way of converting to arrays to a map is different that the "regular Scala" way of converting two arrays to a map.

## Converting Arrays to Maps with Scala

Here's how you'd convert two collections to a map with Scala.

```scala
val list1 = List("a", "b")
val list2 = List(1, 2)

list1.zip(list2).toMap // Map(a -> 1, b -> 2)
```

We could wrap this code in a User Defined Function and define our own `map_from_arrays` function if we wanted.

In general, it's best to rely on the standard Spark library instead of defining our own UDFs.

The key takeaway is that the Spark way of solving a problem is often different from the Scala way. Read the API docs and always try to solve your problems the Spark way.

## Merging maps with map\_concat()

`map_concat()` can be used to combine multiple MapType columns to a single MapType column.

```scala
val df = spark.createDF(
  List(
    (Map("a" -> "aaa", "b" -> "bbb"), Map("c" -> "ccc", "d" -> "ddd"))
  ), List(
    ("some_data", MapType(StringType, StringType, true), true),
    ("more_data", MapType(StringType, StringType, true), true)
  )
)

df
  .withColumn("all_data", map_concat(col("some_data"), col("more_data")))
  .show(false)
```

```
+--------------------+--------------------+----------------------------------------+
|some_data           |more_data           |all_data                                |
+--------------------+--------------------+----------------------------------------+
|[a -> aaa, b -> bbb]|[c -> ccc, d -> ddd]|[a -> aaa, b -> bbb, c -> ccc, d -> ddd]|
+--------------------+--------------------+----------------------------------------+
```

## Using StructType columns instead of MapType columns

Let's create a DataFrame that stores information about athletes.

```scala
val athletesDF = spark.createDF(
  List(
    ("lebron",
      Map(
        "height" -> "6.67",
        "units" -> "feet"
      )
    ),
    ("messi",
      Map(
        "height" -> "1.7",
        "units" -> "meters"
      )
    )
  ), List(
    ("name", StringType, true),
    ("stature", MapType(StringType, StringType, true), true)
  )
)

athletesDF.show(false)
```

```
+------+--------------------------------+
|name  |stature                         |
+------+--------------------------------+
|lebron|[height -> 6.67, units -> feet] |
|messi |[height -> 1.7, units -> meters]|
+------+--------------------------------+
```

```
athletesDF.printSchema()

root
 |-- name: string (nullable = true)
 |-- stature: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
```

`stature` is a MapType column, but we can also store stature as a StructType column.

```scala
val data = Seq(
  Row("lebron", Row("6.67", "feet")),
  Row("messi", Row("1.7", "meters"))
)

val schema = StructType(
  List(
    StructField("player_name", StringType, true),
    StructField(
      "stature",
      StructType(
        List(
          StructField("height", StringType, true),
          StructField("unit", StringType, true)
        )
      ),
      true
    )
  )
)

val athletesDF = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

```
athletesDF.show(false)

+-----------+-------------+
|player_name|stature      |
+-----------+-------------+
|lebron     |[6.67, feet] |
|messi      |[1.7, meters]|
+-----------+-------------+
```

```
athletesDF.printSchema()

root
 |-- player_name: string (nullable = true)
 |-- stature: struct (nullable = true)
 |    |-- height: string (nullable = true)
 |    |-- unit: string (nullable = true)
```

Sometimes both StructType and MapType columns can solve the same problem and you can choose between the two.

## Writing MapType columns to disk

The CSV file format cannot handle MapType columns.

This code will error out.

```scala
val outputPath = new java.io.File("./tmp/csv_with_map/").getCanonicalPath

spark.createDF(
  List(
    (Map("a" -> "aaa", "b" -> "bbb"))
  ), List(
    ("some_data", MapType(StringType, StringType, true), true)
  )
).write.csv(outputPath)
```

Here's the error message:

```
writing to disk
- cannot write maps to disk with the CSV format *** FAILED ***
  org.apache.spark.sql.AnalysisException: CSV data source does not support map<string,string> data type.;
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$$anonfun$verifySchema$1.apply(DataSourceUtils.scala:69)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$$anonfun$verifySchema$1.apply(DataSourceUtils.scala:67)
  at scala.collection.Iterator$class.foreach(Iterator.scala:891)
  at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
  at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
  at org.apache.spark.sql.types.StructType.foreach(StructType.scala:99)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$.verifySchema(DataSourceUtils.scala:67)
  at org.apache.spark.sql.execution.datasources.DataSourceUtils$.verifyWriteSchema(DataSourceUtils.scala:34)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:100)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:159)
```

MapType columns can be written out with the Parquet file format. This code runs just fine:

```scala
val outputPath = new java.io.File("./tmp/csv_with_map/").getCanonicalPath

spark.createDF(
  List(
    (Map("a" -> "aaa", "b" -> "bbb"))
  ), List(
    ("some_data", MapType(StringType, StringType, true), true)
  )
).write.parquet(outputPath)
```

## Conclusion

MapType columns are a great way to store key / value pairs of arbitrary lengths in a DataFrame column.

Spark 2.4 added a lot of native functions that make it easier to work with MapType columns. Prior to Spark 2.4, developers were overly reliant on UDFs for manipulating MapType columns.

StructType columns can often be used instead of a MapType column. Study both of these column types closely so you can understand the tradeoffs and intelligently select the best column type for your analysis.
