---
title: "Defining DataFrame Schemas with StructField and StructType"
date: "2019-03-06"
categories: 
  - "apache-spark"
---

# Defining DataFrame Schemas with StructField and StructType

Spark DataFrames schemas are defined as a collection of typed columns. The entire schema is stored as a `StructType` and individual columns are stored as `StructFields`.

This blog post explains how to create and modify Spark schemas via the `StructType` and `StructField` classes.

We'll show how to work with `IntegerType`, `StringType`, `LongType`, `ArrayType`, `MapType` and `StructType` columns.

Mastering Spark schemas is necessary for debugging code and writing tests.

This blog post provides a great introduction to these topics, but [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) provides a much more comprehensive review of the topics covered in this post. [The book](https://leanpub.com/beautiful-spark/) is the fastest way for you to become a strong Spark programmer.

## Defining a schema to create a DataFrame

Let's invent some sample data, define a schema, and create a DataFrame.

```scala
import org.apache.spark.sql.types._

val data = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val schema = StructType(
  List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

```
df.show()

+------+-----+
|number| word|
+------+-----+
|     8|  bat|
|    64|mouse|
|   -27|horse|
+------+-----+
```

`StructType` objects are instantiated with a `List` of `StructField` objects.

The `org.apache.spark.sql.types` package must be imported to access `StructType`, `StructField`, `IntegerType`, and `StringType`.

The `createDataFrame()` method takes two arguments:

1. RDD of the data
2. The DataFrame schema (a `StructType` object)

The `schema()` method returns a `StructType` object:

```scala
df.schema

StructType(
  StructField(number,IntegerType,true),
  StructField(word,StringType,true)
)
```

## `StructField`

`StructFields` model each column in a DataFrame.

`StructField` objects are created with the `name`, `dataType`, and `nullable` properties. Here's an example:

```
StructField("word", StringType, true)
```

The `StructField` above sets the `name` field to `"word"`, the `dataType` field to `StringType`, and the `nullable` field to `true`.

`"word"` is the name of the column in the DataFrame.

`StringType` means that the column can only take string values like `"hello"` - it cannot take other values like `34` or `false`.

When the `nullable` field is set to `true`, the column can accept `null` values.

## Defining schemas with the `::` operator

We can also define a schema with the `::` operator, like the examples in the [StructType documentation](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType).

```scala
val schema = StructType(
  StructField("number", IntegerType, true) ::
  StructField("word", StringType, true) :: Nil
)
```

The `::` operator makes it easy to construct lists in Scala. We can also use `::` to make a list of numbers.

```
5 :: 4 :: Nil
```

Notice that the last element always has to be `Nil` or the code will error out.

## Defining schemas with the `add()` method

We can use the `StructType#add()` method to define schemas.

```scala
val schema = StructType(Seq(StructField("number", IntegerType, true)))
  .add(StructField("word", StringType, true))
```

`add()` is an overloaded method and there are several different ways to invoke it - this will work too:

```scala
val schema = StructType(Seq(StructField("number", IntegerType, true)))
  .add("word", StringType, true)
```

Check the [StructType documentation](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType) for all the different ways `add()` can be used.

## Common errors

### Extra column defined in Schema

The following code has an extra column defined in the schema and will error out with this message: `java.lang.RuntimeException: Error while encoding: java.lang.ArrayIndexOutOfBoundsException: 2`.

```scala
val data = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val schema = StructType(
  List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true),
    StructField("num2", IntegerType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

The data only contains two columns, but the schema contains three `StructField` columns.

### Type mismatch

The following code incorrectly characterizes a string column as an integer column and will error out with this message: `java.lang.RuntimeException: Error while encoding: java.lang.RuntimeException: java.lang.String is not a valid external type for schema of int`.

```scala
val data = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val schema = StructType(
  List(
    StructField("num1", IntegerType, true),
    StructField("num2", IntegerType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

df.show()
```

The first column of data (`8`, `64`, and `-27`) can be characterized as `IntegerType` data.

The second column of data (`"bat"`, `"mouse"`, and `"horse"`) cannot be characterized as an `IntegerType` column - this could would work if this column was recharacterized as `StringType`.

### Nullable property exception

The following code incorrectly tries to add `null` to a column with a `nullable` property set to false and will error out with this message: `java.lang.RuntimeException: Error while encoding: java.lang.RuntimeException: The 0th field 'word1' of input row cannot be null`.

```scala
val data = Seq(
  Row("hi", "bat"),
  Row("bye", "mouse"),
  Row(null, "horse")
)

val schema = StructType(
  List(
    StructField("word1", StringType, false),
    StructField("word2", StringType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

df.show()
```

## `LongType`

Integers use 32 bits whereas long values use 64 bits.

Integers can hold values between -2 billion to 2 billion (`-scala.math.pow(2, 31)` to `scala.math.pow(2, 31) - 1` to be exact).

Long values are suitable for bigger integers. You can create a long value in Scala by appending `L` to an integer - e.g. `4L` or `-60L`.

Let's create a DataFrame with a `LongType` column.

```scala
val data = Seq(
  Row(5L, "bat"),
  Row(-10L, "mouse"),
  Row(4L, "horse")
)

val schema = StructType(
  List(
    StructField("long_num", LongType, true),
    StructField("word", StringType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

```
df.show()

+--------+-----+
|long_num| word|
+--------+-----+
|       5|  bat|
|     -10|mouse|
|       4|horse|
+--------+-----+
```

You'll get the following error message if you try to add integers to a `LongType` column: `java.lang.RuntimeException: Error while encoding: java.lang.RuntimeException: java.lang.Integer is not a valid external type for schema of bigint`

Here's an example of the erroneous code:

```scala
val data = Seq(
  Row(45, "bat"),
  Row(2, "mouse"),
  Row(3, "horse")
)

val schema = StructType(
  List(
    StructField("long_num", LongType, true),
    StructField("word", StringType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

df.show()
```

## `ArrayType`

Spark supports columns that contain arrays of values. Scala offers lists, sequences, and arrays. In regular Scala code, it's best to use `List` or `Seq`, but `Arrays` are frequently used with Spark.

Here's how to create an array of numbers with Scala:

```scala
val numbers = Array(1, 2, 3)
```

Let's create a DataFrame with an `ArrayType` column.

```scala
val data = Seq(
  Row("bieber", Array("baby", "sorry")),
  Row("ozuna", Array("criminal"))
)

val schema = StructType(
  List(
    StructField("name", StringType, true),
    StructField("hit_songs", ArrayType(StringType, true), true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

```
df.show()

+------+-------------+
|  name|    hit_songs|
+------+-------------+
|bieber|[baby, sorry]|
| ozuna|   [criminal]|
+------+-------------+
```

## `MapType`

Scala [maps](https://docs.scala-lang.org/overviews/collections/maps.html) store key / value pairs (maps are called "hashes" in other programming languages). Let's create a Scala map with beers and their country of origin.

```scala
val beers = Map("aguila" -> "Colombia", "modelo" -> "Mexico")
```

Let's grab the value that's associated with the key `"modelo"`:

```scala
beers("modelo") // Mexico
```

Let's create a DataFrame with a `MapType` column.

```scala
val data = Seq(
  Row("sublime", Map(
    "good_song" -> "santeria",
    "bad_song" -> "doesn't exist")
  ),
  Row("prince_royce", Map(
    "good_song" -> "darte un beso",
    "bad_song" -> "back it up")
  )
)

val schema = StructType(
  List(
    StructField("name", StringType, true),
    StructField("songs", MapType(StringType, StringType, true), true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

```
df.show(false)

+------------+----------------------------------------------------+
|name        |songs                                               |
+------------+----------------------------------------------------+
|sublime     |[good_song -> santeria, bad_song -> doesn't exist]  |
|prince_royce|[good_song -> darte un beso, bad_song -> back it up]|
+------------+----------------------------------------------------+
```

Notice that `MapType` is instantiated with three arguments (e.g. `MapType(StringType, StringType, true)`). The first argument is the `keyType`, the second argument is the `valueType`, and the third argument is a boolean flag for `valueContainsNull`. Map values can contain `null` if `valueContainsNull` is set to `true`, but the key can never be `null`.

## `StructType` nested schemas

DataFrame schemas can be nested. A DataFrame column can be a `struct` - it's essentially a schema within a schema.

Let's create a DataFrame with a `StructType` column.

```scala
val data = Seq(
  Row("bob", Row("blue", 45)),
  Row("mary", Row("red", 64))
)

val schema = StructType(
  List(
    StructField("name", StringType, true),
    StructField(
      "person_details",
      StructType(
        List(
          StructField("favorite_color", StringType, true),
          StructField("age", IntegerType, true)
        )
      ),
      true
    )
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)
```

```
df.show()

+----+--------------+
|name|person_details|
+----+--------------+
| bob|    [blue, 45]|
|mary|     [red, 64]|
+----+--------------+
```

We can use the `printSchema()` method to illustrate that `person_details` is a `struct` column:

```
df.printSchema()

root
 |-- name: string (nullable = true)
 |-- person_details: struct (nullable = true)
 |    |-- favorite_color: string (nullable = true)
 |    |-- age: integer (nullable = true)
```

## `StructType` object oriented programming

The `StructType` object mixes in the [`Seq` trait](https://www.scala-lang.org/api/2.12.0/scala/collection/Seq.html) to access a bunch of collection methods.

Here's how `StructType` is defined:

```scala
case class StructType(fields: Array[StructField])
  extends DataType
  with Seq[StructField]
```

Here's the [StructType source code](https://github.com/apache/spark/blob/a1c1dd3484a4dcd7c38fe256e69dbaaaf10d1a92/sql/catalyst/src/main/scala/org/apache/spark/sql/types/StructType.scala).

The Scala `Seq` trait is defined as follows:

```scala
trait Seq[+A]
  extends PartialFunction[Int, A]
  with Iterable[A]
  with GenSeq[A]
  with GenericTraversableTemplate[A, Seq]
  with SeqLike[A, Seq[A]]
```

By inheriting from the `Seq` trait, the `StructType` class gets access to collection methods like `collect()` and `foldLeft()`.

Let's create a DataFrame schema and use the `foldLeft()` method to create a sequence of all the column names.

```scala
val data = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val schema = StructType(
  List(
    StructField("number", IntegerType, true),
    StructField("word", StringType, true)
  )
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

val columns = schema.foldLeft(Seq.empty[String]) {(memo: Seq[String], s: StructField) =>
  memo ++ Seq(s.name)
}
```

If we really wanted to get a list of all the column names, we could just run `df.columns`, but the `foldLeft()` method is clearly more powerful - it let's us perform arbitrary collection operations on our DataFrame schemas.

## Flattening DataFrames with `StructType` columns

In the previous section, we created a DataFrame with a `StructType` column. Let's expand the two columns in the nested `StructType` column to be two separate fields.

We will leverage a `flattenSchema` method from [spark-daria](https://github.com/MrPowers/spark-daria) to make this easy.

```scala
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

val flattenedDF = df.flattenSchema(delimiter = "_")
```

```
flattenedDF.show()

+----+-----------------------------+------------------+
|name|person_details_favorite_color|person_details_age|
+----+-----------------------------+------------------+
| bob|                         blue|                45|
|mary|                          red|                64|
+----+-----------------------------+------------------+
```

Take a look at the [spark-daria source code](https://github.com/MrPowers/spark-daria/blob/master/src/main/scala/com/github/mrpowers/spark/daria/sql/DataFrameExt.scala) to see how this code works.

## Next steps

You'll be defining a lot of schemas in your test suites so make sure to master all the concepts covered in this blog post.
