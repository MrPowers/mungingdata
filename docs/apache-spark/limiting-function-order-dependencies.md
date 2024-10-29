---
title: "Limiting Order Dependencies in Spark Functions"
date: "2018-09-01"
categories: 
  - "apache-spark"
---

# Limiting Order Dependencies in Spark Functions

Spark codebases can easily become a collection of order dependent custom transformations (see [this blog post](https://medium.com/@mrpowers/the-different-type-of-spark-functions-custom-transformations-column-functions-udfs-bf556c9d0ce7) for background on custom transformations). Your library will be difficult to use if many functions need to be run in a specific order. This post shows how to run functions to add intermediate columns when necessary and limit order dependencies. This design pattern will provider your users a better experience.

## Example of order dependent transformations

Suppose we have a DataFrame with a `city` column. We have one transformation to add a `country` column to the DataFrame and another transformation to add a `hemisphere` column to the DataFrame (to indicate if the country is in the Northern Hemisphere or Southern Hemisphere).

```scala
def withCountry()(df: DataFrame): DataFrame = {
  df.withColumn(
    "country",
    when(col("city") === "Calgary", "Canada")
      .when(col("city") === "Buenos Aires", "Argentina")
      .when(col("city") === "Cape Town", "South Africa")
  )
}

def withHemisphere()(df: DataFrame): DataFrame = {
  df.withColumn(
    "hemisphere",
    when(col("country") === "Canada", "Northern Hemisphere")
      .when(col("country") === "Argentina", "Southern Hemisphere")
      .when(col("country") === "South Africa", "Southern Hemisphere")
  )
}
```

Let's create a DataFrame with cities and run our transformations.

```scala
val df = spark.createDF(
  List(
    ("Calgary"),
    ("Buenos Aires"),
    ("Cape Town")
  ), List(
    ("city", StringType, true)
  )
)

df
  .transform(withCountry())
  .transform(withHemisphere())
  .show()
```

This DataFrame is printed to the console:

```
+------------+------------+-------------------+
|        city|     country|         hemisphere|
+------------+------------+-------------------+
|     Calgary|      Canada|Northern Hemisphere|
|Buenos Aires|   Argentina|Southern Hemisphere|
|   Cape Town|South Africa|Southern Hemisphere|
+------------+------------+-------------------+
```

The `withCountry()` and `withHemisphere()` transformations are order dependent because `withCountry()` must be run before `withHemisphere()`. If `withHemisphere()` is run first, the code will error out.

```scala
df
  .transform(withHemisphere())
  .transform(withCountry())
  .show()
```

Here is the error message:

```
org.apache.spark.sql.AnalysisException: cannot resolve 'country' given input columns: [city];;

'Project \[city#1, CASE WHEN ('country = Canada) THEN Northern Hemisphere WHEN ('country = Argentina) THEN Southern Hemisphere WHEN ('country = South Africa) THEN Southern Hemisphere END AS hemisphere#4\]
```

## Intelligently adding dependencies based on DataFrame columns

Let's write a `withHemisphereRefactored()` method that intelligently runs the `withCountry()` method if the underlying DataFrame does not contain a `country` column.

```scala
def withHemisphereRefactored()(df: DataFrame): DataFrame = {
  if (df.schema.fieldNames.contains("country")) {
    df.withColumn(
      "hemisphere",
      when(col("country") === "Canada", "Northern Hemisphere")
        .when(col("country") === "Argentina", "Southern Hemisphere")
        .when(col("country") === "South Africa", "Southern Hemisphere")
    )
  } else {
    df
      .transform(withCountry())
      .withColumn(
      "hemisphere",
      when(col("country") === "Canada", "Northern Hemisphere")
        .when(col("country") === "Argentina", "Southern Hemisphere")
        .when(col("country") === "South Africa", "Southern Hemisphere")
    )
  }
}
```

We can run `withHemisphereRefactored()` directly on a DataFrame that only contains a `city` column and it will now work:

```scala
val df = spark.createDF(
  List(
    ("Calgary"),
    ("Buenos Aires"),
    ("Cape Town")
  ), List(
    ("city", StringType, true)
  )
)

df
  .transform(withHemisphereRefactored())
  .show()
```

We can use the `explain()` method to inspect the physical plan and see how this code intelligently adds the `country` column:

```
Project [city#19, CASE
  WHEN (city#19 = Calgary) THEN Canada
  WHEN (city#19 = Buenos Aires) THEN Argentina
  WHEN (city#19 = Cape Town) THEN South Africa
END AS country#22,
CASE
  WHEN (CASE WHEN (city#19 = Calgary) THEN Canada WHEN (city#19 = Buenos Aires) THEN Argentina WHEN (city#19 = Cape Town) THEN South Africa END = Canada) THEN Northern Hemisphere
  WHEN (CASE WHEN (city#19 = Calgary) THEN Canada WHEN (city#19 = Buenos Aires) THEN Argentina WHEN (city#19 = Cape Town) THEN South Africa END = Argentina) THEN Southern Hemisphere
  WHEN (CASE WHEN (city#19 = Calgary) THEN Canada WHEN (city#19 = Buenos Aires) THEN Argentina WHEN (city#19 = Cape Town) THEN South Africa END = South Africa) THEN Southern Hemisphere
END AS hemisphere#26]

```

`withHemisphereRefactored()` does not run the `withCountry()` code if the DataFrame already contains a `country` column. We don't want to write code that unnecessarily executes functions because that would slow down our codebase. Let's verify this by running `withHemisphereRefactored()` on a DataFrame with a `country` column and inspecting the physical plan.

```scala
val df = spark.createDF(
  List(
    ("Canada"),
    ("Argentina"),
    ("South Africa")
  ), List(
    ("country", StringType, true)
  )
)

df
  .transform(withHemisphereRefactored())
  .show()
```

```
+------------+-------------------+
|     country|         hemisphere|
+------------+-------------------+
|      Canada|Northern Hemisphere|
|   Argentina|Southern Hemisphere|
|South Africa|Southern Hemisphere|
+------------+-------------------+
```

Let's inspect the physical plan to confirm that `withHemisphereRefactored()` is not unnecessarily running the `withCountry()` related logic when the DataFrame already contains a `country` column.

```scala
df
  .transform(withHemisphereRefactored())
  .explain()
```

```
Project [country#37, CASE
  WHEN (country#37 = Canada) THEN Northern Hemisphere
  WHEN (country#37 = Argentina) THEN Southern Hemisphere
  WHEN (country#37 = South Africa) THEN Southern Hemisphere
END AS hemisphere#48]

```

Let's leverage the [spark-daria](https://github.com/MrPowers/spark-daria) `containsColumn()` DataFrame extension and abstract the transformation logic to a subfunction to express this code more elegantly:

```scala
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

def withHemisphereElegant()(df: DataFrame): DataFrame = {
  def hemTransformation()(df: DataFrame): DataFrame = {
    df.withColumn(
      "hemisphere",
      when(col("country") === "Canada", "Northern Hemisphere")
        .when(col("country") === "Argentina", "Southern Hemisphere")
        .when(col("country") === "South Africa", "Southern Hemisphere")
    )
  }

  if (df.containsColumn("country")) {
    df.transform(hemTransformation())
  } else {
    df
      .transform(withCountry())
      .transform(hemTransformation())
  }
}
```

You can also pass the `containsColumn()` method a `StructField` argument if you'd like to validate the column name, type, and nullable property.

```
df.containsColumn(StructField("country", StringType, true))
```

## Taking it to the next level

We can leverage the [spark-daria](https://github.com/MrPowers/spark-daria) `CustomTransform` case class to encapsulate the DataFrame columns that are required, added, and removed by each DataFrame transformation function.

Here's the `CustomTransform` case class definition:

```scala
case class CustomTransform(
  transform: (DataFrame => DataFrame),
  requiredColumns: Seq[String] = Seq.empty[String],
  addedColumns: Seq[String] = Seq.empty[String],
  removedColumns: Seq[String] = Seq.empty[String]
)
```

Let's define `countryCT` and `hemisphereCT` objects:

```scala
val countryCT = new CustomTransform(
  transform = withCountry(),
  requiredColumns = Seq("city"),
  addedColumns = Seq("country")
)

val hemisphereCT = new CustomTransform(
  transform = withHemisphere(),
  requiredColumns = Seq("country"),
  addedColumns = Seq("hemisphere")
)
```

We can immediately identify how `countryCT` and `hemisphereCT` are related - the column that's added by `countryCT` is the same as the column that's required by `hemisphereCT`.

Custom transformation objects can be executed by a `trans()` method that's also defined in [spark-daria](https://github.com/MrPowers/spark-daria).

```scala
import com.github.mrpowers.spark.daria.sql.DataFrameExt._

val df = spark.createDF(
  List(
    ("Calgary"),
    ("Buenos Aires"),
    ("Cape Town")
  ), List(
    ("city", StringType, true)
  )
)

df
  .trans(countryCT)
  .trans(hemisphereCT)
  .show()
```

```
+------------+------------+-------------------+
|        city|     country|         hemisphere|
+------------+------------+-------------------+
|     Calgary|      Canada|Northern Hemisphere|
|Buenos Aires|   Argentina|Southern Hemisphere|
|   Cape Town|South Africa|Southern Hemisphere|
+------------+------------+-------------------+
```

The [spark-daria](https://github.com/MrPowers/spark-daria) `composeTrans()` method runs a list of CustomTransforms intelligently - the DataFrame transformation is only run if the columns that will be added by the transformation are missing from the DataFrame.

```scala
df.composeTrans(List(countryCT, hemisphereCT))
```

## A path forward

A Spark library can be modeled as a directed acyclic graph (DAG) of `CustomTransform` objects.

This would allow users to specify the columns they want added to a DataFrame without worrying about running a bunch of functions in a specific order. The library would be responsible for intelligently running the required transformations to provide the user their desired DataFrame.

## Next steps

Spark libraries can quickly grow to 30+ order dependent functions that are difficult to use. When order dependencies are rampant in a codebase, users are forced to dig through the source code and often revert to a frustrating trial and error development workflow.

Modeling transformations as `CustomTransform` objects forces you to document the columns that are added and removed by each transformation and will create a codebase that's easier to parse.

Writing transformations that intelligently call other transformations if required columns are missing is another way to make it easier for users to leverage your public interface.

Modeling a library of `CustomTransform` objects as a DAG is the holy grail for a library public interface. The library will be responsible for running functions in a certain order and making sure the Spark physical plan that's generated is optimal. This will provide users a better library experience.
