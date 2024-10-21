---
title: "Spark Column Equality"
date: "2020-03-10"
categories: 
  - "apache-spark"
---

# Spark Column Equality

The term "column equality" refers to two different things in Spark:

1. When a column is equal to a particular value (typically when filtering)
2. When all the values in two columns are equal for all rows in the dataset (especially common when testing)

This blog post will explore both types of Spark column equality.

## Column equality for filtering

Suppose you have a DataFrame with `team_name`, `num_championships`, and `state` columns.

Here's how you can filter to only show the teams from TX (short for Texas).

```
df.filter(df("state") === "TX")
```

Here's a sample dataset that you can paste into a [Spark console](https://mungingdata.com/apache-spark/using-the-console/) to verify this result yourself.

```
val df = Seq(
  ("Rockets", 2, "TX"),
  ("Warriors", 6, "CA"),
  ("Spurs", 5, "TX"),
  ("Knicks", 2, "NY")
).toDF("team_name", "num_championships", "state")
```

Let's filter the DataFrame and make sure it only includes the teams from TX.

```
df.filter(df("state") === "TX").show()

+---------+-----------------+-----+
|team_name|num_championships|state|
+---------+-----------------+-----+
|  Rockets|                2|   TX|
|    Spurs|                5|   TX|
+---------+-----------------+-----+
```

[Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) is the best way to learn about filtering, including the main pitfall to avoid when filtering. Read the book to filter effectively.

## More on Spark's Column class

You'll use the Spark Column class all the time and it's good to [understand how it works](https://mungingdata.com/apache-spark/column-methods/).

Here's the method signature for the `===` method defined in the Column class.

```
def ===(other: Any): Column
```

The `===` takes Any object as an argument and returns a Column.

In `df("state") === "TX"`, the `===` method is supplied a string argument. It can also be supplied a `Column` argument.

```
import org.apache.spark.sql.functions.lit

df.filter(df("state") === lit("TX")).show()
```

Scala methods can be invoked with spaces instead of dot notation. This is an example of [syntactic sugar](https://en.wikipedia.org/wiki/Syntactic_sugar).

We can use `df("state").===(lit("TX"))` to avoid syntactic sugar and invoke the `===` method with standard dot notation.

```
df.filter(df("state").===("TX")).show()

+---------+-----------------+-----+
|team_name|num_championships|state|
+---------+-----------------+-----+
|  Rockets|                2|   TX|
|    Spurs|                5|   TX|
+---------+-----------------+-----+
```

We could also use the `equalTo()` Column method that behaves like `===`:

```
df.filter(df("state").equalTo("TX")).show()
```

Study the Column methods to become a better Spark programmer!

## Naive Column Equality

Let's create a DataFrame with `num` and `is_even_hardcoded` columns.

```
val df = Seq(
  (2, true),
  (6, true),
  (5, false)
).toDF("num", "is_even_hardcoded")
```

```
df.show()

+---+-----------------+
|num|is_even_hardcoded|
+---+-----------------+
|  2|             true|
|  6|             true|
|  5|            false|
+---+-----------------+
```

Let's create a [custom DataFrame transformation](https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/) called `isEven` that'll return `true` if the number is even and `false` otherwise.

```
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def isEven(inputColName: String, outputColName: String)(df: DataFrame) = {
  df.withColumn(outputColName, col(inputColName) % 2 === lit(0))
}
```

Check out [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) if this blog post is moving too fast and you need a gradual introduction to topics like DataFrame transformation functions.

Let's run the `isEven()` function and visually verify that the hardcoded and programatic results are the same.

```
df.transform(isEven("num", "is_even")).show()

+---+-----------------+-------+
|num|is_even_hardcoded|is_even|
+---+-----------------+-------+
|  2|             true|   true|
|  6|             true|   true|
|  5|            false|  false|
+---+-----------------+-------+
```

Visually verifying column equality isn't good for big datasets or automated processes.

Let's get this full DataFrame stored in a new variable, so we don't need to keep on running the `isEven` function.

```
val fullDF = df.transform(isEven("num", "is_even"))
```

We can use the `ColumnComparer` trait defined in [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests) to verify column equality.

```
import com.github.mrpowers.spark.fast.tests.ColumnComparer

assertColEquality(df, "is_even_hardcoded", "is_even")
```

When you're writing unit tests, you'll definitely want to use the spark-fast-tests library.

Let's hack together some code that'll return `true` if two columns are equal.

```
def areColumnsEqual(df: DataFrame, colName1: String, colName2: String) = {
  val elements = df
    .select(colName1, colName2)
    .collect()
  val c1 = elements.map(_(0))
  val c2 = elements.map(_(1))
  c1.sameElements(c2)
}

areColumnsEqual(fullDF, "is_even_hardcoded", "is_even") // true
```

This function doesn't explain all the edge cases, but it's a good start!

## Column equality with ===

We can also evaluate column equality by comparing both columns with the `===` operator and making sure all values evaluate to `true`.

As a reminder, here are the contents of `fullDF`:

```
fullDF.show()

+---+-----------------+-------+
|num|is_even_hardcoded|is_even|
+---+-----------------+-------+
|  2|             true|   true|
|  6|             true|   true|
|  5|            false|  false|
+---+-----------------+-------+
```

Let's append a column to `fullDF` that returns `true` if `is_even_hardcoded` and `is_even` are equal:

```
fullDF
  .withColumn("are_cols_equal", $"is_even_hardcoded" === $"is_even")
  .show()

+---+-----------------+-------+--------------+
|num|is_even_hardcoded|is_even|are_cols_equal|
+---+-----------------+-------+--------------+
|  2|             true|   true|          true|
|  6|             true|   true|          true|
|  5|            false|  false|          true|
+---+-----------------+-------+--------------+
```

Let's write a function to verify that all the values in a given column are true.

```
def areAllRowsTrue(df: DataFrame, colName: String) = {
  val elements = df
    .select(colName)
    .collect()
  val c1 = elements.map(_(0))
  c1.forall(_ == true)
}
```

Let's verify that `areAllRowsTrue` returns `true` for the `are_cols_equal` column.

```
areAllRowsTrue(
  fullDF.withColumn("are_cols_equal", $"is_even_hardcoded" === $"is_even"),
  "are_cols_equal"
) // true
```

This `===` approach unfortunately doesn't work for all column types.

## ArrayType Column Equality

Let's create a `numbersDF` with two ArrayType columns that contain integers.

```
val numbersDF = Seq(
  (Seq(1, 2), Seq(1, 2)),
  (Seq(3, 3), Seq(3, 3)),
  (Seq(3, 3), Seq(6, 6))
).toDF("nums1", "nums2")
```

We can use `===` to assess ArrayType column equality.

```
numbersDF
  .withColumn("nah", $"nums1" === $"nums2")
  .show()

+------+------+-----+
| nums1| nums2|  nah|
+------+------+-----+
|[1, 2]|[1, 2]| true|
|[3, 3]|[3, 3]| true|
|[3, 3]|[6, 6]|false|
+------+------+-----+
```

Let's see if `===` works for nested arrays.

Start by making a `fakeDF` DataFrame with two nested array columns.

```
val fakeDF = Seq(
  (Seq(Seq(1, 2)), Seq(Seq(1, 2))),
  (Seq(Seq(1, 3)), Seq(Seq(1, 2)))
).toDF("nums1", "nums2")
```

The code below confirms that the `===` operator can handle deep column comparisons gracefully.

```
fakeDF
  .withColumn("nah", $"nums1" === $"nums2")
  .show()

+--------+--------+-----+
|   nums1|   nums2|  nah|
+--------+--------+-----+
|[[1, 2]]|[[1, 2]]| true|
|[[1, 3]]|[[1, 2]]|false|
+--------+--------+-----+
```

But watch out, the `===` operator doesn't work for all complex column types.

## MapType Column Equality

The `===` operator does not work for MapType columns.

```
val mapsDF = Seq(
  (Map("one" -> 1), Map("one" -> 1))
).toDF("m1", "m2")
```

```
mapsDF.withColumn("maps_equal", $"m1" === $"m2").show()

org.apache.spark.sql.AnalysisException: cannot resolve '(m1 = m2)' due to data type mismatch: EqualTo does not support ordering on type map<string,int>;;
Project [m1#373, m2#374, (m1#373 = m2#374) AS maps_equal#384]
 Project [_1#370 AS m1#373, _2#371 AS m2#374]
    LocalRelation [_1#370, _2#371]

  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$3.applyOrElse(CheckAnalysis.scala:115)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$3.applyOrElse(CheckAnalysis.scala:107)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:278)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:278)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:277)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:275)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeNode.scala:275)
  at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4.apply(TreeNode.scala:326)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:324)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:275)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:93)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpressionsUp$1.apply(QueryPlan.scala:93)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:105)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$1.apply(QueryPlan.scala:105)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:104)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:116)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1$2.apply(QueryPlan.scala:121)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
  at scala.collection.immutable.List.foreach(List.scala:392)
  at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
  at scala.collection.immutable.List.map(List.scala:296)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:121)
  at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$2.apply(QueryPlan.scala:126)
  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:126)
  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:93)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:107)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:85)
  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)
  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:85)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:95)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$$anonfun$executeAndCheck$1.apply(Analyzer.scala:108)
  at org.apache.spark.sql.catalyst.analysis.Analyzer$$anonfun$executeAndCheck$1.apply(Analyzer.scala:105)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.markInAnalyzer(AnalysisHelper.scala:201)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:105)
  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:57)
  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:55)
  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:47)
  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:79)
  at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$withPlan(Dataset.scala:3407)
  at org.apache.spark.sql.Dataset.select(Dataset.scala:1335)
  at org.apache.spark.sql.Dataset.withColumns(Dataset.scala:2253)
  at org.apache.spark.sql.Dataset.withColumn(Dataset.scala:2220)
  ... 51 elided
```

We need to use different tactics for MapType column equality.

The Scala `==` operator can successfully compare maps:

```
Map("one" -> 1) == Map("one" -> 1) // true
```

The `sameElements` Scala method also works:

```
Map("one" -> 1) sameElements Map("one" -> 1)
```

Recall that `sameElements` was used in the `areColumnsEqual` method we defined earlier:

```
def areColumnsEqual(df: DataFrame, colName1: String, colName2: String) = {
  val elements = df
    .select(colName1, colName2)
    .collect()
  val c1 = elements.map(_(0))
  val c2 = elements.map(_(1))
  c1.sameElements(c2)
}
```

So `areColumnsEqual` will also work for comparing MapType columns:

```
areColumnsEqual(mapsDF, "m1", "m2") // true
```

## StructType Column Equality

That's a whole new can of worms!!

## Testing with Column Equality

Column equality is useful when writing unit tests.

[Testing Spark Applications](https://leanpub.com/testing-spark/) is the best way to learn how to test your Spark code. This section provides a great introduction, but you should really read \[the book\]([Testing Spark Applications](https://leanpub.com/testing-spark/)) if you'd like to accelerate your learning process.

You'll want to leverage a library like [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) with column comparison methods, so you don't need to write them yourself.

Let's look at how [spark-daria](https://github.com/MrPowers/spark-daria) uses the spark-fast-tests `assertColumnEquality` method to test the `removeNonWordCharacters()` function that removes all the non-word characters from a string.

```
def removeNonWordCharacters(col: Column): Column = {
  regexp_replace(col, "[^\\w\\s]+", "")
}
```

Let's write a test to make sure the `removeNonWordCharacters` converts `"Bruce &&**||ok88"` to `"Bruce ok88"`, `"55 oba&&&ma"` to `"55 obama"`, etc.

Here's the test:

```
import utest._

import com.github.mrpowers.spark.fast.tests.ColumnComparer

object FunctionsTest extends TestSuite with ColumnComparer with SparkSessionTestWrapper {
  val tests = Tests {
    'removeNonWordCharacters - {
      "removes all non-word characters from a string, excluding whitespace" - {
        val df = spark
          .createDF(
            List(
              ("Bruce &&**||ok88", "Bruce ok88"),
              ("55    oba&&&ma", "55    obama"),
              ("  ni!!ce  h^^air person  ", "  nice  hair person  "),
              (null, null)
            ),
            List(
              ("some_string", StringType, true),
              ("expected", StringType, true)
            )
          )
          .withColumn(
            "some_string_remove_non_word_chars",
            functions.removeNonWordCharacters(col("some_string"))
          )

        assertColumnEquality(
          df,
          "some_string_remove_non_word_chars",
          "expected"
        )
      }
    }
  }
}
```

We hardcode the expected result in the DataFrame and add the `some_string_remove_non_word_chars` column by running the `removeNonWordCharacters()` function.

The `assertColumnEquality` method that's defined in spark-fast-tests verifies the `some_string_remove_non_word_chars` and `expected` are equal.

This is how you should design most of your Spark unit tests.

## Conclusion

Spark column equality is a surprisingly deep topic… we haven't even covered all the edge cases!

Make sure you understand how column comparisons work at a high level.

Use spark-fast-tests to write elegant tests and abstract column comparison details from your codebase.

Studying the spark-fast-tests codebase is a great way to learn more about Spark!
