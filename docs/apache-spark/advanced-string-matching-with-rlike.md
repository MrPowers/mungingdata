---
title: "Advanced String Matching with Spark's rlike Method"
date: "2018-04-06"
categories: 
  - "apache-spark"
---

# Advanced String Matching with Spark's rlike Method

The Spark `rlike` method allows you to write powerful string matching algorithms with regular expressions (regexp).

This blog post will outline tactics to detect strings that match multiple different patterns and how to abstract these regular expression patterns to CSV files.

[Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) is the best way to learn how to use regular expressions when working with Spark StringType columns.

## Substring matching

Let’s create a DataFrame and use `rlike` to identify all strings that contain the substring `"cat"`.

```
val df = List(
  ("the cat and the hat"),
  ("i love your cat"),
  ("dogs are cute"),
  ("pizza please")
).toDF("phrase")

df
  .withColumn(
    "contains_cat",
    col("phrase").rlike("cat")
  )
  .show(truncate = false)
```

```
+-------------------+------------+
|phrase             |contains_cat|
+-------------------+------------+
|the cat and the hat|true        |
|i love your cat    |true        |
|dogs are cute      |false       |
|pizza please       |false       |
+-------------------+------------+
```

There is nothing special about this example and if you’re only looking to match a single substring, it’s better to use `contains` than `rlike`.

Let’s rework this code to detect all strings that contain the substrings `"cat"` or `"dog"`.

```
df
  .withColumn(
    "contains_cat_or_dog",
    col("phrase").rlike("cat|dog")
  )
  .show(truncate = false)
```

```
+-------------------+------------+
|phrase             |contains_cat|
+-------------------+------------+
|the cat and the hat|true        |
|i love your cat    |true        |
|dogs are cute      |true        |
|pizza please       |false       |
+-------------------+------------+
```

We can refactor this code by storing the animals in a list and concatenating them as a pipe delimited string for the `rlike` method.

```
val animals = List("cat", "dog")

df
  .withColumn(
    "contains_cat_or_dog",
    col("phrase").rlike(animals.mkString("|"))
  )
  .show(truncate = false)
```

```
+-------------------+-------------------+
|phrase             |contains_cat_or_dog|
+-------------------+-------------------+
|the cat and the hat|true               |
|i love your cat    |true               |
|dogs are cute      |true               |
|pizza please       |false              |
+-------------------+-------------------+
```

## Matching strings that start with or end with substrings

Let’s create a new DataFrame and match all strings that begin with the substring `"i like"` or `"i want"`.

```
val df = List(
  ("i like tacos"),
  ("i want love"),
  ("pie is what i like"),
  ("pizza pizza"),
  ("you like pie")
).toDF("phrase")

df
  .withColumn(
    "starts_with_desire",
    col("phrase").rlike("^i like|^i want")
  )
  .show(truncate = false)
```

```
+------------------+------------------+
|phrase            |starts_with_desire|
+------------------+------------------+
|i like tacos      |true              |
|i want love       |true              |
|pie is what i like|false             |
|pizza pizza       |false             |
|you like pie      |false             |
+------------------+------------------+
```

We can also append an `ends_with_food` column using regular expresssions.

```
val foods = List("tacos", "pizza", "pie")
df
  .withColumn(
    "ends_with_food",
    col("phrase").rlike(foods.map(_ + "$").mkString("|"))
  )
  .show(truncate = false)
```

```
+------------------+--------------+
|phrase            |ends_with_food|
+------------------+--------------+
|i like tacos      |true          |
|i want love       |false         |
|pie is what i like|false         |
|pizza pizza       |true          |
|you like pie      |true          |
+------------------+--------------+
```

## Matching strings that contain regex characters

Suppose we want to find all the strings that contain the substring `"fun|stuff"`. We don’t want all strings that contain fun or stuff — we want all strings that match the substring `fun|stuff` exactly.

The approch we’ve been using won’t work as desired, because it will match all strings that contain `fun` or `stuff`.

```
df
  .withColumn(
    "contains_fun_pipe_stuff",
    col("phrase").rlike("fun|stuff")
  )
  .show(truncate = false)
```

```
+------------------+-----------------------+
|phrase            |contains_fun_pipe_stuff|
+------------------+-----------------------+
|fun|stuff         |true                   |
|dancing is fun    |true                   |
|you have stuff    |true                   |
|where is fun|stuff|true                   |
|something else    |false                  |
+------------------+-----------------------+
```

We can use the `java.util.regex.Pattern` to quote the regular expression and properly match the `fun|stuff` string exactly.

```
import java.util.regex.Pattern

df
  .withColumn(
    "contains_fun_pipe_stuff",
    col("phrase").rlike(Pattern.quote("fun|stuff"))
  )
  .show(truncate = false)
```

```
+------------------+-----------------------+
|phrase            |contains_fun_pipe_stuff|
+------------------+-----------------------+
|fun|stuff         |true                   |
|dancing is fun    |false                  |
|you have stuff    |false                  |
|where is fun|stuff|true                   |
|something else    |false                  |
+------------------+-----------------------+
```

`Pattern.quote("fun|stuff")` returns `"\Qfun|stuff\E"`.

The `Pattern.quote()` method wraps the string in `\Q` and `\E` to turn the text into a regexp literal, as described in this Stackoverflow thread.

Alternatively, we can escape the pipe character in the regexp with `\\`.

```
df
  .withColumn(
    "contains_fun_pipe_stuff",
    col("phrase").rlike("fun\\|stuff")
  )
  .show(truncate = false)
```

```
+------------------+-----------------------+
|phrase            |contains_fun_pipe_stuff|
+------------------+-----------------------+
|fun|stuff         |true                   |
|dancing is fun    |false                  |
|you have stuff    |false                  |
|where is fun|stuff|true                   |
|something else    |false                  |
+------------------+-----------------------+
```

## Abstracting multiple pattern match criteria to CSV files

You may want to store multiple string matching criteria in a separate CSV file rather than directly in the code. Let’s create a CSV that matches all strings that start with `coffee`, end with `bread` or contain `nice|person`. Here’s the content of the `random_matches.csv` file.

```
^coffee
bread$
nice\\|person
```

The pipe character in the CSV file needs to be escaped with `\\`.

Here’s the how to use the CSV file to match strings that match at least one of the regexp criteria.

```
val df = List(
  ("coffee is good"),
  ("i need coffee"),
  ("bread is good"),
  ("i need bread"),
  ("you're a nice|person"),
  ("that is nice")
).toDF("phrase")

val weirdMatchesPath = new java.io.File(s"./src/test/resources/random_matches.csv").getCanonicalPath

val weirdMatchesDF = spark
  .read
  .option("header", "true")
  .option("charset", "UTF8")
  .csv(weirdMatchesPath)

val matchString = DataFrameHelpers.columnToArray[String](
  weirdMatchesDF,
  "match_criteria"
).mkString("|")

df
  .withColumn(
    "weird_matches",
    col("phrase").rlike(matchString)
  )
  .show(truncate = false)
```

```
+--------------------+-------------+
|phrase              |weird_matches|
+--------------------+-------------+
|coffee is good      |true         |
|i need coffee       |false        |
|bread is good       |false        |
|i need bread        |true         |
|you're a nice|person|true         |
|that is nice        |false        |
+--------------------+-------------+
```

## Next steps

> Some people, when confronted with a problem, think “I know, I’ll use regular expressions.” Now they have two problems. — Jamie Zawinski

Using regular expressions is controversial to say the least. Regular expressions are powerful tools for advanced string matching, but can create code bases that are difficult to maintain. Thoroughly testing regular expression behavior and documenting the expected results in comments is vital, especially when multiple regexp criteria are chained together.

Spark’s `rlike` method allows for powerful string matching. You’ll be rewarded with great results if you can learn to use these tools effectively.
