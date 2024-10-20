---
title: "Avoiding Dots / Periods in PySpark Column Names"
date: "2020-07-22"
categories: 
  - "pyspark"
---

Dots / periods in PySpark column names need to be escaped with backticks which is tedious and error-prone.

This blog post explains the errors and bugs you're likely to see when you're working with dots in column names and how to eliminate dots from column names.

## Simple example

Let's create a DataFrame with `country.name` and `continent` columns.

```
df = spark.createDataFrame(
    [("china", "asia"), ("colombia", "south america")],
    ["country.name", "continent"]
)
df.show()
```

```
+------------+-------------+
|country.name|    continent|
+------------+-------------+
|       china|         asia|
|    colombia|south america|
+------------+-------------+
```

Here's the error message you'll get when you select `country.name` without backticks: `df.select("country.name")`.

```
pyspark.sql.utils.AnalysisException: "cannot resolve '`country.name`' given input columns: [country.name, continent];;\n'Project ['country.name]\n+- LogicalRDD [country.name#0, continent#1], false\n"
```

Here's how you need to select the column to avoid the error message: `df.select("`country.name`")`.

Having to remember to enclose a column name in backticks every time you want to use it is really annoying. It's also error prone.

Dot notation is used to fetch values from fields that are nested. When you use `country.name` without backticks, Spark thinks you're trying to fetch the `name` field that's nested in the `country` column. Let's create another example with a nested schema to explore the dot syntax in more detail.

## Nested schemas

Create a DataFrame with a nested schema:

```
schema = StructType([
    StructField("person.name", StringType(), True),
    StructField("person", StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)]))
])
data = [
    ("charles", Row("chuck", 42)),
    ("lawrence", Row("larry", 73))
]
df = spark.createDataFrame(data, schema)
df.show()
```

```
+-----------+-----------+
|person.name|     person|
+-----------+-----------+
|    charles|[chuck, 42]|
|   lawrence|[larry, 73]|
+-----------+-----------+
```

In this example, `"person.name"` refers to the `name` field that's nested in the `person` column and ``"`person.name`"`` refers to the `person.name` column. Let's run a `select()` query to verify:

```
cols = ["person", "person.name", "`person.name`"]
df.select(cols).show()
```

```
+-----------+-----+-----------+
|     person| name|person.name|
+-----------+-----+-----------+
|[chuck, 42]|chuck|    charles|
|[larry, 73]|larry|   lawrence|
+-----------+-----+-----------+
```

This code is error prone because you might forget backticks and get the wrong result. It's best to eliminate dots from column names.

## Replaces dots with underscores in column names

Here's how to replace dots with underscores in DataFrame column names.

```
clean_df = df.toDF(*(c.replace('.', '_') for c in df.columns))
clean_df.show()
```

```
+-----------+-----------+
|person_name|     person|
+-----------+-----------+
|    charles|[chuck, 42]|
|   lawrence|[larry, 73]|
+-----------+-----------+
```

You can access the `person_name` and `person.name` fields in `clean_df` without worrying about backticks.

```
clean_df.select("person_name", "person.name", "person.age").show()

+-----------+-----+---+
|person_name| name|age|
+-----------+-----+---+
|    charles|chuck| 42|
|   lawrence|larry| 73|
+-----------+-----+---+
```

Renaming column names in PySpark is an important topic that's [covered in this blog post](https://mungingdata.com/pyspark/rename-multiple-columns-todf-withcolumnrenamed/). The design patterns covered in the blog post will make you a better PySpark programmer. Study it carefully.

## Conclusion

Dots in PySpark column names can cause headaches, especially if you have a complicated codebase and need to add backtick escapes in a lot of different places.

It's easier to replace the dots in column names with underscores, or another character, so you don't need to worry about escaping.

Avoid writing out column names with dots to disk. Don't make youre downstream users deal with backtick escaping either.

If you'd like to continue learning about PySpark, check out [this blog post on chaining custom transformations](https://mungingdata.com/apache-spark/chaining-custom-dataframe-transformations/). Chaining transformations is essential for creating a clean PySpark codebase.
