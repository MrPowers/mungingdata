---
title: "DataFrames in Go with gota, qframe, and dataframe-go"
date: "2020-03-30"
categories: 
  - "go"
---

# DataFrames in Go with gota, qframe, and dataframe-go

Go has great DataFrame libraries that let you easily manipulate data that's stored in CSV files and databases.

Working with CSV files directly can be burdensome. DataFrames are easier because they provide data manipulation and grouping functionality natively.

There are three popular Go libraries:

- [gota](https://github.com/go-gota/gota): started in January 2016
- [qframe](https://github.com/tobgu/qframe): started November 2016
- [dataframe-go](https://github.com/rocketlaunchr/dataframe-go): started in October 2018

This blog post shows you how to perform basic operations with each library so we can see which API is the cleanest.

## Why are DataFrames important for Go

Go is a great language for ETL.

Developers coming from other languages / frameworks love using DataFrames.

Web development is the biggest Go domain, but there is still a nice chunk of developers [that use Go for data science](https://blog.golang.org/survey2018-results).

## Initial impressions

- qframe has the most elegant API and [performs faster than gota in all benchmarks](https://github.com/tobgu/qbench#summary). We need to add [dataframe-go benchmarks](https://github.com/tobgu/qbench/issues/2).
- no native support the Parquet file format yet
- no support for [Arrow](https://arrow.apache.org/) yet

None of the libraries have stable APIs yet. Let's help add key features and move these libraries towards 1.0 releases!

## qframes

Suppose you have a `data/example.csv` file with the following contents:

```
first_name,favorite_number
matthew,23
daniel,8
allison,42
david,18
```

Let's open the CSV file and read it into a DataFrame:

```
csvfile, err := os.Open("data/example.csv")
if err != nil {
    log.Fatal(err)
}

f := qframe.ReadCSV(csvfile)
```

We can view the data with `fmt.Println`.

```
fmt.Println(f)

first_name(s) favorite_number(i)
------------- ------------------
      matthew                 23
       daniel                  8
      allison                 42
        david                 18
```

The `(s)` next to `first_name` means it's a string column. The `(i)` next to `favorite_number` means it's an integer column.

qframe intelligently infers the schema (it doesn't blindly assume all columns are strings).

Let's add an `is_even` column to the DataFrame that contains `true` if `favorite_number` is even.

```
f = f.Apply(
    qframe.Instruction{
        Fn:      isEven,
        DstCol:  "is_even",
        SrcCol1: "favorite_number"})
```

Let's check that `is_even` has been added:

```
fmt.Println(f)

first_name(s) favorite_number(i) is_even(b)
------------- ------------------ ----------
      matthew                 23      false
       daniel                  8       true
      allison                 42       true
        david                 18       true
```

Filter out all the rows that do not have `is_even` set to `true`.

```
newF := f.Filter(qframe.Filter{Column: "is_even", Comparator: "=", Arg: true})
```

Let's take a look at the filtered DataFrame:

```
fmt.Println(newF)

first_name(s) favorite_number(i) is_even(b)
------------- ------------------ ----------
       daniel                  8       true
      allison                 42       true
        david                 18       true
```

Let's write out this result to a CSV file:

```
file, err := os.Create("tmp/qframe_main_ouput.csv")
if err != nil {
    log.Fatal(err)
}
newF.ToCSV(file)
```

The `tmp/qframe_main_ouput.csv` file will look like this:

```
first_name,favorite_number,is_even
daniel,8,true
allison,42,true
david,18,true
```

qframe is easy to work with and has a great public interface.

## rocketlaunchr dataframe-go

Let's use the same dataset and run the same operations with dataframe-go.

Read the CSV into a DataFrame.

```
ctx := context.TODO()

csvfile, err := os.Open("data/example.csv")
if err != nil {
    log.Fatal(err)
}

df, err := imports.LoadFromCSV(ctx, csvfile, imports.CSVLoadOptions{
    DictateDataType: map[string]interface{}{
        "first_name":      "",       // specify this column as string
        "favorite_number": int64(0), // specify this column as int64
    }})
```

View the contents of the `df`:

```
fmt.Print(df.Table())

+-----+------------+-----------------+
|     | FIRST NAME | FAVORITE NUMBER |
+-----+------------+-----------------+
| 0:  |  matthew   |       23        |
| 1:  |   daniel   |        8        |
| 2:  |  allison   |       42        |
| 3:  |   david    |       18        |
+-----+------------+-----------------+
| 4X2 |   STRING   |      INT64      |
+-----+------------+-----------------+
```

The `Print` output is a little confusing because the column names are actually `first_name` and `favorite_number`.

Points to note when reading CSVs with dataframe-go:

- Schema inference is not supported, so we need to use `DictateDataType` to specify that `favorite_number` is an `int64` column
- We need to create a context to use the `LoadFromCSV` method

Let's multiply the `favorite_number` column by two:

```
s := df.Series[1]

applyFn := dataframe.ApplySeriesFn(func(val interface{}, row, nRows int) interface{} {
    return 2 * val.(int64)
})

dataframe.Apply(ctx, s, applyFn, dataframe.FilterOptions{InPlace: true})
```

Let's view the contents of `df`:

```
fmt.Print(df.Table())

+-----+------------+-----------------+
|     | FIRST NAME | FAVORITE NUMBER |
+-----+------------+-----------------+
| 0:  |  matthew   |       46        |
| 1:  |   daniel   |       16        |
| 2:  |  allison   |       84        |
| 3:  |   david    |       36        |
+-----+------------+-----------------+
| 4X2 |   STRING   |      INT64      |
+-----+------------+-----------------+
```

Some notes on this code:

- `df.Series[1]` depends on the `favorite_number` column being the second column in the DataFrame. If the columns are reordered, the code will error out or double another column.
- The empty interface is used in a couple of spots

I couldn't figure out filtering easily.

Here's the filtering example in the README:

```
filterFn := dataframe.FilterDataFrameFn(func(vals map[interface{}]interface{}, row, nRows int) (dataframe.FilterAction, error) {
    if vals["title"] == nil {
        return dataframe.DROP, nil
    }
    return dataframe.KEEP, nil
})

seniors, _ := dataframe.Filter(ctx, df, filterFn)
```

We were able to filter a qframe DataFrame with only a single line of code: `f.Filter(qframe.Filter{Column: "is_even", Comparator: "=", Arg: true})`. dataframe-go is more verbose.

The dataframe-go maintainers are great to work with. Hopefully we can [add dataframe-go](https://github.com/tobgu/qbench/issues/2) to [qbench](https://github.com/tobgu/qbench), so we can compare the gota, qframe, and dataframe-go performance side-by-side.

## gota

Let's load the `data/example.csv` file into a gota DataFrame:

```
csvfile, err := os.Open("data/example.csv")
if err != nil {
    log.Fatal(err)
}

df := dataframe.ReadCSV(csvfile)
```

We can view the DataFrame contents with `Println`:

```
fmt.Println("df: ", df)

    first_name favorite_number
 0: matthew    23
 1: daniel     8
 2: allison    42
 3: david      18
    <string>   <int>
```

gota has smartly inferred that `favorite_number` is an integer column.

Add an `is_even` column to the DataFrame if `favorite_number` is even:

```
isEven := func(s series.Series) series.Series {
    num, _ := s.Int()
    isFavoriteNumberEven := num[0]%2 == 0
    return series.Bools(isFavoriteNumberEven)
}
isEvenSeries := df.Select("favorite_number").Rapply(isEven)
isEvenSeries.SetNames("is_even")
df = df.CBind(isEvenSeries)
```

[Email me](https://github.com/MrPowers/) if you know how to make this code better!

`df` now has an `is_even` column:

```
fmt.Println("df with is even: ", df)

df with is even:  [4x3] DataFrame

    first_name favorite_number is_even
 0: matthew    23              false
 1: daniel     8               true
 2: allison    42              true
 3: david      18              true
    <string>   <int>           <bool>
```

Let's filter the DataFrame so it only contains people with a `favorite_number` that's even (i.e. only include the rows where the `is_even` column is `true`).

```
df = df.Filter(dataframe.F{"is_even", "==", true})
fmt.Println("df filtered: ", df)
```

Here's the output:

```
df filtered:  [3x3] DataFrame

    first_name favorite_number is_even
 0: daniel     8               true
 1: allison    42              true
 2: david      18              true
    <string>   <int>           <bool>
```

Now let's write our filtered DataFrame to disk. Here's the code that'll write this data to your local filesystem:

```
f, err := os.Create("tmp/gota_example_output.csv")
if err != nil {
    log.Fatal(err)
}

df.WriteCSV(f)
```

Open the `tmp/gota_example_output.csv` file in your text editor and inspect the contents:

```
first_name,favorite_number,is_even
daniel,8,true
allison,42,true
david,18,true
```

## Spark / Scala syntax

Spark provides an elegant API for working with [DataFrames](https://mungingdata.com/apache-spark/introduction-to-dataframes/). Let's look at the Spark code to perform these operations.

Read the data into a Spark DataFrame

```
val df = spark.read.option("header", "true").csv(path)
```

Pretty print the DataFrame and the DataFrame schema:

```
df.show()

+----------+---------------+
|first_name|favorite_number|
+----------+---------------+
|   matthew|             23|
|    daniel|              8|
|   allison|             42|
|     david|             18|
+----------+---------------+

df.printSchema()

root
 |-- first_name: string (nullable = true)
 |-- favorite_number: string (nullable = true)
```

Add the `is_even` column to the DataFrame and print the output:

```
val df2 = df.withColumn("is_even", $"favorite_number" % 2 === 0)

df2.show()

+----------+---------------+-------+
|first_name|favorite_number|is_even|
+----------+---------------+-------+
|   matthew|             23|  false|
|    daniel|              8|   true|
|   allison|             42|   true|
|     david|             18|   true|
+----------+---------------+-------+
```

Filter out all the values where `is_even` is `false`:

```
val filteredDF = df2.where($"is_even" === true)

filteredDF.show()

+----------+---------------+-------+
|first_name|favorite_number|is_even|
+----------+---------------+-------+
|    daniel|              8|   true|
|   allison|             42|   true|
|     david|             18|   true|
+----------+---------------+-------+
```

Write the data to disk:

```
filteredDF.repartition(1).write.csv(outputPath)
```

Spark is optimized to write multiple files in parallel. We've used `repartition(1)` to write out a single file, but this is bad practice for bigger datasets.

Here's how Spark will write the data in this example:

```
some_spark_example/
  _SUCCESS
  part-00000-43fad235-8734-4270-9fed-bf0d3b3eda77-c000.csv
```

Check out [Writing Beautiful Apache Spark Code](https://leanpub.com/beautiful-spark/) if you'd like to quickly learn how to use Apache Spark.

## Next steps

A lot of people want to use DataFrames in Go - the existing repos have a lot of stars.

Go is a great language for ETL and a robust DataFrame library will make it even better!

Suggested next steps:

- Study the [Spark DataFrame API](https://mungingdata.com/apache-spark/introduction-to-dataframes/) and see if we can make a Go DataFrame API that's equally elegant (qframe is close!)
- Add native Parquet support
- Add Parquet column pruning for a big speed bump
- Integrate Apache Arrow
- Make 1.0 release
