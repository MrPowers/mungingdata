---
title: "Converting CSV files to Parquet with Go"
date: "2020-03-28"
categories: 
  - "go"
---

This blog post explains how to read data from a CSV file and write it out as a Parquet file.

The Parquet file format is better than CSV for a lot of data operations. Columnar data stores allow for column pruning that massively speeds up lots of queries.

Go is a great language for ETL. Writing out Parquet files makes it easier for downstream Spark or Python to consume data in an optimized manner.

The [parquet-go](https://github.com/xitongsys/parquet-go/) library makes it easy to convert CSV files to Parquet files.

## Sample CSV data

Let's start with the following sample data in the `data/shoes.csv` file:

```
nike,air_griffey
fila,grant_hill_2
steph_curry,curry7
```

Let's read this data and write it out as a Parquet file.

Check out the [parquet-go-example](https://github.com/MrPowers/parquet-go-example/) repo if you'd like to run this code yourself.

## Create Parquet file

Create a `Shoe` struct that'll be used for each row of data in the CSV file:

```
type Shoe struct {
    ShoeBrand string `parquet:"name=shoe_brand, type=UTF8"`
    ShoeName  string `parquet:"name=shoe_name, type=UTF8"`
}
```

Setup the Parquet writer so it's ready to accept data writes:

```
var err error

fw, err := local.NewLocalFileWriter("tmp/shoes.parquet")
if err != nil {
    log.Println("Can't create local file", err)
    return
}

pw, err := writer.NewParquetWriter(fw, new(Shoe), 2)
if err != nil {
    log.Println("Can't create parquet writer", err)
    return
}

pw.RowGroupSize = 128 * 1024 * 1024 //128M
pw.CompressionType = parquet.CompressionCodec_SNAPPY
```

Open up the CSV file, iterate over every line in the file, and then write each line to the Parquet file:

```
csvFile, _ := os.Open("data/shoes.csv")
reader := csv.NewReader(bufio.NewReader(csvFile))

for {
    line, error := reader.Read()
    if error == io.EOF {
        break
    } else if error != nil {
        log.Fatal(error)
    }
    shoe := Shoe{
        ShoeBrand: line[0],
        ShoeName:  line[1],
    }
    if err = pw.Write(shoe); err != nil {
        log.Println("Write error", err)
    }
}
```

Once we've iterated over all the lines in the file, we can stop the `NewParquetWriter` and close the `NewLocalFileWriter`.

```
if err = pw.WriteStop(); err != nil {
    log.Println("WriteStop error", err)
    return
}

log.Println("Write Finished")
fw.Close()
```

The data will be written in the `tmp/shoes.parquet` file. You can run this on your local machine with the `go run csv_to_parquet.go` command.

Let's read this Parquet file into a Spark DataFrame to verify that it's compatible with another framework. Spark loves Parquet files ;)

## Read into Spark DataFrame

You can [download Spark](https://spark.apache.org/downloads.html) to run this code on your local machine if you'd like.

The Parquet file was ouputted to `/Users/powers/Documents/code/my_apps/parquet-go-example/tmp/shoes.parquet` on my machine.

`cd` into the downloaded Spark directory (e.g. `cd ~/spark-2.4.0-bin-hadoop2.7/bin/`) and then run `./spark-shell` to start the Spark console.

Let's read the Parquet file into a Spark DataFrame:

```
val path = "/Users/powers/Documents/code/my_apps/parquet-go-example/tmp/shoes.parquet"
val df = spark.read.parquet(path)
```

Run the `show()` method to inspect the DataFrame contents:

```
df.show()

+-----------+------------+
| shoe_brand|   shoe_name|
+-----------+------------+
|       nike| air_griffey|
|       fila|grant_hill_2|
|steph_curry|      curry7|
+-----------+------------+
```

Run the `printSchema()` method to view the DataFrame schema.

```
df.printSchema()

root
 |-- shoe_brand: string (nullable = true)
 |-- shoe_name: string (nullable = true)
```

You can use Go to build a Parquet data lake and then do further data analytics with Spark. Parquet is the perfect pass off between Go and Spark!

## Reading into a Go DataFrame

[qframe](https://github.com/tobgu/qframe) seems to be the most promising Go DataFrame library.

It doesn't support Parquet yet, but hopefully we can get a `qframe.ReadParquet` method added ;)

## Next steps

We need to create more examples and demonstrate that parquet-go can also write out other column types like integers.

Go is a great language for ETL. Parquet support makes it even better!
