---
title: "Reading CSVs and Writing Parquet files with Dask"
date: "2020-08-23"
categories: 
  - "dask"
---

# Reading CSVs and Writing Parquet files with Dask

Dask is a great technology for converting CSV files to the Parquet format. Pandas is good for converting a single CSV file to Parquet, but Dask is better when dealing with multiple files.

Convering to Parquet is important and CSV files should generally be avoided in data products. Column file formats like Parquet allow for column pruning, so queries run a lot faster. If you have CSV files, it's best to start your analysis by first converting the files to the Parquet file format.

## Simple example

Let's look at some code that converts two CSV files to two Parquet files.

Suppose the `data/people/people1.csv` file contains the following data:

```
first_name,last_name
jose,cardona
jon,smith
```

The `data/people/people2.csv` file contains the following data

```
first_name,last_name
luisa,montoya
federica,lugo
```

Here's the code that'll write out two Parquet files:

```python
import dask.dataframe as dd

df = dd.read_csv('./data/people/*.csv')
df.to_parquet('./tmp/people_parquet2', write_index=False)
```

Here are the files that are output:

```
tmp/
  people_parquet2/
    part.0.parquet
    part.1.parquet
```

Let's inspect the contents of the `tmp/people_parquet2/part.0.parquet` file:

```python
import pandas as pd
pd.read_parquet('./tmp/people_parquet2/part.0.parquet')
```

```
  first_name last_name
0       jose   cardona
1        jon     smith
```

The `part.0.parquet` file has the same data that was in the `people1.csv` file. Here's how the code was executed:

- The `people1.csv` and `people2.csv` files were read into a Dask DataFrame. A Dask DataFrame contains multiple Pandas DataFrames. Each Pandas DataFrame is referred to as a partition of the Dask DataFrame. In this example, the Dask DataFrame consisted of two Pandas DataFrames, one for each CSV file.
- Each partition in the Dask DataFrame was written out to disk in the Parquet file format. Dask writes out files in parallel, so both Parquet files are written simultaneously. This is one example of how parallel computing makes operations quick!

## Customizing number of output files

Here's code that'll read in the same two CSV files and write out four Parquet files:

```python
df = dd.read_csv('./data/people/*.csv')
df = df.repartition(npartitions=4)
df.to_parquet('./tmp/people_parquet4', write_index=False)
```

Here are the files that are written out to disk:

```
tmp/
  people_parquet4/
    part.0.parquet
    part.1.parquet
    part.2.parquet
    part.3.parquet
```

The repartition method shuffles the Dask DataFrame partitions and creates new partitions.

In this example, the Dask DataFrame starts with two partitions and then is updated to contain four partitions (i.e. it starts with two Pandas DataFrames and the data is the then spread out across four Pandas DataFrames).

Let's take a look at the contents of the `part.0.parquet` file:

```python
import pandas as pd
pd.read_parquet('./tmp/people_parquet4/part.0.parquet')
```

```
  first_name last_name
0       jose   cardona
```

Each row of CSV data has been separated to a different partition. Partitions should generally be 100 MB and you can repartition large datasets with `repartition(partition_size="100MB")`. Repartitioning datasets can be slow, so knowing when and how to repartition is a vital skill when working on distributed computing clusters.

## Other technologies to read / write files

CSV files can also be converted to Parquet files with PySpark and Koalas, [as described in this post](https://mungingdata.com/python/writing-parquet-pandas-pyspark-koalas/). Spark is a powerful tool for writing out lots of Parquet data, but it requires a JVM runtime and is harder to use than Dask.

## Next steps

Dask makes it easy to convert CSV files to Parquet.

Compared to other cluster computing frameworks, Dask also makes it easy to understand how computations are executed under the hood. Cluster computing often feels like a black box - it's hard to tell what computations your cluster is running.

Dask is an awesome framework that's fun to play with. Many more Dask blog posts are coming soon!
