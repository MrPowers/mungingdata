---
title: "Read multiple CSVs into pandas DataFrame"
date: "2021-12-28"
categories: 
  - "pandas"
---

# Read multiple CSVs into pandas DataFrame

This post explains how to read multiple CSVs into a pandas DataFrame.

pandas filesystem APIs make it easy to load multiple files stored in a single directory or in nested directories.

Other Python libraries can even make this easier and more scalable. Let's take a look at an example on a small dataset.

## Reading CSVs with filesystem functions

Suppose you have the following files.

```
animals/
  file1.csv
  file2.csv
```

Here's how to load the files into a pandas DataFrame.

```
import glob
import os
import pandas as pd

all_files = glob.glob("animals/*.csv")
df = pd.concat((pd.read_csv(f) for f in all_files))
print(df)
```

Here's what's printed:

```
  animal_name animal_type
0        susy     sparrow
1       larry        lion
0         dan     dolphin
1      camila         cat
```

This script loads each file into a separate pandas DataFrames and then concatenates all the individual DataFrames into one final result.

Here's how to load the files into a pandas DataFrame when the files aren't located in the present working directory. The files are located in the `~/Documents/code/coiled/coiled-datasets/data/animals` directory on my machine.

```
home = os.path.expanduser("~")

path = f"{home}/Documents/code/coiled/coiled-datasets/data/animals/"
all_files = glob.glob(path + "/*.csv")

df = pd.concat((pd.read_csv(f) for f in all_files))

print(df)
```

This script gives the same result.

You'd need to tweak the script to make it multiplatform. It's tedious to write logic to list the files when creating a Pandas DataFrame from multiple files. Let's try Dask which doesn't require us to write the file listing code or worry ourselves with multiplatform compatibility.

## Loading multiple files with Dask

Read the files into a Dask DataFrame with Dask's `read_csv` method.

```
import dask.dataframe as dd

ddf = dd.read_csv(f"{path}/*.csv")
```

Now convert the Dask DataFrame to a pandas DataFrame with the `compute()` method and print the contents.

```
df = ddf.compute()

print(df)
```

Here's what's printed:

```
  animal_name animal_type
0        susy     sparrow
1       larry        lion
0         dan     dolphin
1      camila         cat
```

Dask takes care of the file listing operation and doesn't require you to perform it manually. Dask makes it a lot easier to read and write multiple files compared to pandas.

## Benefits of Dask

Dask is also designed to handle large datasets without erroring out like pandas.

Dask splits up data into partitions so it can be processed in parallel. It also allows for computations to be performed in a streaming manner without loading all the data in memory at once.

Dask computations can be scaled up to use all the cores of a single machine or scaled out to leverage a cluster of multiple computers in parallel. Dask is a good option whenever you're facing pandas related scaling issues.

## Reading nested CSVs

Suppose you'd like to read CSV data into a pandas DataFrame that's stored on disk as follows:

```
fish/
  files/
    file1.csv
  more-files/
    file2.csv
    file3.csv
```

Load all of these files into a pandas DataFrame and print the result.

```
path = f"{home}/Documents/code/coiled/coiled-datasets/data/fish/"
all_files = glob.glob(path + "/**/*.csv")

df = pd.concat((pd.read_csv(f) for f in all_files))

print(df)
```

Here's the result that's printed:

```
  fish_name fish_type
0     carol   catfish
1     maria  mackerel
0     betty      bass
1     sally   snapper
0    marvin    marlin
1      tony      tuna
```

`glob` makes it relatively easy to fetch CSVs that are stored in a nested directory structure.

## Conclusion

This post demonstrates how it's straightforward to load multiple CSV files in a pandas DataFrame.

Dask makes it even easier to load multiple files into a Dask DataFrame, which can easily be converted to a pandas DataFrame.

pandas can only handle datasets that are small enough to fit into memory (the [rule of thumb from 2017](https://wesmckinney.com/blog/apache-arrow-pandas-internals/) was data should be 5-10 times smaller than RAM). When you're loading multiple CSV files, it's more likely that you're working with a bigger dataset that'll cause pandas memory issues.

When datasets are small enough to comfortably fit into memory, pandas is the best option. If you start running into memory issues, or would like you analysis to run faster with parallel computations, try scaling up with Dask.
