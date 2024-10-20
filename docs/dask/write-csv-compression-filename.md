---
title: "Different ways to write CSV files with Dask"
date: "2021-05-24"
categories: 
  - "dask"
---

This post explains how to write a Dask DataFrame to CSV files.

You'll see how to write CSV files, customize the filename, change the compression, and append files to an existing lake.

We'll also discuss best practices and gotchas that you need to watch out for when productionalizing your code.

## Simple example

Let's create a dataset, write it out to disk, and observe the files that are created.

Create a Dask DataFrame with two partitions and then write it out to disk with `to_csv`:

```
pdf = pd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
)
df = dd.from_pandas(pdf, npartitions=2)
df.to_csv("./tmp/csv_simple")
```

Here are the files that are outputted:

```
csv_simple/
  0.part
  1.part
```

The Dask DataFrame is written as two files because it contains two partitions. Each partition in a Dask DataFrame is written in parallel so the operation is quick.

Here are the contents of `0.part`:

```
,num1,num2
0,1,7
1,2,8
```

Dask writes the index by default. You can set a flag, so the index isn't written:

```
df.to_csv("./tmp/csv_simple", index=False)
```

Now let's see how to customize the filename.

## Customize filename

Here's how to write the data out with a proper file extension:

```
df.to_csv("./tmp/csv_simple2/number-data-*.csv", index=False)
```

Here are the files that are outputted.

```
csv_simple2/
  number-data-0.csv
  number-data-1.csv
```

You can even write the files out to a directory structure:

```
df.to_csv("./tmp/csv_simple3/batch-1/numbers-*.csv", index=False)
```

Here's how the files are written:

```
csv_simple3/
  batch-1/
    numbers-0.csv
    numbers-1.csv
```

## Set compression

Write out the files with gzip compression:

```
df.to_csv("./tmp/csv_compressed/hi-*.csv.gz", index=False, compression="gzip")
```

Here's how the files are outputted:

```
csv_compressed/
  hi-0.csv.gz
  hi-1.csv.gz
```

## Watch out!

The `to_csv` writer clobbers existing files in the event of a name conflict. Be careful whenever you're writing to a folder with existing data, especially if you're using the default file names.

Let's write some data to a CSV directory.

```
pdf = pd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
)
df = dd.from_pandas(pdf, npartitions=2)
df.to_csv("./tmp/csv_danger")
```

Here are the files on disk:

```
csv_danger/
  0.part
  1.part
```

Let's write another file to disk:

```
pdf = pd.DataFrame(
    {"firstname": ["cat"], "lastname": ["dog"]},
)
df = dd.from_pandas(pdf, npartitions=1)
df.to_csv("./tmp/csv_danger")
```

The `csv_danger` folder still has two files, but `0.part` was clobbered.

Here's the contents of `0.part`:

```
,firstname,lastname
0,cat,dog
```

Here's the contents of `1.part`:

```
,num1,num2
2,3,9
3,4,10
```

Luckily, it's easy to add a UUID to the outputted filename to avoid accidental clobbering.

## Concurrency safe appends

You can use a UUID in the filename to make sure none of the underlying files will be clobbered when performing an append operation.

Here's a chunk of code that'll write out two files in the first batch and then write out another file in the second batch:

```
import uuid

pdf = pd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
)
df = dd.from_pandas(pdf, npartitions=2)
batch_id = uuid.uuid4()
df.to_csv(f"./tmp/csv_appender/mything-{batch_id}-*.csv", index=False)

pdf = pd.DataFrame(
    {"num1": [11, 11], "num2": [12, 12]},
)
df = dd.from_pandas(pdf, npartitions=2)
batch_id = uuid.uuid4()
df.to_csv(f"./tmp/csv_appender/mything-{batch_id}-*.csv", index=False)
```

Here's what gets written to the filesystem:

```
csv_appender/
  mything-44a0201c-c381-4ac8-adb6-91ba82b60a92-0.csv
  mything-44a0201c-c381-4ac8-adb6-91ba82b60a92-1.csv
  mything-3ff08ac4-50e3-4b68-bbdb-611a8afe0d09-0.csv
```

You can tell which files are from each batch from the UUID that's embedded in the filename.

Multiple different writers can append to the same directory and there won't be any name collisions with this design pattern.

## Writing to a single file

The `single_file` flag makes it easy to write a single file:

```
df.to_csv("./tmp/my_single_file.csv", index=False, single_file=True)
```

There are other techniques for writing single files that are more appropriate in other situations, [see here for the full description](https://mungingdata.com/dask/ouput-dataframe-single-csv-file/).

## Having fun

Dask even lets you supply a function argument to customize the number part of the output. Let's write two files as `10.part` and `12.part` instead of `0.part` and `1.part`.

```
my_fun = lambda x: str(x * 2 + 10)
pdf = pd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
)
df = dd.from_pandas(pdf, npartitions=2)
df.to_csv("./tmp/csv_even", name_function=my_fun)
```

Here's what is written to the filesystem:

```
csv_even/
  10.part
  12.part
```

## When to use CSV

CSV is a bad file format for data analyses:

- quoting bugs are common
- cannot distinguish between the empty string and "nothing"
- doesn't allow for column pruning
- less compressible than a columnar file format

CSV is human readable and that's really the only advantage.

Dask offers great support for file formats that are more amenable for production grade data workflows, like Parquet.

Parquet gives better performances and saves you from suffering with a lot weird bugs. Avoid CSV whenever possible!

## Conclusion

Dask makes it easy to write CSV files and provides a lot of customization options.

Only write CSVs when a human needs to actually open the file and inspect the contents.

Whenever possible, use a better file format for data analyses like Parquet, Avro, or ORC.
