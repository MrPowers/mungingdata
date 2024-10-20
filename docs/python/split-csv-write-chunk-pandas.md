---
title: "Splitting Large CSV files with Python"
date: "2021-11-24"
categories: 
  - "python"
---

This blog post demonstrates different approaches for splitting a large CSV file into smaller CSV files and outlines the costs / benefits of the different approaches.

TL;DR

- It’s faster to split a CSV file with a shell command / the Python filesystem API
- Pandas / Dask are more robust and flexible options

Let’s investigate the different approaches & look at how long it takes to split a 2.9 GB CSV file with 11.8 million rows of data.

## Split with shell

You can split a CSV on your local filesystem with a shell command.

```
FILENAME=nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2015.csv

split -b 10000000 $FILENAME tmp/split_csv_shell/file
```

This only takes 4 seconds to run. Each file output is 10MB and has around 40,000 rows of data.

This approach has a number of key downsides:

- It cannot be run on files stored in a cloud filesystem like S3
- It breaks if there are newlines in the CSV row (possible for quoted data)
- Does not handle the header row

## Python filesystem APIs

You can also use the Python filesystem readers / writers to split a CSV file.

```
chunk_size = 40000

def write_chunk(part, lines):
    with open('../tmp/split_csv_python/data_part_'+ str(part) +'.csv', 'w') as f_out:
        f_out.write(header)
        f_out.writelines(lines)

with open("../nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", "r") as f:
    count = 0
    header = f.readline()
    lines = []
    for line in f:
        count += 1
        lines.append(line)
        if count % chunk_size == 0:
            write_chunk(count // chunk_size, lines)
            lines = []
    # write remainder
    if len(lines) > 0:
        write_chunk((count // chunk_size) + 1, lines)
```

This takes 9.6 seconds to run and properly outputs the header row in each split CSV file, unlike the shell script approach.

It’d be easier to adapt this script to run on files stored in a cloud object store than the shell script as well.

Let’s look at some approaches that are a bit slower, but more flexible.

## Pandas

Here’s how to read in chunks of the CSV file into Pandas DataFrames and then write out each DataFrame.

```
source_path = "../nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2015.csv"

for i,chunk in enumerate(pd.read_csv(source_path, chunksize=40000, dtype=dtypes)):
    chunk.to_csv('../tmp/split_csv_pandas/chunk{}.csv'.format(i), index=False)
```

This approach writes 296 files, each with around 40,000 rows of data.  It takes 160 seconds to execute.

The Pandas approach is more flexible than the Python filesystem approaches because it allows you to process the data before writing.  You could easily update the script to add columns, filter rows, or write out the data to different file formats.

Manipulating the output isn’t possible with the shell approach and difficult / error-prone with the Python filesystem approach.

## Dask

Here’s how to read the CSV file into a Dask DataFrame in 10 MB chunks and write out the data as 287 CSV files.

```
ddf = dd.read_csv(source_path, blocksize=10000000, dtype=dtypes)

ddf.to_csv("../tmp/split_csv_dask")
```

The Dask script runs in 172 seconds.

For this particular computation, the Dask runtime is roughly equal to the Pandas runtime.  The Dask task graph that builds instructions for processing a data file is similar to the Pandas script, so it makes sense that they take the same time to execute.

Dask allows for some intermediate data processing that wouldn’t be possible with the Pandas script, like sorting the entire dataset.  The Pandas script only reads in chunks of the data, so it couldn’t be tweaked to perform shuffle operations on the entire dataset.

## Comparing approaches

This graph shows the program execution runtime by approach.

![](https://lh4.googleusercontent.com/M6gBvWVM5dRItUC6vWb43Y-vC_rYdBWaMCQKaze_sGCB8jf2kRdKztZHGWlOCDh-ci_DH45oG40_kFcCEmoI9warhErFy6bVil3FgKmNRaBa3ERiENpDBAuZhASBKDjToALKnUOL)

If you need to quickly split a large CSV file, then stick with the Python filesystem API.

Processing time generally isn’t the most important factor when splitting a large CSV file.  Production grade data analyses typically involve these steps:

- Validating data and throwing out junk rows
- Properly assigning types to each column
- Writing data to a good file format for data analysis, like Parquet
- Compressing the data

The main objective when splitting a large CSV file is usually to make downstream analyses run faster and more reliably.  Dask is the most flexible option for a production-grade solution.

## Next steps

Large CSV files are not good for data analyses because they can’t be read in parallel.  Multiple files can easily be read in parallel.

CSV files in general are limited because they don’t contain schema metadata, the header row requires extra processing logic, and the row based nature of the file doesn’t allow for column pruning.  The main advantage of CSV files is that they’re human readable, but that doesn’t matter if you’re processing your data with a production-grade data processing engine, like Python or Dask.

Splitting up a large CSV file into multiple Parquet files (or another good file format) is a great first step for a production-grade data processing pipeline.  Dask takes longer than a script that uses the Python filesystem API, but makes it easier to build a robust script.  The performance drag doesn’t typically matter.  You only need to split the CSV once.

The more important performance consideration is figuring out how to split the file in a manner that’ll make all your downstream analyses run significantly faster.
