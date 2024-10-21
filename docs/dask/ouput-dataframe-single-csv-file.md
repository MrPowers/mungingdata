---
title: "Writing Dask DataFrame to a Single CSV File"
date: "2021-05-15"
categories: 
  - "dask"
---

# Writing Dask DataFrame to a Single CSV File

Dask DataFrames are composed of multiple partitions and are outputted as multiple files, one per partition, by default.

This post explains the different approaches to write a Dask DataFrame to a single file and the strategy that works best for different situations.

It also explains why it's generally best to avoid single files whenever possible.

## Multi file default output

Create a Dask DataFrame with two partitions and output the DataFrame to disk to see multiple files are written by default.

Start by creating the Dask DataFrame:

```
import pandas as pd
from dask import dataframe as dd

pdf = pd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
)
df = dd.from_pandas(pdf, npartitions=2)
df.to_csv("./tmp/some_files", index=False)
```

Here are the files that get outputted:

```
some_files/
  0.part
  1.part
```

Here are the contents of `some_files/0.part`:

```
num1,num2
1,7
2,8
```

Here are the contents of `some_files/1.part`:

```
num1,num2
3,9
4,10
```

Each partition in a Dask DataFrame is outputted to a separate file. Outputting multiple files is an intentional design decision. This lets Dask write to multiple files in parallel, which is faster than writing to a single file.

Now let's look at how to write single files with Dask.

## compute

`compute` collects all the data in a Dask DataFrame to a single Pandas partition.

Once all the data is collected to a single Pandas partition, you can write it out as a single file, just as you would with a normal Pandas DataFrame.

Here's how to write the Dask DataFrame to a single file with `compute`:

```
df.compute().to_csv("./tmp/my_one_file.csv", index=False)
```

Here are the contents of `tmp/my_one_file.csv`:

```
num1,num2
1,7
2,8
3,9
4,10
```

This approach only works if your data is small enough to fit in a single Pandas DataFrame. "small enough" depends the size of the computer's RAM.

1 GB of data works fine on a machine with 16 GB of RAM. 10 GB of data might not work on a computer with 16 GB of RAM.

[The Pandas rule of thumb](https://wesmckinney.com/blog/apache-arrow-pandas-internals/) is "have 5 to 10 times as much RAM as the size of your dataset". This rule of thumb is from 2017 - let me know if it has been updated.

## single\_file

You can also write out a single CSV file with the `single_file` flag:

```
df.to_csv("./tmp/my_single_file.csv", index=False, single_file=True)
```

Here are the contents of `my_single_file.csv`:

```
num1,num2
1,7
2,8
3,9
4,10
```

The `single_file` approach has two huge caveats:

1. it only works on local filesystem
2. only works with certain file formats

`single_file` works locally, but you can't use it on a cloud object storage system like AWS S3. S3 objects are immutable and don't support the append operations that `single_file` uses to write data to a single file.

`single_file` wouldn't work for Parquet files either because Parquet files are immutable. You can't open a Parquet file and perform an append operation.

`single_file` is viable for localhost, CSV workflows, and doesn't work in all situations.

## Remote cluster approach

You can use a big computer in the cloud if you want to write a really big CSV file and your local machine isn't powerful enough. You can rent these computers and only pay for the time you're using the machine, down to the nearest second, so this can be a surprisingly affordable option.

There are ec2 machines that have 768 GB of RAM to give you an idea of what kind of computing power you can access in the cloud.

Computers with 128GB of RAM cost $1 per hour and computers with 768GB of RAM cost $6 per hour with on demand pricing ([even cheaper on the spot market](https://mrpowers.medium.com/playing-the-aws-ec2-spot-market-74b703454f4f)), so a big computation that only takes a few hours to run isn't that costly.

The main challenge of this approach is the devops - getting the machine provisioned with Dask/Pandas and setting the right permissions.

## Limitations of single file

Single files are fine for small datasets, but not desirable for large datasets. It's faster to read, compute, and write big datasets that are spread across multiple files.

Developers that are new to cluster computing sometimes gravitate to single files unnecessarily. Sometimes these devs just need to learn a little more Dask to understand that they actually don't need their data in a single file.

When an experienced big data developer encounters a large file, the first think they usually do it split it up into smaller files. Big files are a common performance bottleneck in big data processing pipelines, especially if they're using a compression algorithm that's not splittable. Snappy compression splittable and that's why it's typically used in data analyses instead of gzip, which isn't splittable.

You'll sometimes need to output a single file, but you should generally try to perform computations on multiple files, in parallel.

## Conclusion

Dask makes it easy to write out a single file. There are some limitations to writing out single files, but you'll see those same limitations with any big data processing framework.

Cluster computing newbies are usually more comfortable when working with a single file. Once they learn how to leverage the power of the cluster, they're happy splitting up their workflows to multiple files and enjoying blazing fast computation runtimes.
