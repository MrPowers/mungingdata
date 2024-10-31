---
title: "Analyzing Parquet Metadata and Statistics with PyArrow"
date: "2020-08-24"
categories: 
  - "pyarrow"
---

# Analyzing Parquet Metadata and Statistics with PyArrow

The PyArrow library makes it easy to read the metadata associated with a Parquet file.

This blog post shows you how to create a Parquet file with PyArrow and review the metadata that contains important information like the compression algorithm and the min / max value of a given column.

Parquet files are vital for a lot of data analyses. Knowing how to read Parquet metadata will enable you to work with Parquet files more effectively.

## Converting a CSV to Parquet with PyArrow

PyArrow makes it really easy to convert a CSV file into a Parquet file. Suppose you have the following `data/people/people1.csv` file:

```
first_name,last_name
jose,cardona
jon,smith
```

You can read in this CSV file and write out a Parquet file with just a few lines of PyArrow code:

```python
import pyarrow.csv as pv
import pyarrow.parquet as pq

table = pv.read_csv('./data/people/people1.csv')
pq.write_table(table, './tmp/pyarrow_out/people1.parquet')
```

Let's look at the metadata associated with the Parquet file we just wrote out.

## Fetching metadata of Parquet file

Let's create a PyArrow Parquet file object to inspect the metadata:

```python
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile('./tmp/pyarrow_out/people1.parquet')
```

```
parquet_file.metadata

<pyarrow._parquet.FileMetaData object at 0x10a3d8650>
  created_by: parquet-cpp version 1.5.1-SNAPSHOT
  num_columns: 2
  num_rows: 2
  num_row_groups: 1
  format_version: 1.0
  serialized_size: 531
```

Use the `row_group` method to get row group metadata:

```
parquet_file.metadata.row_group(0)

<pyarrow._parquet.RowGroupMetaData object at 0x10a3dcdc0>
  num_columns: 2
  num_rows: 2
  total_byte_size: 158
```

You can use the `column` method to get column chunk metadata:

```
parquet_file.metadata.row_group(0).column(0)

<pyarrow._parquet.ColumnChunkMetaData object at 0x10a413a00>
  file_offset: 78
  file_path:
  physical_type: BYTE_ARRAY
  num_values: 2
  path_in_schema: first_name
  is_stats_set: True
  statistics:
    <pyarrow._parquet.Statistics object at 0x10a413a50>
      has_min_max: True
      min: jon
      max: jose
      null_count: 0
      distinct_count: 0
      num_values: 2
      physical_type: BYTE_ARRAY
      logical_type: String
      converted_type (legacy): UTF8
  compression: SNAPPY
  encodings: ('PLAIN_DICTIONARY', 'PLAIN', 'RLE')
  has_dictionary_page: True
  dictionary_page_offset: 4
  data_page_offset: 35
  total_compressed_size: 74
  total_uncompressed_size: 70
```

The compression algorithm used by the file is stored in the column chunk metadata and you can fetch it as follows:

```python
parquet_file.metadata.row_group(0).column(0).compression # => 'SNAPPY'
```

## Fetching Parquet column statistics

The min and max values for each column are stored in the metadata as well.

Let's create another Parquet file and fetch the min / max statistics via PyArrow.

Here's the CSV data.

```
nickname,age
fofo,3
tio,1
lulu,9
```

Convert the CSV file to a Parquet file.

```python
table = pv.read_csv('./data/pets/pets1.csv')
pq.write_table(table, './tmp/pyarrow_out/pets1.parquet')
```

Inspect the Parquet metadata statistics to see the min and max values of the `age` column.

```python
parquet_file = pq.ParquetFile('./tmp/pyarrow_out/pets1.parquet')
print(parquet_file.metadata.row_group(0).column(1).statistics)
```

```
<pyarrow._parquet.Statistics object at 0x11ac17eb0>
  has_min_max: True
  min: 1
  max: 9
  null_count: 0
  distinct_count: 0
  num_values: 3
  physical_type: INT64
  logical_type: None
  converted_type (legacy): NONE
```

The Parquet metadata statistics can make certain types of queries a lot more efficient. Suppose you'd like to find all the pets that are 10 years or older in a Parquet data lake containing thousands of files. You know that the max age in the `tmp/pyarrow_out/pets1.parquet` file is 9 based on the Parquet metadata, so you know that none of the data in that file is relevant for your analysis of pets that are 10 or older. You can simply skip the file entirely.

## `num_rows` and `serialized_size`

The number of rows and dataset size are also included in the Parquet metadata.

Big data systems are known to accumulate small files over time with incremental updates. Too many small files can cause performance bottlenecks, so the small files should periodically get compacted into bigger files.

You can query the metadata of all the Parquet files in a lake to identify the small files and determine how they should be compacted so the Parquet lake can be queried efficiently.

## Next steps

Parquet files are important when performing analyses with Pandas, Dask, Spark, or AWS services like Athena.

Most Parquet file consumers don't know how to access the file metadata. This blog post has taught you an important trick that'll put you ahead of your competition ;)
