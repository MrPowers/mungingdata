---
title: "Writing Custom Metadata to Parquet Files and Columns with PyArrow"
date: "2020-08-28"
categories: 
  - "pyarrow"
---

# Writing Custom Metadata to Parquet Files and Columns with PyArrow

Metadata can be written to Parquet files or columns. This blog post explains how to write Parquet files with metadata using PyArrow.

Here are some powerful features that Parquet files allow for because they support metadata:

- the schema is defined in the footer, so it doesn't need to be inferred
- the columns have min / max statistics, which allows for parquet predicate pushdown filtering

CSV files don't support these features:

- there is no schema, so it needs to be inferred (slow / error prone) or explicitly defined (tedious)
- there are no column statistics, so predicate pushdown filtering isn't an option

You can add custom metadata to your Parquet files to make your lakes even more powerful for your specific query patterns. For example, you can store additional column metadata to allow for predicate pushdown filters that are more expansive than what can be supported by the min/max column statistics. Predicate pushdown filtering can make some queries run a lot faster.

This blog post demonstrates how to add file metadata and column metadata to your Parquet files.

If you're just getting started with PyArrow, [read this article on analyzing metadata](https://mungingdata.com/pyarrow/parquet-metadata-min-max-statistics/) before reading the rest of this article.

## Creating a file with arbitrary metadata

Let's read a CSV file into a PyArrow table and write it out as a Parquet file with custom metadata appended to the columns and file schema.

Suppose you have the following `movies.csv` file:

```
movie,release_year
three idiots,2009
her,2013
```

Import the necessary PyArrow code libraries and read the CSV file into a PyArrow table:

```python
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

table = pv.read_csv('movies.csv')
```

Define a custom schema for the table, with metadata for the columns and the file itself.

```python
my_schema = pa.schema([
    pa.field("movie", "string", False, metadata={"spanish": "pelicula"}),
    pa.field("release_year", "int64", True, metadata={"portuguese": "ano"})],
    metadata={"great_music": "reggaeton"})

t2 = table.cast(my_schema)
```

Write out the table as a Parquet file.

```python
pq.write_table(t2, 'movies.parquet')
```

Let's inspect the metadata of the Parquet file:

```python
s = pq.read_table('movies.parquet').schema

s.metadata # => {b'great_music': b'reggaeton'}
s.metadata[b'great_music'] # => b'reggaeton'
```

Notice that b-strings, aka byte strings, are used in the metadata dictionaries. Parquet is a binary format and you can't store regular strings in binary file types.

Fetch the metadata associated with the `release_year` column:

```python
parquet_file = pq.read_table('movies.parquet')
parquet_file.schema.field('release_year').metadata[b'portuguese'] # => b'ano'
```

## Updating schema metadata

You can also merge your custom metadata with the existing file metadata.

Suppose you have another CSV file with some data on pets:

```
nickname,age
fofo,3
tio,1
lulu,9
```

Read in the CSV data to a PyArrow table and demonstrate that the schema metadata is `None`:

```python
table = pv.read_csv('pets1.csv')
table.schema.metadata # => None
```

Define some custom metadata and merge it with the existing metadata.

```python
custom_metadata = {b'is_furry': b'no_fluffy', b'likes_cats': b'negative'}
merged_metadata = { **custom_metadata, **(table.schema.metadata or {}) }
```

Create a new PyArrow table with the `merged_metadata`, write it out as a Parquet file, and then fetch the metadata to make sure it was written out correctly.

```python
fixed_table = table.replace_schema_metadata(merged_metadata)
pq.write_table(fixed_table, 'pets1_with_metadata.parquet')
parquet_table = pq.read_table('pets1_with_metadata.parquet')
parquet_table.schema.metadata # => {b'is_furry': b'no_fluffy', b'likes_cats': b'negative'}
parquet_table.schema.metadata[b'is_furry'] # => b'no_fluffy'
```

## Closing thoughts

Parquet is a powerful file format, partially because it supports metadata for the file and columns.

Storing the data schema in a file is more accurate than inferring the schema and less tedious than specifying the schema when reading the file.

Column statistics allow for features like predicate pushdown filtering that significantly speed up some queries.

PyArrow makes it easy for you to add your own metadata to the Parquet file or columns, so you can make your Parquet data lakes even more powerful.
