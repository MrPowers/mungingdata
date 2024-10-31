---
title: "Renaming Columns in Pandas DataFrames"
date: "2021-06-23"
categories: 
  - "pandas"
---

# Renaming Columns in Pandas DataFrames

This article explains how to rename a single or multiple columns in a Pandas DataFrame.

There are multiple different ways to rename columns and you'll often want to perform this operation, so listen up.

## Simple example

Create a Pandas DataFrame and print the contents.

```python
df = pd.DataFrame({"num": [1, 2], "let": ["a", "b"]})
print(df)
```

```
   num let
0    1   a
1    2   b
```

Let's rename the columns to be `number` and `letter` so they're more descriptive.

```python
df.rename({"num": "number", "let": "letter"}, axis="columns", inplace=True)
print(df)
```

```
   number letter
0       1      a
1       2      b
```

`inplace=True` causes the DataFrame to be mutated.

## Rename single column

Create a DataFrame with `first_name` and `last-name` columns.

```python
df = pd.DataFrame({"first_name": ["li", "karol"], "last-name": ["Fung", "G"]})
print(df)
```

```
  first_name last-name
0         li      Fung
1      karol         G
```

We don't want one column in our DataFrame to use underscores and the other to use hyphens. Rename the `last-name` column to be `last_name`.

```python
df.rename({"last-name": "last_name"}, axis="columns", inplace=True)
print(df)
```

```
  first_name last_name
0         li      Fung
1      karol         G
```

It's easy to rename a single column in a DataFrame and leave the other column names unchanged.

## Apply function to all column names

You can also apply a function to all column names. This is especially useful when you have a lot of columns or want to build a reusable transformation that can be applied to different DataFrames.

Create a DataFrame with spaces in the column names.

```python
df = pd.DataFrame({"some place": ["hawaii", "costa rica"], "fun activity": ["surfing", "zip lining"]})
print(df)
```

```
   some place fun activity
0      hawaii      surfing
1  costa rica   zip lining
```

Write a function that'll replace all the spaces with underscores in the column names.

```python
df.rename(lambda x: x.replace(" ", "_"), axis="columns", inplace=True)
print(df)
```

```
   some_place fun_activity
0      hawaii      surfing
1  costa rica   zip lining
```

## Next steps

Stick to the column renaming methods mentioned in this post and don't use the techniques that were popular in earlier versions of Pandas.

This article intentionally omits legacy approaches that shouldn't be used anymore.

The Pandas API is flexible and supports all common column renaming use cases:

- renaming multiple columns with user specifed names
- renaming some columns
- applying a function to all column names

Be happy that Pandas is providing you with such a nice user interface!
