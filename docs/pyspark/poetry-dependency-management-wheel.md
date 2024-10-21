---
title: "PySpark Dependency Management and Wheel Packaging with Poetry"
date: "2020-06-01"
categories: 
  - "pyspark"
---

# PySpark Dependency Management and Wheel Packaging with Poetry

This blog post explains how to create a PySpark project with Poetry, the best Python dependency management system. It'll also explain how to package PySpark projects as wheel files, so you can build libraries and easily access the code on Spark clusters.

Poetry is beloved by [the co-creator of Django](https://jacobian.org/2019/nov/11/python-environment-2020/) and [other bloggers](https://hackersandslackers.com/python-poetry-package-manager/). It's even part of the [hypermodern Python](https://cjolowicz.github.io/posts/hypermodern-python-01-setup/) stack.

This post will demonstrate how Poetry is great for PySpark projects.

## Creating a project

Let's create a project called angelou after the poet [Maya Angelou](https://en.wikipedia.org/wiki/Maya_Angelou). [Check out this repo for all the code in this blog](https://github.com/MrPowers/angelou).

Create the project with `poetry new angelou`.

This will create the `angelou` project with these contents:

```
angelou
  angelou
    __init__.py
  pyproject.toml
  README.rst
  tests
    __init__.py
    test_angelou.py
```

Here are the contents of the `pyproject.toml` file:

```
[tool.poetry]
name = "angelou"
version = "0.1.0"
description = ""
authors = ["MrPowers <matthewkevinpowers@gmail.com>"]

[tool.poetry.dependencies]
python = "^3.7"

[tool.poetry.dev-dependencies]
pytest = "^5.2"

[build-system]
requires = ["poetry>=0.12"]
build-backend = "poetry.masonry.api"
```

The `pyproject.toml` file specifies the Python version and the project dependencies.

## Add PySpark to project

Add PySpark to the project with the `poetry add pyspark` command.

Here's the console output when the command is run:

```
Creating virtualenv angelou--6rG3Bgg-py3.7 in /Users/matthewpowers/Library/Caches/pypoetry/virtualenvs
Using version ^2.4.5 for pyspark

Updating dependencies
Resolving dependencies... (2.1s)

Writing lock file


Package operations: 13 installs, 0 updates, 0 removals

  - Installing zipp (3.1.0)
  - Installing importlib-metadata (1.6.0)
  - Installing pyparsing (2.4.7)
  - Installing six (1.15.0)
  - Installing attrs (19.3.0)
  - Installing more-itertools (8.3.0)
  - Installing packaging (20.4)
  - Installing pluggy (0.13.1)
  - Installing py (1.8.1)
  - Installing py4j (0.10.7)
  - Installing wcwidth (0.1.9)
  - Installing pyspark (2.4.5)
  - Installing pytest (5.4.2)
```

Poetry automatically created a virtual environment all the project dependencies are stored in the `/Users/matthewpowers/Library/Caches/pypoetry/virtualenvs/angelou--6rG3Bgg-py3.7/` directory.

You might wonder why 13 dependencies were added to the project - didn't we just install PySpark? Why wasn't one dependency added?

PySpark depends on other libraries like `py4j`, [as you can see with this search](https://github.com/apache/spark/search?l=Python&q=py4j). Poetry needs to add everything PySpark depends on to the project as well.

[pytest requires](https://github.com/pytest-dev/pytest/blob/70b5bdf4ba6f16e45cb6ac51b8c256a3d1608bda/setup.py) `py`, `importlib-metadata`, and `pluggy`, so those dependencies need to be added to our project as well.

Poetry makes sure your virtual environment contains all your explicit project dependencies and all dependencies of your explicit dependencies.

## Poetry lock file

The `poetry add pyspark` command also creates a `poetry.lock` file as hinted by the "Writing lock file" console output when `poetry add pyspark` is run.

Here's what the Poetry website says about [the Lock file](https://python-poetry.org/docs/libraries/#lock-file):

> For your library, you may commit the `poetry.lock` file if you want to. This can help your team to always test against the same dependency versions. However, this lock file will not have any effect on other projects that depend on it. It only has an effect on the main project. If you do not want to commit the lock file and you are using git, add it to the `.gitignore`.

## PySpark DataFrame transformation

Let's create a [PySpark DataFrame transformation](https://mungingdata.com/pyspark/chaining-dataframe-transformations/) that'll append a `greeting` column to a DataFrame.

Create a `transformations.py` file and add this code:

```python
import pyspark.sql.functions as F

def with_greeting(df):
    return df.withColumn("greeting", F.lit("hello!"))
```

Let's verify that the `with_greeting` function appends a `greeting` column as expected.

## Testing the DataFrame transformation

Here's how we'll test the `with_greeting` function:

- Create a DataFrame and run the `with_greeting` function (`actual_df`)
- Create another DataFrame with the anticipated results (`expected_df`)
- Compare the DataFrames and make sure the actual result is the same as what's expected

We need to create a SparkSession to create the DataFrames that'll be used in the test.

Create a `sparksession.py` file with these contents:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
            .master("local")
            .appName("angelou")
            .getOrCreate())
```

Create a `test_transformations.py` file in the `tests/` directory and add this code:

```python
import pytest

import angelou.sparksession as S
import angelou.transformations as T

class TestTransformations(object):

    def test_with_greeting(self):
        source_data = [
            ("jose", 1),
            ("li", 2)
        ]
        source_df = S.spark.createDataFrame(
            source_data,
            ["name", "age"]
        )

        actual_df = T.with_greeting(source_df)

        expected_data = [
            ("jose", 1, "hello!"),
            ("li", 2, "hello!")
        ]
        expected_df = S.spark.createDataFrame(
            expected_data,
            ["name", "age", "greeting"]
        )

        assert(expected_df.collect() == actual_df.collect())
```

## Console workflow

Run `poetry shell` to spawn a shell within the virtual environment. Then run `python` to start a REPL.

We can copy and paste code from the test file to run the `with_greeting` function in the shell.

```
>>> import angelou.sparksession as S
>>> import angelou.transformations as T
>>> source_data = [
...     ("jose", 1),
...     ("li", 2)
... ]
>>> source_df = S.spark.createDataFrame(
...     source_data,
...     ["name", "age"]
... )

>>> source_df.show()
+----+---+
|name|age|
+----+---+
|jose|  1|
|  li|  2|
+----+---+

>>> actual_df = T.with_greeting(source_df)
>>> actual_df.show()
+----+---+--------+
|name|age|greeting|
+----+---+--------+
|jose|  1|  hello!|
|  li|  2|  hello!|
+----+---+--------+
```

The console REPL workflow can be useful when you're experimenting with code.

It's generally best to keep your console workflow to a minimum and devote your development efforts to building a great test suite.

## Adding quinn dependency

[quinn](https://github.com/MrPowers/quinn) contains useful PySpark helper functions.

Add quinn to the project with `poetry add quinn`.

The `pyproject.toml` file will be updated as follows when `quinn` is added:

```
[tool.poetry.dependencies]
python = "^3.7"
pyspark = "^2.4.5"
quinn = "^0.4.0"
```

quinn is also added to the lock file in two places:

```
[[package]]
category = "main"
description = "Pyspark helper methods to maximize developer efficiency."
name = "quinn"
optional = false
python-versions = ">=2.7"
version = "0.4.0"

quinn = [
    {file = "quinn-0.4.0-py3-none-any.whl", hash = "sha256:4f2f1dd0086a4195ee1ec4420351001ee5687e8183f80bcbc93d4e724510d114"}
]
```

## Using quinn

Create a `with_clean_first_name` function that'll remove all the non-word characters in the `first_name` column of a DataFrame. The quinn `remove_non_word_characters` function will help.

```python
import quinn

def with_clean_first_name(df):
    return df.withColumn(
        "clean_first_name",
        quinn.remove_non_word_characters(F.col("first_name"))
    )
```

Let's create a test to verify that `with_clean_first_name` removes all the non-word characters in the `first_name` field of this DataFrame:

```
+----------+------+
|first_name|letter|
+----------+------+
|    jo&&se|     a|
|      ##li|     b|
|   !!sam**|     c|
+----------+------+
```

This test will use the `create_df` SparkSession extension defined in quinn that makes it easier to create DataFrames.

```python
from pyspark.sql.types import *
from quinn.extensions import *

import angelou.sparksession as S
import angelou.transformations as T

class TestTransformations(object):

    def test_with_clean_first_name(self):
        source_df = S.spark.create_df(
            [("jo&&se", "a"), ("##li", "b"), ("!!sam**", "c")],
            [("first_name", StringType(), True), ("letter", StringType(), True)]
        )

        actual_df = T.with_clean_first_name(source_df)

        expected_df = S.spark.create_df(
            [("jo&&se", "a", "jose"), ("##li", "b", "li"), ("!!sam**", "c", "sam")],
            [("first_name", StringType(), True), ("letter", StringType(), True), ("clean_first_name", StringType(), True)]
        )

        assert(expected_df.collect() == actual_df.collect())
```

## Packaging wheel file

You can build a wheel file with the `poetry build` command.

This will output the following files:

```
dist/
  angelou-0.1.0-py3-none-any.whl
  angelou-0.1.0.tar.gz
```

You can run `poetry publish` to publish the package to PyPi.

## Conclusion

Dependency management has been a pain point for Python developers for years and [the debate on how to solve the issue goes on](https://chriswarrick.com/blog/2018/07/17/pipenv-promises-a-lot-delivers-very-little/).

Thankfully, Python tooling has come a long way and Poetry makes it easy to manage project dependencies.

Poetry is great option for PySpark projects. It makes it easy to build public libraries that are uploaded to PyPi or to build private wheel files so you can run your private projects on Spark clusters.
