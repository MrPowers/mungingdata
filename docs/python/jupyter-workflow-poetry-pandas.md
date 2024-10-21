---
title: "Amazing Python Data Workflow with Poetry, Pandas, and Jupyter"
date: "2020-09-05"
categories: 
  - "python"
---

# Amazing Python Data Workflow with Poetry, Pandas, and Jupyter

Poetry makes it easy to install Pandas and Jupyter to perform data analyses.

Poetry is a robust dependency management system and makes it easy to make Python libraries accessible in Jupyter notebooks.

The workflow outlined in this post makes projects that can easily be run on other machines. Your teammates can easily run `poetry install` to setup an identical Jupyter development environment on their computers.

Python dependency management is hard, especially for projects with notebooks. You'll often find yourself in dependency hell when trying to setup someone else's repo with Jupyter notebooks. This workflow saves you from dependency hell.

This post shows how to manage environments with Poetry, but [you can also use conda of course](https://mungingdata.com/dask/software-environments-conda-miniconda/).

## Create a project

[Install Poetry](https://python-poetry.org/docs/#installation) and run the `poetry new blake` command to create a project called `blake`. All the code covered in this post is [in a GitHub repo](https://github.com/MrPowers/blake), but it's best to run all the commands on your local machine, so you learn more.

Change into the `blake` directory with `cd blake` and examine the file structure:

```
blake/
  blake/
    __init__.py
  tests/
    __init__.py
    test_blake.py
  pyproject.toml
  README.rst
```

We'll investigate the contents of these files later in this post.

## Install Pandas and Jupyter

Run `poetry add pandas jupyter ipykernel` to install the dependencies that are required for running notebooks on your local machine.

This command downloads a bunch of Python code in the `~/Library/Caches/pypoetry/virtualenvs/blake-Y_2IcspR-py3.7/` directory. This is referred to as the "virtual environment" of your project.

If you cloned the [blake repo](https://github.com/MrPowers/blake), you could simply run `poetry install` to setup the virtual environment.

## Notebook workflow

Run `poetry shell` in your Terminal to create a subshell within the virtual environment. This is the key step that lets you run a Jupyter notebook with all the right project dependencies.

<figure>

![](images/Screen-Shot-2020-09-04-at-8.00.06-AM-1024x115.png)

<figcaption>

poetry shell

</figcaption>

</figure>

Run `jupyter notebook` to open the project with Jupyter in your browser.

Click New => Folder to create a folder called `notebooks/`.

<figure>

![](images/Screen-Shot-2020-09-04-at-8.05.01-AM.png)

<figcaption>

Create folder

</figcaption>

</figure>

Go to the `notebooks` folder and click New => Notebook: Python 3 to create a notebook.

<figure>

![](images/Screen-Shot-2020-09-04-at-8.06.28-AM.png)

<figcaption>

Create notebook

</figcaption>

</figure>

Click Untitled at the top of the page that opens and rename the notebook to be `some_pandas_fun`:

<figure>

![](images/Screen-Shot-2020-09-04-at-8.07.27-AM.png)

<figcaption>

Rename notebook

</figcaption>

</figure>

Run `2 + 2` in the first cell to make sure the notebook can run a basic Python command.

Then run this series of commands in the subsequent cells to create a Pandas DataFrame.

```
import pandas as pd
data = [['tom', 10], ['nick', 15], ['juli', 14]]
df = pd.DataFrame(data, columns = ['Name', 'Age'])
print(df)
```

## Accessing application code in notebook

The application code goes in the `blake/` directory. Create a `blake/super_important.py` file with a `some_message` function.

```
def some_message():
    return "I like dancing reggaeton"
```

Create another Jupyter notebook, import the `some_message` function, and run the code to make sure it's accessible.

<figure>

![](images/Screen-Shot-2020-09-04-at-9.05.21-AM.png)

<figcaption>

Access application code in notebook

</figcaption>

</figure>

## Testing application code

Create a `tests/test_super_important.py` file that verifies the `some_message` function is working properly.

```
import pytest

from blake.super_important import *

def test_some_message():
    assert some_message() == "I like dancing reggaeton"
```

Run the test suite with the `poetry run pytest tests/` command.

<figure>

![](images/Screen-Shot-2020-09-04-at-10.53.51-AM.png)

<figcaption>

Run tests

</figcaption>

</figure>

As you can see, Poetry makes it easy to organize the application code, notebooks, and tests for a project.

## Write a Parquet file

Let's create a CSV file and then write it out as a Parquet file from a notebook.

Pandas requires PyArrow to write Parquet files so run `poetry add pyarrow` to include the dependency.

Create the `data/coffee.csv` file with this data:

```
coffee_type,has_milk
black,false
latte,true
americano,false
```

Create a `notebooks/csv_to_parquet.ipynb` file that'll convert the CSV to a Parquet file.

Here's the code:

```
import pandas as pd
import os

df = pd.read_csv(os.environ['HOME'] + '/Documents/code/my_apps/blake/data/coffee.csv')
out_dirname = os.environ['HOME'] + '/Documents/code/my_apps/blake/tmp'
os.makedirs(out_dirname, exist_ok=True)
df.to_parquet(out_dirname + '/coffee.parquet')
```

<figure>

![](images/Screen-Shot-2020-09-05-at-7.18.55-AM.png)

<figcaption>

Create Parquet file

</figcaption>

</figure>

## Read a Parquet file

Create a `notebooks/csv_to_parquet.ipynb` file and read the Parquet file into a DataFrame.

Then count how many different types of coffee contain milk in the dataset.

```
import pandas as pd
import os

df = pd.read_parquet(os.environ['HOME'] + '/Documents/code/my_apps/blake/tmp/coffee.parquet')
coffees_with_milk = df[df['has_milk'] == True]
coffees_with_milk.count()
```

<figure>

![](images/Screen-Shot-2020-09-05-at-7.45.46-AM.png)

<figcaption>

Read Parquet file to DataFrame

</figcaption>

</figure>

Parquet is a better file format than CSV for almost all data analyses. Use this design pattern to build Parquet files, so you can perform your analyses quicker.

## pyproject.toml

Here are the contents of the `pyproject.toml` file:

```
[tool.poetry]
name = "blake"
version = "0.1.0"
description = ""
authors = ["MrPowers"]

[tool.poetry.dependencies]
python = "^3.7"
pandas = "^1.1.1"
jupyter = "^1.0.0"
ipykernel = "^5.3.4"
pyarrow = "^1.0.1"

[tool.poetry.dev-dependencies]
pytest = "^5.2"

[build-system]
requires = ["poetry>=0.12"]
build-backend = "poetry.masonry.api"

```

The project dependencies clearly specify the versions that are required to run this project. The dev-dependencies specify additional dependencies that are required when running the test suite (i.e. when running `poetry run pytest tests/`).

## poetry.lock

Here's how Poetry builds the virtual environment when `poetry install` is run:

- If the `poetry.lock` file exists, use the exact dependency version specified in the lock file to build the virtual environment
- If the `poetry.lock` file doesn't exist, then use the `pypoetry.toml` file to resolve the dependencies, build a lock file, and setup the virtual environment

You should check the lock file into source control so collaborators can build a virtual environment that's identical to what you're using.

If you're ever having trouble with the virtual environment or lock file, feel free to simply delete them and recreate them with `poetry install`. Don't manually modify the lock file or virtual environment.

## Conclusion

Poetry allows for an amazing local workflow with Pandas and Jupyter.

Poetry virtual environments are clean, easy to use, and save you from dependency hell.

Poetry also makes it easy to work with Python cluster computing libraries like [Dask](https://mungingdata.com/dask/read-csv-to-parquet/) and [PySpark](https://mungingdata.com/pyspark/poetry-dependency-management-wheel/).

The Poetry workflow outlined in this post is especially useful, because it easily extends to a team of multiple developers. You can follow the steps outlined in this guide, upload your code to GitHub, so your teammates can clone the repo and run `poetry install` to create an identical development environment on their machine.

Poetry saves your teammates from suffering with Python dependency hell. Make your projects easy to use and you'll get more users and adoption!
