---
title: "Designing Scala Packages and Imports for Readable Spark Code"
date: "2019-11-24"
categories: 
  - "apache-spark"
---

# Designing Scala Packages and Imports for Readable Spark Code

This blog post explains how to import core Spark and Scala libraries like [spark-daria](https://github.com/MrPowers/spark-daria/) into your projects.

It's important for library developers to organize package namespaces so it's easy for users to import their code.

Library users should import code so it's easy for teammates to identify the source of functions when they're invoked.

I wrote a book called [Beautiful Spark](https://leanpub.com/beautiful-spark) that teaches you the easiest way to build Spark applications and manage complex logic. [The book](https://leanpub.com/beautiful-spark) will teach you the most important aspects of Spark development and will supercharge your career.

Let's start with a simple example that illustrates why wilcard imports can generate code that's hard to follow.

## Simple example

Let's look at a little code snippet that uses the Spark `col()` function and the [spark-daria](https://github.com/MrPowers/spark-daria/) `removeAllWhitespace()` function.

```
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql._

df.withColumn("clean_text", removeAllWhitespace(col("text")))
```

Wildcard imports (imports with underscores) create code that's difficult to follow. It's hard to tell where the `removeAllWhitespace()` function is defined.

## Curly brace import

We can use the curly brace import style to make it easy for other programmers to search the codebase and find where `removeAllWhitespace()` is defined.

```
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.{removeAllWhitespace}
```

Per the [Databricks Scala style guide](https://github.com/databricks/scala-style-guide#imports):

> Avoid using wildcard imports, unless you are importing more than 6 entities, or implicit methods. Wildcard imports make the code less robust to external changes.

In other words, use curly brace imports unless you're going to use more than 6 methods from the object that's being imported.

That's not the best advice because `import com.github.mrpowers.spark.daria.sql._` will still leave users confused about where `removeAllWhitespace()` is defined, regardless of how extensively the [spark-daria](https://github.com/MrPowers/spark-daria/) functions are used.

## Named import

We can name imports so all function invocations make it clear where the functions are defined.

```
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.{functions => dariaFunctions}

df.withColumn("clean_text", dariaFunctions.removeAllWhitespace(col("text")))
```

This allows users to search for `dariaFunctions` and figure out that `removeAllWhitespace` is defined in [spark-daria](https://github.com/MrPowers/spark-daria).

This approach is potentially confusing if the same import isn't consistently named throughout the codebase. If some developers import the daria functions as `darF` and other developers import them as `dariaFunctions`, it could get confusing.

Named imports help here because the package name is so ridiculously long. If the package name was shorter, we could do `import mrpowers.daria` and invoke the function with `daria.functions.removeAllWhitespace()`. This gives us the best of both worlds - easy imports and consistent function invocation.

## Complicated package names are the root issue

Scala package names typically follow the verbose Java conventions. Instead of `spark.sql.functions` we have `org.apache.spark.sql.functions`.

Most Spark libraries follow the same trend. We have `com.github.mrpowers.spark.daria.sql.functions` instead of `mrpowers.daria.functions`.

Some of the great libraries created by Li Haoyi allow for short imports:

- `import fastparse._`
- `import utest._`

It's arguable that Li Haoyi's import statments are too short because they could cause some namespace collisions (with another library named `fastparse` for example).

These imports would strike a good balance of being short and having a low probability of name collisions.

- `import lihaoyi.fastparse._`
- `import lihaoyi.utest._`

The [internet has mixed feelings](https://www.reddit.com/r/scala/comments/e0mqjn/should_scala_library_developers_ignore_the_java/) on if the import statements should be short or if they should follow the verbose Java style.

## Wildcard Imports are OK for Spark core classes

Wildcard imports should be avoided in general, but they're OK for core Spark classes.

The following code is completely fine, even though you're importing a ton of functions to the global namespace.

```
import org.apache.spark.sql.functions._
```

Spark programmers are familiar with the Spark core functions and will know that functions like `col()` and `lit()` are defined in Spark.

## Implicit imports

You have to use the wildcard syntax to import objects that wrap implicit classes. Here's a snippet of code that extends the Spark `Column` class:

```
package com.github.mrpowers.spark.daria.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object ColumnExt {

  implicit class ColumnMethods(col: Column) {
    def chain(colMethod: (Column => Column)): Column = {
      colMethod(col)
    }
  }

}
```

You can import the column extensions as follows: `import com.github.mrpowers.spark.daria.sql.ColumnExt._`.

Staying away from implicits is generally best, so feel free to avoid this language feature.

## Spark implicits

Spark implicits come in handy, expecially in the notebook enviroment. They can be imported with `import spark.implicits._`.

The Spark shell and Databricks notebooks both import implicits automatically after creating the SparkSession. [See here for best practices on managing the SparkSession in Scala projects](https://mungingdata.com/apache-spark/sparksession/).

If you'd like to access Spark implicits other environments (e.g. code in an IntelliJ text editor), you'll need to import them youself.

Importing Spark implicits is a little tricky because the [SparkSession](https://mungingdata.com/apache-spark/sparksession/) needs to be instantiated first.

## Conclusion

There are a variety of ways to import Spark code.

It's hard to maintain code that has lots of wildcard imports. The namespace gets cluttered with functions from a variety of objects and it can be hard to tell which methods belong to which library.

I recommend ignoring the Java conventions of having deeply nested packages, so you don't force users to write really long import statements. Also give your projects one word names so import statements are shorter too ;)

Library developers are responsible for providing users with a great user experience. The package structure of your project is a key part of your public interface. Choose wisely!

Check out [Beautiful Spark](https://leanpub.com/beautiful-spark) to learn more Spark best practices.
