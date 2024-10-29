---
title: "Introduction to SBT for Spark Programmers"
date: "2019-03-09"
categories: 
  - "apache-spark"
---

# Introduction to SBT for Spark Programmers

SBT is an interactive build tool that is used to run tests and package your projects as JAR files.

SBT lets you create a project in a text editor and package it, so it can be run in a cloud cluster computing environment (like Databricks).

SBT has a comprehensive [Getting started guide](https://www.scala-sbt.org/1.x/docs/Getting-Started.html), but let's be honest - who wants to read a book on a build tool?

This guide teaches Spark programmers what they need to know about SBT and skips all the other details!

## Sample code

I recommend cloning the [spark-daria](https://github.com/MrPowers/spark-daria) project on your local machine, so you can run the SBT commands as you read this post.

## Running SBT commands

SBT commands can be run from the command line or from the SBT shell.

For example, here's how to run the test suite from Bash: `sbt test`.

Alternatively, we can open the SBT shell by running `sbt` in Bash and then simply run `test`.

Run `exit` to leave the SBT shell.

## `build.sbt`

The SBT build definition is specified in the `build.sbt` file.

This is where you'll add code to specify your dependencies, the Scala version, how to build your JAR files, how to manage memory, etc.

One of the only things that's not specified in the `build.sbt` file is the SBT version itself. The SBT version is specified in the `project/build.properties` file, for example:

```
sbt.version=1.2.8
```

## `libraryDependencies`

You can specify `libraryDependencies` in your `build.sbt` file to fetch libraries from Maven or [JitPack](https://jitpack.io/).

Here's how to add Spark SQL and Spark ML to a project:

```scala
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided"
```

SBT provides shortcut sytax if we'd like to clean up our `build.sbt` file a bit.

```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided"
)
```

`"provided"` dependencies are already included in the environment where we run our code.

Here's an example of some test dependencies that are only used when we run our test suite:

```scala
libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.17.1-s_2.11" % "test"
```

Read this post on [Building JAR files](https://www.mungingdata.com/apache-spark/building-jar-files-with-sbt) for a more detailed discussion on `provided` and `test` dependencies.

## `sbt test`

You can run your test suite with the `sbt test` command.

You can set environment variables in your test suite by adding this line to your `build.sbt` file: `envVars in Test := Map("PROJECT_ENV" -> "test")`. Read the blog post on [Environment Specific Config in Spark Projects](https://www.mungingdata.com/apache-spark/environment-specific-configuration) for more details about this design pattern.

You can run a single test file when using Scalatest with this command:

```
sbt "test:testOnly *LoginServiceSpec"
```

This command is easier to run from the SBT shell:

```
> testOnly *LoginServiceSpec
```

Here is how to run a single test file when using uTest:

```
> testOnly -- com.github.mrpowers.spark.daria.sql.DataFrameExtTest
```

Complicated SBT commands are generally easier to run from the SBT shell, so you don't need to think about proper quoting.

Read [this Stackoverflow thread](https://stackoverflow.com/questions/11159953/scalatest-in-sbt-is-there-a-way-to-run-a-single-test-without-tags) if you'd like to run a single test with Scalatest and [this blog post](https://www.mungingdata.com/apache-spark/testing-with-utest) if you'd like to run a single test with uTest.

## `sbt doc`

The `sbt doc` command generates HTML documentation for your project.

You can open the documentation on your local machine with `open target/scala-2.11/api/index.html` after it's been generated.

You should diligently mark functions and objects as private if they're not part of the API. `sbt doc` won't generate documentation for private members.

Codebases are always easier to understand when the public API is clearly defined.

For more information, read the [spark-style-guide Documentation guidelines](https://github.com/MrPowers/spark-style-guide#documentation) and the [Documenting Spark Code with Scaladoc](https://medium.com/@mrpowers/documenting-spark-code-with-scaladoc-3f71dfe530a2) blog post.

## `sbt console`

The `sbt console` command starts the Scala interpreter with easy access to all your project files.

Let's run `sbt console` in the `spark-daria` project and then invoke the `StringHelpers.snakify()` method.

```
scala> com.github.mrpowers.spark.daria.utils.StringHelpers.snakify("FunStuff") // fun_stuff
```

Running `sbt console` is similar to running the Spark shell with the `spark-daria` JAR file attached. Here's how to start the Spark shell with the `spark-daria` JAR file attached.

```
./spark-shell --jars ~/Documents/code/my_apps/spark-daria/target/scala-2.11/spark-daria-assembly-0.28.0.jar
```

The same code from before also works in the Spark shell:

```
scala> com.github.mrpowers.spark.daria.utils.StringHelpers.snakify("FunStuff") // fun_stuff
```

[This blog post](https://www.mungingdata.com/apache-spark/using-the-console) provides more details on how to use the Spark shell.

The `sbt console` is sometimes useful for playing around with code, but the test suite is usually better. Don't "test" your code in the console and neglect writing real tests.

## `sbt package` / `sbt assembly`

`sbt package` builds a thin JAR file (only includes the project files). For `spark-daria`, the `sbt package` command builds the `target/scala-2.11/spark-daria-0.28.0.jar` file.

`sbt assembly` builds a fat JAR file (includes all the project and dependency files). For `spark-daria`, the `sbt assembly` command builds the `target/scala-2.11/spark-daria-assembly-0.28.0.jar` file.

Read this blog post on [Building Spark JAR files](https://www.mungingdata.com/apache-spark/building-jar-files-with-sbt) for a detailed discussion on how `sbt package` and `sbt assembly` differ. To further customize JAR files, read this blog post on [shading dependencies](https://www.mungingdata.com/apache-spark/shading-dependencies-with-sbt).

You should be comfortable with developing Spark code in a text editor, packaging your project as a JAR file, and attaching your JAR file to a cloud cluster for production analyses.

## `sbt clean`

The `sbt clean` command deletes all of the generated files in the `target/` directory.

This command will delete the documentation generated by `sbt doc` and will delete the JAR files generated by `sbt package` / `sbt assembly`.

It's good to run `sbt clean` frequently, so you don't accumlate a lot of legacy clutter in the `target/` directory.

## Next steps

SBT is a great build tool for Spark projects.

It lets you easily run tests, generate documentation, and package code as JAR files.

In a future post, we'll investigate how [Mill](https://github.com/lihaoyi/mill) can be used as a build tool for Spark projects.
