---
title: "Building Spark Projects with Mill"
date: "2019-04-04"
categories: 
  - "apache-spark"
---

# Building Spark Projects with Mill

Mill is a SBT alternative that can be used to build Spark projects.

This post explains how to create a Spark project with Mill and why you might want to use it instead of SBT.

## Project structure

Here's is the directory structure of the `mill_spark_example` project:

```
mill_spark_example/
  foo/
    src/
      Example.scala
  test/
    src/
      ExampleTests.scala
  out/
  build.sc
```

The `build.sc` file specifies the project dependencies, similar to the `build.sbt` file for SBT projects.

## Running a test

Let's add a simple DataFrame transformation to the `foo/src/Example.scala` file:

```
package foo

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.functions.removeAllWhitespace

object Example {

  def withGreeting()(df: DataFrame): DataFrame = {
    df.withColumn("greeting", removeAllWhitespace(lit("hello YOU !")))
  }

}
```

Now let's add a test in `foo/test/src/ExampleTests.scala`:

```
package foo

import utest._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

object ExampleTests extends TestSuite with SparkSessionTestWrapper with DatasetComparer {

  val tests = Tests {

    import spark.implicits._

    "withGreeting" - {

      val sourceDF = spark.createDF(
        List(
          ("jose"),
          ("li"),
          ("luisa")
        ), List(
          ("name", StringType, true)
        )
      )

      val actualDF = sourceDF.transform(Example.withGreeting())

      val expectedDF = Seq(
        ("jose", "helloYOU!"),
        ("li", "helloYOU!"),
        ("luisa", "helloYOU!")
      ).toDF("name", "greeting")

      assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)

    }

  }

}
```

The `foo/src/Example.scala` needs access to Spark SQL and spark-daria. The `foo/test/src/ExampleTests.scala` file needs access to Spark SQL, uTest, spark-daria, and spark-fast-tests.

Let's create a simple `build.sc` file that specifies the application and test dependencies.

```
import mill._
import mill.scalalib._
import coursier.maven.MavenRepository

object foo extends ScalaModule {
  def scalaVersion = "2.11.12"

  def repositories = super.repositories ++ Seq(
    MavenRepository("http://dl.bintray.com/spark-packages/maven")
  )

  def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.3.0",
    ivy"mrpowers:spark-daria:0.26.1-s_2.11",
  )

  object test extends Tests{
    def ivyDeps = Agg(
      ivy"org.apache.spark::spark-sql:2.3.0",
      ivy"com.lihaoyi::utest:0.6.0",
      ivy"MrPowers:spark-fast-tests:0.17.1-s_2.11",
      ivy"mrpowers:spark-daria:0.26.1-s_2.11",
    )
    def testFrameworks = Seq("utest.runner.Framework")
  }
}
```

We can run our tests with the `mill foo.test` command.

## Thin JAR file

The `mill foo.jar` command builds a thin JAR file that gets written out to `out/foo/jar/dest/out.jar`.

We can use the `jar` command to see that the JAR file only includes the `Example` code:

```
$ jar tvf out/foo/jar/dest/out.jar
    49 Thu Apr 04 11:21:08 EDT 2019 META-INF/MANIFEST.MF
  1265 Thu Apr 04 09:24:30 EDT 2019 foo/Example$.class
   843 Thu Apr 04 09:24:30 EDT 2019 foo/Example.class
```

## Fat JAR file

The `mill foo.assembly` command builds a fat JAR file that gets written out to `out/foo/assembly/dest/out.jar`.

The fat JAR file contains all the Spark, Scala, spark-daria, and project classes.

Let's update the `build.sc` file, so the assembly JAR file does not contain any Spark SQL or Scala classes.

```
import mill._
import mill.scalalib._
import mill.modules.Assembly
import coursier.maven.MavenRepository

object foo extends ScalaModule {
  def scalaVersion = "2.11.12"

  def repositories = super.repositories ++ Seq(
    MavenRepository("http://dl.bintray.com/spark-packages/maven")
  )

  def compileIvyDeps = Agg(
    ivy"org.apache.spark::spark-sql:2.3.0"
  )

  def ivyDeps = Agg(
    ivy"mrpowers:spark-daria:0.26.1-s_2.11"
  )

  def assemblyRules = Assembly.defaultRules ++
    Seq("scala/.*", "org\\.apache\\.spark/.*")
      .map(Assembly.Rule.ExcludePattern.apply)

  object test extends Tests{
    def ivyDeps = Agg(
      ivy"org.apache.spark::spark-sql:2.3.0",
      ivy"com.lihaoyi::utest:0.6.0",
      ivy"MrPowers:spark-fast-tests:0.17.1-s_2.11",
      ivy"mrpowers:spark-daria:0.26.1-s_2.11",
    )
    def testFrameworks = Seq("utest.runner.Framework")
  }
}
```

## Is Mill better?

TODO

- Figure out if Mill runs a test suite faster
- Figure out if Mill can generate JAR files faster
