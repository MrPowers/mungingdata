---
title: "Testing Spark Applications with uTest"
date: "2018-04-02"
categories: 
  - "apache-spark"
---

# Testing Spark Applications with uTest

The uTest Scala testing framework can be used to elegantly test your Spark code.

The other popular Scala testing frameworks (Scalatest and Specs2) provide multiple different ways to solve the same thing, whereas uTest only provides one way to solve common testing problems.

Spark users face a lot of choices when testing Spark code (testing framework customizations, creating Spark DataFrames, comparing DataFrame equality) and uTest does a great job making some choices for you and limiting your menu of options.

Let's dive in an see how uTest can be used to test Scala and Spark code.

<iframe width="560" height="315" src="https://www.youtube.com/embed/3lii47GjMEI" allowfullscreen></iframe>

## Testing Scala code with uTest

Let's start with the basics and show how uTest can test some pure Scala code.

Add uTest to the `build.sbt` file to add this testing framework to your project.

```
libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("utest.runner.Framework")
```

Create a `Calculator` object with an `add()` method that adds two integers.

```
package com.github.mrpowers.spark.daria.utils

object Calculator {

  def add(x: Int, y: Int): Int = {
    x + y
  }

}
```

Create a `CalculatorTest` file to test the `Calculator.add()` method.

```
package com.github.mrpowers.spark.daria.utils

import utest._

object CalculatorTest extends TestSuite {

  val tests = Tests {

    'add - {

      "adds two integers" - {
        assert(Calculator.add(2, 3) == 5)
      }

    }

  }

}
```

Important observations about a uTest test file:

- `import utest._` provides access to a `TestSuite` trait that all uTest test files must mix in
- `CalculatorTest` must be an `object` and cannot be a `class`
- `val tests = Tests` must be included in each test file
- `sbt test` runs all the test files in the project, just like other frameworks
- `sbt "testOnly -- com.github.mrpowers.spark.daria.utils.CalculatorTest"` only runs the tests in the `CalculatorTest` file
- `sbt "testOnly -- com.github.mrpowers.spark.daria.utils.CalculatorTest.add"` only runs the tests in the `'add` expression
- `'add` is an example of a [Scala symbol](https://stackoverflow.com/questions/3554362/purpose-of-scalas-symbol). We could also use a string instead (i.e. `"add"` works too).

## Testing Spark code with uTest

[spark-testing-base](https://github.com/holdenk/spark-testing-base/) and [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) are the two most popular libraries with helper functions for testing Spark code.

[spark-testing-base](https://github.com/holdenk/spark-testing-base/) only works with the Scalatest framework, so you'll need to use [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) or write your own test helper methods when using the uTest framework.

Let's see how uTest and [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) are used to test the `removeAllWhitespace()` function in the [spark-daria](https://github.com/MrPowers/spark-daria) project. Here's the `removeAllWhitespace()` function definition.

```
object functions {

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

}
```

The [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) `assertColumnEquality` method can be used to verify the equality of two columns in a DataFrame.

You can create a DataFrame, add a column that removes all the whitespace with the `removeAllWhitespace` function, and compare the actual column that's appended with your expectations.

The [spark-daria](https://github.com/MrPowers/spark-daria) `createDF` method is used to create the DataFrame in this test.

```
object FunctionsTest
    extends TestSuite
    with ColumnComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'removeAllWhitespace - {

      "removes all whitespace from a string" - {

        val df = spark.createDF(
          List(
            ("Bruce   willis   ", "Brucewillis"),
            ("    obama", "obama"),
            ("  nice  hair person  ", "nicehairperson"),
            (null, null)
          ), List(
            ("some_string", StringType, true),
            ("expected", StringType, true)
          )
        ).withColumn(
            "some_string_without_whitespace",
            functions.removeAllWhitespace(col("some_string"))
          )

        assertColumnEquality(df, "expected", "some_string_without_whitespace")

      }

    }

  }

}
```

We can also test this function by creating two DataFrames and verifying equality with the [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) `assertSmallDataFrameEquality` method.

```
object FunctionsTest
    extends TestSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  val tests = Tests {

    'removeAllWhitespace - {

      "removes all whitespace from a string with a column argument" - {

        val sourceDF = spark.createDF(
          List(
            ("Bruce   willis   "),
            ("    obama"),
            ("  nice  hair person  "),
            (null)
          ), List(
            ("some_string", StringType, true)
          )
        )

        val actualDF = sourceDF.withColumn(
          "some_string_without_whitespace",
          functions.removeAllWhitespace(col("some_string"))
        )

        val expectedDF = spark.createDF(
          List(
            ("Bruce   willis   ", "Brucewillis"),
            ("    obama", "obama"),
            ("  nice  hair person  ", "nicehairperson"),
            (null, null)
          ), List(
            ("some_string", StringType, true),
            ("some_string_without_whitespace", StringType, true)
          )
        )

        assertSmallDataFrameEquality(actualDF, expectedDF)

      }

    }

  }

}
```

The `assertSmallDataFrameEquality` requires more code than `assertColumnEquality`. `assertSmallDataFrameEquality` is also slower because you need to create two DataFrames instead of one. You should always use `assertColumnEquality` whenever possible.

## What testing framework should you use?

I think the uTest framework's design philosophy of only providing one way performing common tasks makes it the best framework for testing Spark code.

When testing Spark code you'll already face multiple decisions on if you'll use a library with Spark test helper methods ([spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) or [spark-testing-base](https://github.com/holdenk/spark-testing-base/)) or if you'll create your own test helper methods. You don't need more decisions about the testing framework that you'll use.

uTest also forces multiple teams to define tests and make assertions the same way. You won't have one team that uses `===`, another team that uses `must beEqual`, and a third team that uses `should be` for equality comparisons.

If you're forced to use Scalatest (because you need to leverage some [spark-testing-base](https://github.com/holdenk/spark-testing-base/) features), you should use [FreeSpec](http://doc.scalatest.org/3.0.1/#org.scalatest.FreeSpec), so the tests look like uTest specs at least.
