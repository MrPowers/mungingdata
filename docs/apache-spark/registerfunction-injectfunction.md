---
title: "Registering Native Spark Functions"
date: "2021-05-07"
categories: 
  - "apache-spark"
---

# Registering Native Spark Functions

This post explains how Spark registers native functions internally and the public facing APIs for you to register your own functions.

Registering native functions is important if you want to access functions via the SQL API. You don't need to register functions if you're using the PySpark or Scala DSLs.

This post is organized as follows:

- registering functions to an existing SparkSession
- registering functions via SparkSessionExt in Databricks
- command line workflow
- how Spark core registers functions

This is a low level post for advanced Spark users and talks about code written by Spark core maintainers. You'll need to grok it hard. Studying this code is highly recommended if you really want to understand how Spark works under the hood.

## Registering functions to existing SparkSession

The [itachi](https://github.com/yaooqinn/itachi) project provides access to Postgres / Presto SQL syntax in Spark. Let's look at how to use itachi and then dig into the implementation details.

Suppose you have the following DataFrame:

```
+------+------+
|  arr1|  arr2|
+------+------+
|[1, 2]|    []|
|[1, 2]|[1, 3]|
+------+------+
```

Concatenate the two arrays with the `array_cat` function that's defined in itachi:

```scala
yaooqinn.itachi.registerPostgresFunctions

spark
  .sql("select array_cat(arr1, arr2) as both_arrays from some_data")
  .show()
```

```
+------------+
| both_arrays|
+------------+
|      [1, 2]|
|[1, 2, 1, 3]|
+------------+
```

`array_cat` is [a Postgres function](https://w3resource.com/PostgreSQL/postgresql_array_cat-function.php) and itachi provides syntax that Postgres developers are familiar with. This makes for an easy transition to Spark.

itachi also defines an `age` function [similar to Postgres](https://www.postgresqltutorial.com/postgresql-age/).

Here's the `Age` Catalyst code:

```scala
case class Age(end: Expression, start: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = end

  override def right: Expression = start

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def nullSafeEval(e: Any, s: Any): Any = {
    DateTimeUtils.age(e.asInstanceOf[Long], s.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getCanonicalName.stripSuffix("$")
    defineCodeGen(ctx, ev, (ed, st) => s"$dtu.age($ed, $st)")
  }

  override def dataType: DataType = CalendarIntervalType
}
```

We need to take this Catalyst code and package it properly for `registerFunction` in Spark.

`registerFunction` takes three arguments:

- name: FunctionIdentifier
- info: ExpressionInfo
- builder: FunctionBuilder

itachi defines a package object that helps organize the required arguments:

```scala
package object extra {

  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)

  type Extensions = SparkSessionExtensions => Unit

}
```

Let's define the `FunctionDescription` for `Age`:

```scala
object Age {
  val fd: FunctionDescription = (
    new FunctionIdentifier("age"),
    ExpressionUtils.getExpressionInfo(classOf[Age], "age"),
    (children: Seq[Expression]) => if ( children.size == 1) {
      Age(CurrentDate(), children.head)
    } else {
      Age(children.head, children.last)
    })
}
```

Now register the function with the current SparkSession:

```
spark.sessionState.functionRegistry.registerFunction(Age.fd._1, Age.fd._2, Age.fd._3)
```

Library users don't need to concern themselves with any low level function registration of course. They can just attach itachi to their cluster and run the following line to access the Postgres-style SQL functions:

```
yaooqinn.itachi.registerPostgresFunctions
```

Big thanks to [Alex Ott](https://twitter.com/alexott_en?lang=en) for showing me this solution!

## registering functions via SparkSessionExt in Databricks

There is a SparkSessionExt#injectFunction method that also allows you to register functions. This approach isn't as user friendly because it can't register functions with an existing SparkSession. It can only register functions to a new SparkSession. Let's look at the workflow and the usability issues will be apparent.

Create an [init script](https://docs.databricks.com/clusters/init-scripts.html) in DBFS:

```
dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

dbutils.fs.put("/databricks/scripts/itachi-install.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/itachi_2.12-0.1.0.jar https://repo1.maven.org/maven2/com/github/yaooqinn/itachi_2.12/0.1.0/itachi_2.12-0.1.0.jar""", true)
```

Before starting the cluster, set the Spark config:

```scala
spark.sql.extensions org.apache.spark.sql.extra.PostgreSQLExtensions
```

Also set the DBFS file path to the init script before starting the cluster:

```
dbfs:/databricks/scripts/itachi-install.sh
```

You can now attach a notebook to the cluster using Postgres SQL syntax.

This workflow is nice in some circumstances because all notebooks attached to the cluster can access the Postgres syntax without doing any imports. In general, it's much easier to register functions to an existing SparkSession, like we did with the first approach.

All you need to write to support the spark.sql.extensions approach is a class that extends `Extensions`:

```scala
class PostgreSQLExtensions extends Extensions {
  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectFunction(Age.fd)
  }
}
```

`Age.fd` returns the tuple that `injectFunction` needs.

## command line workflow

It's work noting that the `SparkSessionExt` approach isn't as bulky on the command line (compared to Databricks);

```
bin/spark-sql --packages com.github.yaooqinn:itachi_2.12:0.1.0 --conf spark.sql.extensions=org.apache.spark.sql.extra.PostgreSQLExtensions
```

`SparkSessionExt` may be easier for runtimes other than Databricks that don't require init scripts.

## How Spark registers native functions

Here's a snippet of the Spark `FunctionRegistry` code:

```scala
object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    expression[Abs]("abs"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    // all the other Spark SQL functions
  )

  private def expression[T <: Expression : ClassTag](name: String, setAlias: Boolean = false)
      : (String, (ExpressionInfo, FunctionBuilder)) = {
    val (expressionInfo, builder) = FunctionRegistryBase.build[T](name)
    val newBuilder = (expressions: Seq[Expression]) => {
      val expr = builder(expressions)
      if (setAlias) expr.setTagValue(FUNC_ALIAS, name)
      expr
    }
    (name, (expressionInfo, newBuilder))
  }

  // bunch of other stuff

}
```

The `expression` method is passed an expression class name and a function name and returns a `String`, `ExpressionInfo`, and `FunctionBuilder`.

The code eventually loops over the `expressions` map and registers each function.

```scala
expressions.foreach {
  case (name, (info, builder)) => fr.registerFunction(FunctionIdentifier(name), info, builder)
}
```

We've made a full loop back to `registerFunction` in the original approach ;)

## Thanks

Big shout out to cloud-fan for [helping with my basic questions](https://github.com/yaooqinn/itachi/issues/8#issuecomment-830752760). Thanks to yaooqinn for making [itachi](https://github.com/yaooqinn) and Sim [for also talking about registering native functions](https://blog.simeonov.com/2018/11/14/apache-spark-native-functions/).

The Spark open source community is a pleasure to work with.

## Conclusion

Spark doesn't make it easy to register native functions, but this isn't a common task, so the existing interfaces are fine.

Registering Spark native functions is only for advanced Spark programmers that are comfortable writing Catalyst expressions and want to expose functionality via the SQL API (rather than the Python / Scala DSLs).

The `SparkSessionExt#injectFunction` approach is problematic because it's difficult to use and init scripts are fragile.

`spark.sessionState.functionRegistry.registerFunction` is a better approach because it gives end users a smoother interface.

itachi shows how this design pattern can provide powerful functionality to end users.

A company with Spark experts and SQL power users could also benefit from this design pattern. Spark experts can register native SQL functions with for custom business use cases and hypercharge the productivity of the SQL analysts.
