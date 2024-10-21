---
title: "Shading Dependencies in Spark Projects with SBT"
date: "2018-09-23"
categories: 
  - "apache-spark"
---

# Shading Dependencies in Spark Projects with SBT

`sbt-assembly` makes it easy to shade dependencies in your Spark projects when you create fat JAR files. This blog post will explain why it's useful to shade dependencies and will teach you how to shade dependencies in your own projects.

## When shading is useful

Let's look at a snippet from the [spark-pika](https://github.com/MrPowers/spark-pika) `build.sbt` file and examine the JAR file that's constructed by `sbt assembly`.

You can read [this blog post on building JAR files with SBT](https://www.mungingdata.com/apache-spark/building-jar-files-with-sbt) if you need background information on JAR file basics before diving into shading, which is an advanced feature.

```
libraryDependencies += "mrpowers" % "spark-daria" % "2.3.1_0.24.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.3.1_0.15.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_2.11-${sparkVersion.value}_${version.value}.jar"
```

The `sbt assembly` command will create a JAR file that includes `spark-daria` and all of the `spark-pika` code. The JAR file won't include the `libraryDependencies` that are flagged with "provided" or "test" (i.e. `spark-sql`, `spark-fast-tests`, and `scalatest` won't be included in the JAR file). Let's verify the contents of the JAR file with the `jar tvf target/scala-2.11/spark-pika_2.11-2.3.1_0.0.1.jar` command.

```
0 Sun Sep 23 23:04:00 COT 2018 com/
0 Sun Sep 23 23:04:00 COT 2018 com/github/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/daria/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/daria/ml/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/daria/sql/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/daria/sql/types/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/daria/utils/
0 Sun Sep 23 23:04:00 COT 2018 com/github/mrpowers/spark/pika/
```

If the `spark-pika` fat JAR file is attached to a cluster, users will be able to access the `com.github.mrpowers.spark.daria` and `com.github.mrpowers.spark.pika` namespaces.

We don't want to provide access to the `com.github.mrpowers.spark.daria` namespace when `spark-pika` is attached to a cluster for two reasons:

1. It just feels wrong. When users attach the `spark-pika` JAR file to their Spark cluster, they should only be able to access the `spark-pika` namespace. Adding additional namespaces to the classpath is unexpected.
2. It prevents users from accessing a different `spark-daria` version than what's specified in the `spark-pika` `build.sbt` file. In this example, users are forced to use `spark-daria` version `2.3.1_0.24.0`.

## How to shade the `spark-daria` dependency

We can use SBT to change the `spark-daria` namespace for all the code that's used by `spark-pika`. `spark-daria` will still be in the fat JAR file, but the namespace will be different, so users can still attach their own version of `spark-daria` to the cluster.

Here is the code to shade the `spark-daria` dependency in `spark-pika`.

```
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.github.mrpowers.spark.daria.**" -> "shadedSparkDariaForSparkPika.@1").inAll
)
```

Let's run `sbt clean` and then rebuild the `spark-pika` JAR file with `sbt assembly`. Let's examine the contents of the new JAR file with `jar tvf target/scala-2.11/spark-pika_2.11-2.3.1_0.0.1.jar`.

```
0 Sun Sep 23 23:29:32 COT 2018 com/
0 Sun Sep 23 23:29:32 COT 2018 com/github/
0 Sun Sep 23 23:29:32 COT 2018 com/github/mrpowers/
0 Sun Sep 23 23:29:32 COT 2018 com/github/mrpowers/spark/
0 Sun Sep 23 23:29:32 COT 2018 com/github/mrpowers/spark/pika/
0 Sun Sep 23 23:29:32 COT 2018 shadedSparkDariaForSparkPika/
0 Sun Sep 23 23:29:32 COT 2018 shadedSparkDariaForSparkPika/ml/
0 Sun Sep 23 23:29:32 COT 2018 shadedSparkDariaForSparkPika/sql/
0 Sun Sep 23 23:29:32 COT 2018 shadedSparkDariaForSparkPika/sql/types/
0 Sun Sep 23 23:29:32 COT 2018 shadedSparkDariaForSparkPika/utils/
```

The JAR file used to contain the `com.github.mrpowers.spark.daria` namespace and that's now been replaced with a `shadedSparkDariaForSparkPika` namespace.

All the `spark-pika` references to `spark-daria` will use the `shadedSparkDariaForSparkPika` namespace.

Users can attach both `spark-daria` and `spark-pika` to the same Spark cluster now and there won't be a `com.github.mrpowers.spark.daria` namespace collision anymore.

## Conclusion

When creating Spark libraries, make sure to shade dependencies that are included in the fat JAR file, so your library users can specify different versions for dependencies at will. Try your best to design your libraries to only add a single namespace to the classpath when the JAR files is attached to a cluster.
