---
title: "Building Spark JAR Files with SBT"
date: "2018-04-06"
categories: 
  - "apache-spark"
---

# Building Spark JAR Files with SBT

Spark JAR files let you package a project into a single file so it can be run on a Spark cluster.

A lot of developers develop Spark code in brower based notebooks because they're unfamiliar with JAR files. Scala is a difficult language and it's especially challenging when you can't leverage the development tools provided by an IDE like IntelliJ.

This episode will demonstrate how to build JAR files with the SBT `package` and `assembly` commands and how to customize the code that's included in JAR files. Hopefully it will help you make the leap and start writing Spark code in SBT projects with a powerful IDE by your side!

<iframe width="560" height="315" src="https://www.youtube.com/embed/0yyw2gD0SrY" allowfullscreen></iframe>

## JAR File Basics

> A JAR (Java ARchive) is a package file format typically used to aggregate many Java class files and associated metadata and resources (text, images, etc.) into one file for distribution. - [Wikipedia](https://en.wikipedia.org/wiki/JAR_\(file_format\))

JAR files can be attached to Databricks clusters or launched via `spark-submit`.

You can build a "thin" JAR file with the `sbt package` command. Thin JAR files only include the project's classes / objects / traits and don't include any of the project dependencies.

You can build "fat" JAR files by adding [sbt-assembly](https://github.com/sbt/sbt-assembly) to your project. Fat JAR files inlude all the code from your project and all the code from the dependencies.

Let's say you add the uJson library to your `build.sbt` file as a library dependency.

```
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.6.5"
```

If you run `sbt package`, SBT will build a thin JAR file that only includes your project files. The thin JAR file will not include the uJson files.

If you run `sbt assembly`, SBT will build a fat JAR file that includes both your project files and the uJson files.

Let's dig into the gruesome details!

## Building a Thin JAR File

As discussed, the `sbt package` builds a thin JAR file of your project.

[spark-daria](https://github.com/MrPowers/spark-daria) is a good example of an open source project that is distributed as a thin JAR file. This is an excerpt of the spark-daria `build.sbt` file:

```
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"

libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "2.3.0_0.8.0" % "test"
libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.3" % "test"
testFrameworks += new TestFramework("utest.runner.Framework")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}
```

Important take-aways:

- The "provided" string at the end of the `libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"` line indicates that the spark-sql dependency should be provided by the runtime environment that uses this JAR file.
- The "test" string at the end of the `libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "2.3.0_0.8.0" % "test"` indicates that spark-fast-tests dependency is only for the test suite. The application code does not rely on spark-fast-tests, but spark-fast-tests is needed when running `sbt test`.
- The `artifactName := ...` line customizes the name of the JAR file created with the `sbt package` command. As discussed in the [spark-style-guide](https://github.com/MrPowers/spark-style-guide#jar-files), it's best to include the Scala version, Spark version, and project version in the JAR file name, so it's easier for your users to select the right JAR file for their projects.

The `sbt package` command creates the `target/scala-2.11/spark-daria_2.11-2.3.0_0.19.0.jar` JAR file. We can use the `jar tvf` command to inspect the contents of the JAR file.

```
$ jar tvf target/scala-2.11/spark-daria_2.11-2.3.0_0.19.0.jar

   255 Wed May 02 20:50:14 COT 2018 META-INF/MANIFEST.MF
     0 Wed May 02 20:50:14 COT 2018 com/
     0 Wed May 02 20:50:14 COT 2018 com/github/
     0 Wed May 02 20:50:14 COT 2018 com/github/mrpowers/
     0 Wed May 02 20:50:14 COT 2018 com/github/mrpowers/spark/
     0 Wed May 02 20:50:14 COT 2018 com/github/mrpowers/spark/daria/
     0 Wed May 02 20:50:14 COT 2018 com/github/mrpowers/spark/daria/sql/
     0 Wed May 02 20:50:14 COT 2018 com/github/mrpowers/spark/daria/utils/
  3166 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/DataFrameHelpers.class
  1643 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/DataFrameHelpers$$anonfun$twoColumnsToMap$1.class
   876 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/ColumnExt$.class
  1687 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/transformations$$anonfun$1.class
  3278 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/DataFrameColumnsChecker.class
  3607 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/functions$$typecreator3$1.class
  1920 Wed May 02 20:50:12 COT 2018 com/github/mrpowers/spark/daria/sql/DataFrameColumnsException$.class
  ...
  ...
```

The sbt-assembly plugin needs to be added to build fat JAR files that include the project's dependencies.

## Building a Fat JAR File

[spark-slack](https://github.com/MrPowers/spark-slack) is a good example of a project that's distributed as a fat JAR file. The spark-slack JAR file includes all of the spark-slack code and all of the code in two external libraries (`net.gpedro.integrations.slack.slack-webhook` and `org.json4s.json4s-native`).

Let's take a snippet from the spark-slack `build.sbt` file:

```
libraryDependencies ++= Seq(
  "net.gpedro.integrations.slack" % "slack-webhook" % "1.2.1",
  "org.json4s" %% "json4s-native" % "3.3.0"
)

libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v2.3.0_0.18.0" % "test"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v2.3.0_0.7.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion.value}_${version.value}.jar"
```

Important observations:

- `"net.gpedro.integrations.slack" % "slack-webhook" % "1.2.1"` and `"org.json4s" %% "json4s-native" % "3.3.0"` aren't flagged at "provided" or "test" dependencies, so they will be included in the JAR file when `sbt assembly` is run.
- `spark-daria`, `spark-fast-tests`, and `scalatest` are all flagged as "test" dependencies, so they won't be included in the JAR file.
- `assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)` means that the Scala code itself should not be included in the JAR file. Your Spark runtime environment should already have Scala setup.
- `assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion.value}_${version.value}.jar"` customizes the JAR file name that's created by `sbt assembly`. Notice that `sbt package` and `sbt assembly` require different code to customize the JAR file name.

Let's build the JAR file with `sbt assembly` and then inspect the content.

```
$ jar tvf target/scala-2.11/spark-slack_2.11-2.3.0_0.0.1.jar

     0 Wed May 02 21:09:18 COT 2018 com/
     0 Wed May 02 21:09:18 COT 2018 com/github/
     0 Wed May 02 21:09:18 COT 2018 com/github/mrpowers/
     0 Wed May 02 21:09:18 COT 2018 com/github/mrpowers/spark/
     0 Wed May 02 21:09:18 COT 2018 com/github/mrpowers/spark/slack/
     0 Wed May 02 21:09:18 COT 2018 com/github/mrpowers/spark/slack/slash_commands/
     0 Wed May 02 21:09:18 COT 2018 com/google/
     0 Wed May 02 21:09:18 COT 2018 com/google/gson/
     0 Wed May 02 21:09:18 COT 2018 com/google/gson/annotations/
     0 Wed May 02 21:09:18 COT 2018 com/google/gson/internal/
     0 Wed May 02 21:09:18 COT 2018 com/google/gson/internal/bind/
     0 Wed May 02 21:09:18 COT 2018 com/google/gson/reflect/
     0 Wed May 02 21:09:18 COT 2018 com/google/gson/stream/
     0 Wed May 02 21:09:18 COT 2018 com/thoughtworks/
     0 Wed May 02 21:09:18 COT 2018 com/thoughtworks/paranamer/
     0 Wed May 02 21:09:18 COT 2018 net/
     0 Wed May 02 21:09:18 COT 2018 net/gpedro/
     0 Wed May 02 21:09:18 COT 2018 net/gpedro/integrations/
     0 Wed May 02 21:09:18 COT 2018 net/gpedro/integrations/slack/
     0 Wed May 02 21:09:18 COT 2018 org/
     0 Wed May 02 21:09:18 COT 2018 org/json4s/
     0 Wed May 02 21:09:18 COT 2018 org/json4s/native/
     0 Wed May 02 21:09:18 COT 2018 org/json4s/prefs/
     0 Wed May 02 21:09:18 COT 2018 org/json4s/reflect/
     0 Wed May 02 21:09:18 COT 2018 org/json4s/scalap/
     0 Wed May 02 21:09:18 COT 2018 org/json4s/scalap/scalasig/
  1879 Wed May 02 21:09:14 COT 2018 com/github/mrpowers/spark/slack/Notifier.class
  1115 Wed May 02 21:09:14 COT 2018 com/github/mrpowers/spark/slack/SparkSessionWrapper$class.class
   683 Wed May 02 21:09:14 COT 2018 com/github/mrpowers/spark/slack/SparkSessionWrapper.class
  2861 Wed May 02 21:09:14 COT 2018 com/github/mrpowers/spark/slack/slash_commands/SlashParser.class
  ...
  ...
```

`sbt assembly` provides us with the `com/github/mrpowers/spark/slack`, `net/gpedro/`, and `org/json4s/` as expected. But why does our fat JAR file include `com/google/gson/` code as well?

If we look at the [net.gpedro `pom.xml` file](https://github.com/gpedro/slack-webhook/blob/master/pom.xml#L159-L165), we can see that the net.gpedro relies on com.google.code.gson:

```
<dependencies>
  <dependency>
    <groupId>com.google.code.gson</groupId>
    <artifactId>gson</artifactId>
    <version>${gson.version}</version>
  </dependency>
</dependencies>
```

You'll want to be very careful to minimize your project dependencies. You'll also want to rely on external libraries that have minimal dependencies themselves as the dependies of a library quickly become your dependencies as soon as you add the library to your project.

## Next Steps

Make sure to always mark your `libraryDependencies` with "provided" or "test" whenever possible to keep your JAR files as thin as possible.

Only add dependencies when it's absolutely required and try to avoid libraries that depend on a lot of other libraries.

It's very easy to find yourself in [dependency hell](https://en.wikipedia.org/wiki/Dependency_hell) with Scala and you should proactively avoid this uncomfortable situation.

Your Spark runtime environment should generally provide the Scala and Spark dependencies and you shouldn't include these in your JAR files.

I fought long and hard to develop the `build.sbt` strategies outlined in this episode. Hopefully this will save you from some headache!
