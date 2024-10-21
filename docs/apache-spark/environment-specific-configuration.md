---
title: "Environment Specific Config in Spark Scala Projects"
date: "2018-05-08"
categories: 
  - "apache-spark"
---

# Environment Specific Config in Spark Scala Projects

Environment config files return different values for the test, development, staging, and production environments.

In Spark projects, you will often want a variable to point to a local CSV file in the test environment and a CSV file in S3 in the production environment.

This episode will demonstrate how to add environment config to your projects and how to set environment variables to change the environment.

<iframe width="560" height="315" src="https://www.youtube.com/embed/aRbxcLgs7YA" allowfullscreen></iframe>

## Basic use case

Let's create a `Config` object with one `Map[String, String]` with test configuration and another `Map[String, String]` with production config.

```
package com.github.mrpowers.spark.spec.sql

object Config {

  var test: Map[String, String] = {
    Map(
      "libsvmData" -> new java.io.File("./src/test/resources/sample_libsvm_data.txt").getCanonicalPath,
      "somethingElse" -> "hi"
    )
  }

  var production: Map[String, String] = {
    Map(
      "libsvmData" -> "s3a://my-cool-bucket/fun-data/libsvm.txt",
      "somethingElse" -> "whatever"
    )
  }

  var environment = sys.env.getOrElse("PROJECT_ENV", "production")

  def get(key: String): String = {
    if (environment == "test") {
      test(key)
    } else {
      production(key)
    }
  }

}
```

The `Config.get()` method will grab values from the `test` or `production` map depending on the `PROJECT_ENV` value.

Let's use the `sbt console` command to demonstrate this.

```
$ PROJECT_ENV=test sbt console
scala> com.github.mrpowers.spark.spec.sql.Config.get("somethingElse")
res0: String = hi
```

Let's restart the SBT console and run the same code in the production environment.

```
$ PROJECT_ENV=production sbt console
scala> com.github.mrpowers.spark.spec.sql.Config.get("somethingElse")
res0: String = whatever
```

Here is how the `Config` object can be used to fetch a file in your GitHub repository in the test environment and also fetch a file from S3 in the production environment.

```
val training = spark
  .read
  .format("libsvm")
  .load(Config.get("libsvmData"))
```

This solution is elegant and does not clutter our application code with environment logic.

## Environment specific code anitpattern

Here is an example of how you should not add environment paths to your code.

```
var environment = sys.env.getOrElse("PROJECT_ENV", "production")
val training = if (environment == "test") {
  spark
    .read
    .format("libsvm")
    .load(new java.io.File("./src/test/resources/sample_libsvm_data.txt").getCanonicalPath)
} else {
  spark
    .read
    .format("libsvm")
    .load("s3a://my-cool-bucket/fun-data/libsvm.txt")
}
```

> An anti-pattern is a common response to a recurring problem that is usually ineffective and risks being highly counterproductive. - [source](https://en.wikipedia.org/wiki/Anti-pattern)

You should never write code with different execution paths in the production and test environments because then your test suite won't really be testing the actual code that's run in production.

## Overriding config

The `Config.test` and `Config.production` maps are defined as variables (with the `var` keyword), so they can be overridden.

```
scala> import com.github.mrpowers.spark.spec.sql.Config
scala> Config.get("somethingElse")
res1: String = hi

scala> Config.test = Config.test ++ Map("somethingElse" -> "give me clean air")
scala> Config.get("somethingElse")
res2: String = give me clean air
```

Giving users the ability to swap out config on the fly makes your codebase more flexible for a variety of use cases.

## Setting the `PROJECT_ENV` variable for test runs

The `Config` object uses the production environment by default. You're not going to want to have to remember to set the `PROJECT_ENV` to test everytime you run your test suite (e.g. you don't want to type `PROJECT_ENV=test sbt test`).

You can update your `build.sbt` file as follows to set `PROJECT_ENV` to test whenever the test suite is run.

```
fork in Test := true
envVars in Test := Map("PROJECT_ENV" -> "test")
```

Big thanks to the StackOverflow community for [helping me figure this out](https://stackoverflow.com/questions/39902049/setting-environment-variables-when-running-scala-sbt-test-suite?rq=1).

## Other implementations

[This StackOverflow thread](https://stackoverflow.com/questions/21607745/specific-config-by-environment-in-scala) discusses other solutions.

One answer relies on an external library, one is in Java, and one doesn't allow for overrides. I will add an answer with the implementation discussed in this blog post now.

## Next steps

Feel free to extend this solution to account for other environments. For example, you might want to add a staging environment that uses different paths to test code before it's run in production.

Just remember to follow best practices and avoid the config anti-pattern that can litter your codebase and reduce the protection offered by your test suite.

Adding `Config` objects to your functions adds a dependency you might not want. In a future blog post, we'll discuss how dependency injection can abstract these `Config` depencencies and how the `Config` object can be leveraged to access smart defaults - the best of both worlds!
