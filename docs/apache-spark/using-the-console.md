---
title: "How to use the Spark Shell (REPL)"
date: "2019-07-23"
categories: 
  - "apache-spark"
---

The Spark console is a great way to run Spark code on your local machine.

You can easily create a DataFrame and play around with code in the Spark console to avoid spinning up remote servers that cost money!

<iframe width="560" height="315" src="https://www.youtube.com/embed/6BeLHKlwols" allowfullscreen></iframe>

## Starting the console

[Download Spark](https://spark.apache.org/downloads.html) and run the `spark-shell` executable command to start the Spark console. Consoles are also known as read-eval-print loops (REPL).

I store my Spark versions in the `~/Documents/spark` directory, so I can start my Spark shell with this command.

```
bash ~/Documents/spark/spark-2.3.0-bin-hadoop2.7/bin/spark-shell
```

## Important variables accessible in the console

The Spark console creates a `sc` variable to access the `SparkContext` and a `spark` variable to access the `SparkSession`.

You can use the `spark` variable to read a CSV file on your local machine into a DataFrame.

```
val df = spark.read.csv("/Users/powers/Documents/tmp/data/silly_file.csv")
```

You can use the `sc` variable to convert a sequence of `Row` objects into a RDD:

```
import org.apache.spark.sql.Row

sc.parallelize(Seq(Row(1, 2, 3)))
```

The Spark console automatically runs `import spark.implicits._` when it starts, so you have access to handy methods like `toDF()` and the shorthand `$` syntax to create column objects. We can easily create a column object like this: `$"some_column_name"`.

## Console commands

The `:quit` command stops the console.

The `:paste` lets the user add multiple lines of code at once. Here's an example:

```
scala> :paste
// Entering paste mode (ctrl-D to finish)

val y = 5
val x = 10
x + y

// Exiting paste mode, now interpreting.

y: Int = 5
x: Int = 10
res8: Int = 15
```

The `:help` command lists all the available console commands. Here's a full list of all the console commands.

```
scala> :help
All commands can be abbreviated, e.g., :he instead of :help.
:edit <id>|<line>        edit history
:help [command]          print this summary or command-specific help
:history [num]           show the history (optional num is commands to show)
:h? <string>             search the history
:imports [name name ...] show import history, identifying sources of names
:implicits [-v]          show the implicits in scope
:javap <path|class>      disassemble a file or class name
:line <id>|<line>        place line(s) at the end of history
:load <path>             interpret lines in a file
:paste [-raw] [path]     enter paste mode or paste a file
:power                   enable power user mode
:quit                    exit the interpreter
:replay [options]        reset the repl and replay all previous commands
:require <path>          add a jar to the classpath
:reset [options]         reset the repl to its initial state, forgetting all session entries
:save <path>             save replayable session to a file
:sh <command line>       run a shell command (result is implicitly => List[String])
:settings <options>      update compiler options, if possible; see reset
:silent                  disable/enable automatic printing of results
:type [-v] <expr>        display the type of an expression without evaluating it
:kind [-v] <expr>        display the kind of expression's type
:warnings                show the suppressed warnings from the most recent line which had any
```

[This Stackoverflow answer](https://stackoverflow.com/a/32808382/1125159) contains a good description of the available console commands.

## Starting the console with a JAR file

The Spark console can be initiated with a JAR files as follows:

```
bash ~/Documents/spark/spark-2.3.0-bin-hadoop2.7/bin/spark-shell --jars ~/Downloads/spark-daria-2.3.0_0.24.0.jar
```

You can download the spark-daria JAR file [on this release page](https://github.com/MrPowers/spark-daria/releases/tag/v2.3.0_0.24.0) if you'd like to try for yourself.

Let's access the `EtlDefinition` class in the console to make sure that the spark-daria namespace was successfully added to the console.

```
scala> com.github.mrpowers.spark.daria.sql.EtlDefinition
res0: com.github.mrpowers.spark.daria.sql.EtlDefinition.type = EtlDefinition
```

You can add a JAR file to an existing console session with the `:require` command.

```
:require /Users/powers/Downloads/spark-daria-2.3.0_0.24.0.jar
```

## Next steps

The Spark console is a great way to play around with Spark code on your local machine.

Try reading the [Introdution to Spark DataFrames](https://www.mungingdata.com/apache-spark/introduction-to-dataframes) post and pasting in all the examples to a Spark console as you go. It'll be a great way to learn about the Spark console and DataFrames!
