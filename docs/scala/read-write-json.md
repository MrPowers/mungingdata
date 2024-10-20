---
title: "Reading and writing JSON with Scala"
date: "2020-12-17"
categories: 
  - "scala"
---

This blog post explains how to read and write JSON with Scala using the [uPickle / uJSON library](https://github.com/lihaoyi/upickle).

This library makes it easy to work with JSON files in Scala.

The Scala JSON ecosystem is disjointed. Many popular Scala JSON libraries are hard to use.

Luckily for us, we don't need to do an exaustive analysis of all the libraries and figure out what's best. Li has already demonstrated that the alternative JSON libraries [have unintuitive the user interfaces](https://www.lihaoyi.com/post/uJsonfastflexibleandintuitiveJSONforScala.html).

Thankfully, Li created a clean solution, so we don't need to suffer.

## Adding the library

Add this to your `build.sbt` file:

```
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.9.5"
```

This dependency adds both the `upickle` and `ujson` packages to your project in one fell swoop.

## Reading JSON file

Suppose you have the following JSON file:

```
{
  "first_name": "Phil",
  "last_name": "Hellmuth",
  "birth_year": 1964
}
```

Here's how you can read this JSON data into a `LinkedHashMap`:

```
val jsonString = os.read(os.pwd/"src"/"test"/"resources"/"phil.json")
val data = ujson.read(jsonString)
data.value // LinkedHashMap("first_name" -> Str("Phil"), "last_name" -> Str("Hellmuth"), "birth_year" -> Num(1964.0))
```

The [os-lib](https://github.com/lihaoyi/os-lib) library is used to construct the path and read the file, [as detailed here](https://mungingdata.com/scala/filesystem-paths-move-copy-list-delete-folders/).

We can fetch the `first_name` value as follows:

```
data("first_name") // ujson.Value = Str("Phil")
data("first_name").str // String = "Phil"
data("first_name").value // Any = "Phil"
```

You need to fetch the value correctly to get the correct result type.

## Writing JSON file

Let's change the last name from the `phil.json` file to "Poker Brat" and then write the updated JSON to disk.

```
val jsonString = os.read(os.pwd/"src"/"test"/"resources"/"phil.json")
val data = ujson.read(jsonString)
data("last_name") = "Poker Brat"
os.write(os.pwd/"tmp"/"poker_brat.json", data)
```

Here are the contents of the `tmp/poker_brat.json` file:

```
{"first_name":"Phil","last_name":"Poker Brat","birth_year":1964}
```

This example demonstrates that `ujson` objects are mutable.

Some Scala JSON libraries try to stick with immutable data structures, but that forces inconvenient user interfaces that aren't as performant. uJSON made a concious design decision to use mutable data structures, so the user interface is intuitive.

## Reading JSON with an array

Suppose you have the following `colombia.json` file:

```
{
  "continent": "South America",
  "cities": ["Medellin", "Cali", "Bogotá"]
}
```

You can read this JSON file as follows:

```
val jsonString = os.read(os.pwd/"src"/"test"/"resources"/"colombia.json")
val data = ujson.read(jsonString)
```

You can interact with the cities array as follows:

```
data("cities") // ujson.Value = Arr(ArrayBuffer(Str("Medellin"), Str("Cali"), Str("Bogot\u00e1")))
data("cities").value // Any = ArrayBuffer(Str("Medellin"), Str("Cali"), Str("Bogot\u00e1"))
data("cities").arr // collection.mutable.ArrayBuffer[ujson.Value] = ArrayBuffer(Str("Medellin"), Str("Cali"), Str("Bogot\u00e1"))
data("cities").arr.map(_.str) // collection.mutable.ArrayBuffer[String] = ArrayBuffer("Medellin", "Cali", "Bogot\u00e1")
```

A mutable collection is returned, so you can easily modify the array.

## Writing JSON with an array

Let's read the `colombia.json` file, add a city to the array, and write it out as a separate JSON file.

```
val jsonString = os.read(os.pwd/"src"/"test"/"resources"/"colombia.json")
val data = ujson.read(jsonString)
data("cities").arr.append("Cartagena")
os.write(os.pwd/"tmp"/"more_colombia.json", data)
```

Here are the contents of the `more_colombia.json` file:

```
{"continent":"South America","cities":["Medellin","Cali","Bogotá","Cartagena"]}
```

uJSON makes it easy to modify an array and write out a JSON file.

## Creating JSON in memory

We've been constructing uJSON objects by reading files on disk. Let's built a JSON object in memory and then write it out to disk.

```
val brasil = ujson.Obj("population" -> "210 million")
brasil("cities") = ujson.Arr("recife", "sao paolo")
os.write(os.pwd/"tmp"/"brasil.json", brasil)
```

Here are the contents of the `brasil.json` file:

```
{"population":"210 million","cities":["recife","sao paolo"]}
```

## JSON ecosystem messiness

[json4s](https://github.com/json4s/json4s) aims to create a single Abstract Syntax Tree for Scala JSON libraries because "there are at least 6 JSON libraries for Scala, not counting the Java JSON libraries".

There was a native `scala.util.parsing` package with JSON parsing capabilities, but it was removed from the standard library in Scala 2.11. You can access this package with a separate import, but `scala.util.parsing.json.JSON` is deprecated as of Scala 2.13. Needless to say, stay away from this package.

It's fitting for new Scala JSON libraries like [circe](https://github.com/circe/circe) self identify as "Yet another JSON library for Scala".

## Conclusion

os-lib and ujson make it easy to read and write JSON files with Scala.

It's painful to work with JSON and Scala without these libraries.

Read [this article](https://www.lihaoyi.com/post/HowtoworkwithJSONinScala.html) or [Hands on Scala Programming](https://www.handsonscala.com/) to learn more details about how ujson is implemented and other use cases.

Notice how Li created a Python-like / Ruby-like clean interface for this library. His libraries always try to port elegant user inferfaces from popular Python libraries to Scala.

You should strive to build elegant user interfaces when building Scala libraries. If you want to build a popular open source Scala project, take a popular Python project, and port it over to Scala with an identical user interface.

Notice that ujson does not have a complicated import interface. You can type `ujson.read(jsonString)` without doing any imports. Always strive to provide minimalistic import interfaces for your users. Complicated imports are indicative of leaky abstractions.
