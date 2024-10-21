---
title: "Wrapping Java Code with Clean Scala Interfaces"
date: "2020-12-23"
categories: 
  - "scala"
---

# Wrapping Java Code with Clean Scala Interfaces

This post explains how to wrap a Java library with a Scala interface.

You can instantiate Java classes directly in Scala, but it's best to wrap the Java code, so you don't need to interface with it directly. Scala wrappers help hide the Java messiness.

os-lib is a great example of a project that [hides the underlying Java messiness of common filesystem operations](https://mungingdata.com/scala/filesystem-paths-move-copy-list-delete-folders/). It's a game changing lib to work with. Hiding verbose interfaces can add tons of value.

[commonmark-java](https://github.com/commonmark/commonmark-java) is a great library for converting Markdown strings into HTML. Let's wrap this library in a single Scala function that'll take a Markdown string and return a HTML string.

## Java interface

Here's the basic commonmark-java usage, per the README:

```
import org.commonmark.node.*;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

Parser parser = Parser.builder().build();
Node document = parser.parse("This is *Sparta*");
HtmlRenderer renderer = HtmlRenderer.builder().build();
renderer.render(document);  // "<p>This is <em>Sparta</em></p>\n"
```

That's a lot of imports and object instantiations to convert a Markdown string to a HTML string.

The `Parser.builder().build()` chunk isn't even a typo - that's really the interface that's being exposed to end users.

Let's see if we can clean this up.

## Include the library

Start by including the commonmark-java project in your `build.sbt` file:

```
libraryDependencies += "com.atlassian.commonmark" % "commonmark" % "0.16.1"
```

Scala libraries are included with two percent signs between the `groupId` and the `artifactId`, for example `libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"`. The `%%` syntax is because Scala projects are cross compiled for different versions of Scala (e.g. one release for Scala 2.12 and another release for Scala 2.13).

Java projects are not cross compiled, so they don't need the double `%%`.

## Create a Scala wrapper

Let's create a single function that takes a Markdown string argument and returns a HTML string.

```
object CommonmarkWrapper {

  def renderMd(text: String) = {
    val parser = org.commonmark.parser.Parser.builder().build()
    val document = parser.parse(text)
    val renderer = org.commonmark.renderer.html.HtmlRenderer.builder().build()
    renderer.render(document)
  }

}
```

See how we've added `val` to the Java code to build syntactically correct Scala code.

Let's see how this library can be used:

```
CommonmarkWrapper.renderMd("This is *Sparta*") // "<p>This is <em>Sparta</em></p>\n"
```

All of the Java messiness is now being hidden from end users. They can just call the `renderMd` method without worrying out instantiating a bunch of different objects.

## Scala, the better Java

A lot of early Scala adopters were just looking for a "better Java". Something similar to Java, but less verbose.

Scala is now losing part of the "better Java" market share to Kotlin, but the idea of providing elegant interfaces via Scala still remains.

You don't need to suffer with verbose Java code in a Scala codebase. Wrap the Java messiness in a file that's hidden away from the rest of your Scala code.

Succinct Scala makes for a readable codebase.
