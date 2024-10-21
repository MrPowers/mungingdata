---
title: "Scala Templates with Scalate, Mustache, and SSP"
date: "2020-10-30"
categories: 
  - "scala"
---

# Scala Templates with Scalate, Mustache, and SSP

The [scalate](https://github.com/scalate/scalate) library makes it easy to use Mustache or SSP templates with Scala.

This blog post will show how to use Mustache and SSP templates and compares the different templating philosophies (Mustache is logic-less and SSP templates contain logic).

There are pros / cons to the different templating styles. Luckily scalate makes it easy for you to use either style.

## Simple Mustache example

Suppose you have the following `simple_example.mustache` file:

```
I like {{programming_language}}
The code is {{code_description}}
```

Here's how to generate a file with `programming_language` set to "Scala" and `code_description` set to "pretty".

```
import org.fusesource.scalate.TemplateEngine

val sourceDataPath = new java.io.File("./src/test/resources/simple_example.mustache").getCanonicalPath
val engine = new TemplateEngine
val someAttributes = Map(
  "programming_language" -> "Scala",
  "code_description" -> "pretty"
)
engine.layout(sourceDataPath, someAttributes)
```

Here's the string that's returned:

```
I like Scala
The code is pretty
```

## Add library to SBT

You need to add the following line to your `build.sbt` file to access the scalate library:

```
libraryDependencies += "org.scalatra.scalate" %% "scalate-core" % "1.9.6"
```

The `import org.fusesource.scalate.TemplateEngine` import statement and `org.scalatra.scalate` package name aren't aligned like they are for most projects. Just keep that in mind when you're using the library.

## More Mustache examples

### Lists

Let's look at how to render a list of values via a Mustache template. Suppose you'd like to display a bulleted list of popular Scala projects.

Here's a Mustache template that'll display a list of values:

```
{{#popular_projects}}
  <b>{{name}}</b>
{{/popular_projects}}
```

And here is the Scala code:

```
val sourceDataPath = new java.io.File("./src/test/resources/scala_projects.mustache").getCanonicalPath
val engine = new TemplateEngine
val someAttributes = Map(
  "popular_projects" -> List(
    Map("name" -> "Spark"),
    Map("name" -> "Play"),
    Map("name" -> "Akka")
  )
)
engine.layout(sourceDataPath, someAttributes)
```

It'll return this string:

```
<b>Spark</b>
<b>Play</b>
<b>Akka</b>
```

### Boolean values

You can include boolean values to imitate if statement logic.

Here's a Mustache template that'll add "probably likes functional programming" to a list if the `loves_scala?` flag is set to true.

```
**nerd profile**
* applies unix philosophy to human interactions
* dreams in binary
{{#loves_scala?}}
  * probably likes functional programming
{{/loves_scala?}}
```

Here's Scala code that uses this template and outputs a string:

```
val sourceDataPath = new java.io.File("./src/test/resources/boolean_example.mustache").getCanonicalPath
val engine = new TemplateEngine
val someAttributes = Map(
  "loves_scala?" -> true
)
println(engine.layout(sourceDataPath, someAttributes))
```

```
**nerd profile**
* applies unix philosophy to human interactions
* dreams in binary
* probably likes functional programming
```

### Functions

Mustache can even handle functions that take string arguments.

Here's a Mustache template that includes a function.

```
select
  {{#comma_delimited}}
  {{column_names}}
{{/comma_delimited}}
from
  {{table_name}}
```

Here's the Scala code that executes the function (it assumes the column names are passed in as a pipe delimited string).

```
val sourceDataPath = new java.io.File("./src/test/resources/function_example.mustache").getCanonicalPath
val engine = new TemplateEngine
val someAttributes = Map(
  "column_names" -> "first_name|last_name|age",
  "comma_delimited" -> ((cols: String) => cols.replaceAll("\\|", ",")),
  "table_name" -> "fun_people"
)
println(engine.layout(sourceDataPath, someAttributes))
```

Here's what the code outputs:

```
select
  first_name,last_name,age
from
  fun_people
```

From what I can tell, the functions can only accept a single string argument. They don't work if you supply a sequence of strings for example.

The Mustache template was intentionally formatted strangely to output the SQL code with proper indentation.

## SSP

SSP is another templating framework, similar to ERB in Ruby.

SSP does not follow the "logic-less templates" philosophy used by Mustache. You can add lots of logic to a SSP template.

Let's define an object that we'll access in our SSP template:

```
object FunStuff {
  val dinnertime = "eating stuff!"
}
```

Let's create a `simple_example.ssp` file that contains some arbitrary code and also accesses the `FunStuff` object.

```
<% import mrpowers.scalate.example.FunStuff  %>
<p>
    My message is "<%= List("hi", "there", "reader!").mkString(" ") %>"
    At dinnertime, I like <%= FunStuff.dinnertime %>
</p>
```

Notice that the template needs to import the `FunStuff` object.

Let's write some Scala code that'll use the template to generate a string.

```
val sourceDataPath = new java.io.File("./src/test/resources/simple_example.ssp").getCanonicalPath
val engine = new TemplateEngine
println(engine.layout(sourceDataPath))
```

Here's the string that's returned:

```
<p>
    My message is "hi there reader!"
    At dinnertime, I like eating stuff!
</p>
```

scalate is intelligent enough to recognize that the file extension is `ssp`, so the code should be processed with the SSP templating engine.

## Templating philosophies

Logic-less templates force you to keep logic out of the templates. This makes the templates approachable to folks that are comfortable modifying HTML / SQL logic, but aren't programmers. Templates with logic aren't approachable for non-coders.

[Here is a great blog](https://warpspire.com/posts/mustache-style-erb) on the benefits of logic-less templating.

The author argues that in the MVC context, it's best to keep logic in the views and keep the templates simple (with only HTML / mustaches).

## Tips on picking templating languages

The creator of scalate has some great tips on picking a templating language [in this slide deck](https://www.slideshare.net/strachaj/introducing-scalate-the-scala-template-engine) - see slide 18.

Key points:

- Use mustache if you'd like non-programmers to "own the templates"
- Use SSP if you'd like the option to do complex template hacking

## Other template options

scalate also supports Scaml (a Scala version of HAML) and Jade (another template engine inspired by HAML). There is [a separate Scalatags](https://www.lihaoyi.com/scalatags/) library to build XML / HTML / CSS.

These options are only applicable for HTML templating. I personally prefer writing HTML than using fancy templating engines.

## Conclusion

Scala offers great support for all different types of templating.

Templates are generally used to create HTML for websites, but they can also be used to generate SQL and other types of files.

Logic-less templating encourages a good separation of logic and makes the templates more approachable for semi-technical users, which is often desirable.

See the examples in [this repo](https://github.com/MrPowers/scalate-example) if you'd like working code snippets of all the code covered in this blog post.
