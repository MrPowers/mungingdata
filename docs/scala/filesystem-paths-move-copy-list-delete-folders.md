---
title: "Scala Filesystem Operations (paths, move, copy, list, delete)"
date: "2020-12-12"
categories: 
  - "scala"
---

Basic filesystem operations have traditionally been complex in Scala.

A simple operation like copying a file is a one-liner in some languages like Ruby, but a multi-line / multi-import mess if you rely on Java libraries.

Li thankfully created an [os-lib](https://github.com/lihaoyi/os-lib) project that makes Scala filesystem operations easy and intuitive. The library has a Ruby-like elegance.

Scala developers no longer need to mess around with low level Java libraries for basic filesystem operations.

## Moving a file

Suppose you're working in an SBT project with a `people.csv` file in this directory structure:

```
src/
  main/
  test/
    resources/
      people.csv
  tmp/
```

Here's how to copy `src/test/resources/people.csv` into the `tmp` directory with os-lib:

```
os.copy(
  os.pwd/"src"/"test"/"resources"/"people.csv",
  os.pwd/"tmp"/"people_copy.csv"
)
```

Here's the [top ranked Stackoverflow answer](https://stackoverflow.com/questions/2225214/scala-script-to-copy-files) when you Google "copy file Scala".

The top ranked answer uses `java.io`:

```
import java.io.{File,FileInputStream,FileOutputStream}
val src = new File(args(0))
val dest = new File(args(1))
new FileOutputStream(dest) getChannel() transferFrom(
    new FileInputStream(src) getChannel, 0, Long.MaxValue )
```

There's another answer that uses `java.nio`:

```
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy
import java.nio.file.Paths.get

implicit def toPath (filename: String) = get(filename)

copy (from, to, REPLACE_EXISTING)
```

The Java libraries require a bunch of imports and low level library understanding. You usually just want to copy a file and don't care about low level details.

The Java libraries also require some yak-shaving research. You'll need learn how to handle Java paths and investigate if the blocking / non-blocking APIs are better for your application.

The path / filesystem operations are both packed into os-lib, so you don't need to mess around with different packages.

## Copying a file in Ruby

The `os-lib` solution is similar to how you can copy a file in Ruby:

```
require 'fileutils'

FileUtils.cp(
  Dir.pwd + "/src/test/resources/people.csv",
  Dir.pwd + "/tmp/people_copy.csv"
)
```

Ruby is know for having a great filesystem API and it's a great accomplishment for a Scala library to provide such an elegant user interface.

Li has [a history of porting good Python libraries to Scala](https://www.lihaoyi.com/post/TheDeathofHypeWhatsNextforScala.html#usability). Want to build a wonderful Scala graph library? [Email me](https://github.com/mrpowers) and let's build [NetworkX](https://github.com/networkx/networkx) for Scala.

## No imports needed

The `os-lib` API is exposed via the `os` object.

You can add `libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"` to your `build.sbt` files and the `os` object is automatically available.

You don't need to add any additional imports when using the library, so it feels like it's a native Scala package.

Libraries generally shouldn't be exposed in this manner, but Li can get away with it ;)

## Creating a folder and file

Let's create a `my_folder` directory:

```
os.makeDir(os.pwd/"my_folder")
```

Now let's add a `story.txt` file to the folder with some text.

```
os.write(os.pwd/"my_folder"/"story.txt", "once upon a time")
```

These commands feel Unix-like.

## Deleting a file

Let's delete the `story.txt` file:

```
os.remove(os.pwd/"my_folder"/"story.txt")
```

`os.remove` can also be used to delete empty folders:

```
os.remove(os.pwd/"my_folder")
```

## File listing

Let's create a folder with a few files and then list the folder contents.

```
os.makeDir(os.pwd/"dogs")
os.write(os.pwd/"dogs"/"dog1.txt", "ruff ruff")
os.write(os.pwd/"dogs"/"dog2.txt", "bow wow")
os.write(os.pwd/"dogs"/"dog3.txt", "rrrrr")
```

`os.list(os.pwd/"dogs")` returns a listing of the files as an ArraySeq:

```
ArraySeq(
  /Users/powers/Documents/code/my_apps/scala-design/dogs/dog1.txt,
  /Users/powers/Documents/code/my_apps/scala-design/dogs/dog2.txt,
  /Users/powers/Documents/code/my_apps/scala-design/dogs/dog3.txt
)
```

These file listing capabilities allow for idiomatic Scala file processing. You can recursively list a directory and find the largest nested file for example.

You can't delete the `dogs` directory with `os.remove(os.pwd/"dogs")` because it contains files. You need to use `os.remove.all(os.pwd/"dogs")`.

## This library is a good example for the Scala community

This library provides several good examples for the Scala community:

- it provides a Ruby-like, clean public interface
- it's dependency free
- it's intuitive and you can answer your own questions by searching the README (you don't need to read source code or ask Stackoverflow)
- the API is stable
- import interface is clean (well, non-existent for this lib)
- it's cross compiled with Scala 2.12 and 2.13

You can depend on os-lib without worrying that it'll become a maintenance burden.

## Next steps

Li's [Hands on Scala Programming book](https://www.handsonscala.com/) has an entire chapter dedicated to this library and covers it in much more detail. You should buy the book. It's a masterpiece.

You don't need to study the library extensively to start using it. Learn the basic examples and then answer your specific questions by searching the README with Command + F. Libraries that document the public interface in the README are so easy to use.

Include the os-lib library in your project and enjoy it. Study the library design patterns that Li uses to make wonderful contributions to the Scala community. Writing code with minimal dependencies and clear public interfaces will make you a better Scala programmer.
