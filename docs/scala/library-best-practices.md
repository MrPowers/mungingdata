---
title: "Scala Library Best Practices"
date: "2020-01-20"
categories: 
  - "scala"
---

# Scala Library Best Practices

This post explains the best practices Scala libraries should follow.

Here are the most important best practices to follow:

- Great README
- Clearly defined API
- Accessible API documentation
- Clean JAR file
- Continuous integration
- Automated deployment
- Versioning

Some of these best practices also apply to applications, but this post focuses on libraries.

If you learn how to make awesome libraries, you can build open source codebases that are used by people around the world and private repos that are beloved by your company.

## Ultimate litmus test for a library

A user should be able to review the library README (and associated links) and use the library you've developed to add business value without any individual coaching or training. This litmus test applies to both open source and private libraries.

Build a great library, add some marketing, and you'll be piling up GitHub stars like [tj](https://github.com/tj) and [lihaoyi](https://github.com/lihaoyi).

Let's examine characteristics of well designed Scala libraries in more detail.

## Great open source library example

[utest](https://github.com/lihaoyi/utest) is a great example of a library that's easily adoptable.

The README gives a short overview of how to use the library, a detailed description of key functionality, and [a great explanation on why uTest is better than other testing libraries](https://github.com/lihaoyi/utest#why-utest).

A Scala programmer can easily add utest to their `build.sbt` file as a dependency and start writing tests based on the README examples.

You don't need to meet with Li to understand how to use his library. He's already given you everything you need.

Private repos should meet the same standard of excellence. New hires should be able to consult your private README and start using your library, even if there is a ton of domain specific context.

## Great README

The README should start out by explaining why the library will add value for the user.

[spark-testing-base](https://github.com/holdenk/spark-testing-base#why) is a great example of a library that jumps right in and immediately tells the user how it adds value.

The README should also explain how to install your library, a short overview of the code, a detailed overview and how to contribute.

READMEs are generally too short and don't make a sales pitch to attract users. Don't be shy. Tell users why your library will make their lives better.

## Clearly defined API

Libraries should have a clearly defined public interface.

The `private` keyword should be used to differentiate implementation details from end user functions.

Great library developers obssess over building public interfaces that are clean and intuitive. [This blog post on uJson](http://www.lihaoyi.com/post/uJsonfastflexibleandintuitiveJSONforScala.html) shows the thought process of a Scala developer that's fighting to create the best Scala JSON library possible.

You can preliminarily assess the quality of a library by searching for the `private` keyword. It's a red flag if a library isn't using `private` extensively.

## Accessible API documentation

The programatic API documentation should be easily accessible via a README link.

[The Spark API docs are a great example](https://spark.apache.org/docs/latest/api/scala/index.html#package). Your API docs should look a lot like the Spark API docs.

Custom documentation like [scalafmt](https://scalameta.org/scalafmt/), [mill](http://www.lihaoyi.com/mill/), and [sbt](https://www.scala-sbt.org/1.x/docs/) is a perfectly acceptable replacement for bigger projects.

Empathize with your users. Give them materials that'll make it easy for them to adopt and quickly derive value from your library.

`sbt doc` will exclude all methods that are flagged as `private`. Developers that care about providing their users with clean documentation are also the developers that work hard to build beautiful public interfaces.

## Limit dependencies

Limiting dependencies is so important that some libraries use it as a selling point.

The [uPickle](https://github.com/lihaoyi/upickle) library is marketed as "a simple, fast, **dependency-free** JSON & Binary (MessagePack) serialization library for Scala".

Users can add uPickle to their project without worrying about a bunch of other dependencies being added.

Users like dependency-free libraries, so they can avoid [dependency hell](https://en.wikipedia.org/wiki/Dependency_hell).

Vendoring dependencies is another way to avoid build file dependencies (another technique to bypass downstream dependency hell).

The [utest](https://github.com/lihaoyi/utest) vendors [Fansi](https://github.com/lihaoyi/fansi) to avoid having a dependency.

> Note that uTest uses an internal copy of the [Fansi](https://github.com/lihaoyi/fansi) library, vendored at utest.ufansi, in order to avoid any compatibility problems with any of your other dependencies. You can use ufansi to construct the colored ufansi.Strs that these methods require, or you could just return colored java.lang.String objects containing ANSI escapes, created however you like, and they will be automatically parsed into the correct format.

Fansi is a [single file project](https://github.com/lihaoyi/fansi/blob/master/fansi/src/fansi/Fansi.scala), so they is no reason to add it as a dependency to a core library like utest.

Fansi is also vendored in the [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests/) project. If Fanzi wasn't vendored, an application that depends on utest, spark-fast-tests, and Fanzi directly could create a dependency hell situation for users. Strategic vendoring saves users from this headache.

Library dependencies are sometimes unavoidable. For example, Spark depends on [FasterXML](https://github.com/FasterXML) libraries. You should only add a dependency to a library when these criteria are met:

1. The dependency saves you from significantly reinventing the wheel. Don't forget about the [NPM left-pad debacle](https://qz.com/646467/how-one-programmer-broke-the-internet-by-deleting-a-tiny-piece-of-code/) a few years back where a developer deleted a package with 11 lines of code which ended up breaking a lot of websites. You shouldn't depend on a library that only provides a tiny bit of functionality.
2. You've inspected the dependencies of the dependency you're considering adding. The NPM left pad incident broke the internet because React dependended on a library that depended on left-pad (React had a "transitive dependency"). React didn't depend on left-pad directly, but it shouldn't have had a transitive dependency on such a trivial library.

Library dependency decisions can create dependency hell for downstream users.

## Clean JAR file

The JAR file you distribute to users should only include the files you want to distribute. Your code might depend on Spark, Scala, and [Scalatest](http://www.scalatest.org/), but that doesn't necessarily mean that those files should be inlucded in your JAR file.

Spark and Scala are typically marked as "provided" dependencies and Scalatest is a "test" dependency. You should flag your dependencies accordingly. [sbt-assembly](https://github.com/sbt/sbt-assembly) doesn't include provided or test depenencies in the JAR file by default.

You should run `jar tvf` and inspect the contents of your JAR file to make sure your build file only includes the right files.

You should also consider shading certain dependencies when building JAR files.

## Continuous integration

You should setup you libraries to run the test suite whenever code is merged with master.

Continuous integration services are free for open source project and worth the money for private repos.

The build status badge should be displayed prominently in the README.

Some library maintainers can get in a bad habit of breaking the build and letting the repo sit in a failed state for days or weeks. This is really bad. Whenever a build is broken, you should take immediate action to fix it.

## Automated deployment

You should have a single command that's documented in the README to deploy your project. Here's an example of what a deploy script can perform for an open source project:

- Build the JAR file
- Create a GitHub release with the JAR file attached
- Upload the JAR to Maven
- Build the new documentation and upload it to GitHub pages

Maintaining libraries can be difficult and tedious… especially when years have passed and the bug reports keep flowing in. Fully automated deploy processes make maintenance less painful.

## Versioning

Most libraries should stricly follow [Semantic Versioning](https://semver.org/).

Your users should feel comfortable upgrading to a new minor version without any backwards incompatible changes.

Some Scala library developers seem to randomly apply Semantic Versioning. They bump the PATCH version for "minor changes" and bump the MINOR version for "medium sized changes". It's better to tell users that the project follows Semantic Versioning in the README and then follow the guidlines strictly.

[CalVer](https://calver.org/) is another popular versioning scheme. Let me know if you know of any popular Scala projects that use CalVer.

## Good Scala libraries add a tremendous amount of value

You can help people from all over the world by developing a good Scala library.

[spark-daria](https://github.com/MrPowers/spark-daria/) allowed me to collaborate with smart Spark developers in India, Armenia, Spain, China, and other countries. I was able to meet many of these collaborators at Spark Summits in San Francisco and Amsterdam.

The Scala community is supportive and participating is rewarding. I highly recommend building some libraries and seeing for yourself!

## What other best practices should Scala projects follow?

Ping me in the comments or [email me](https://github.com/mrpowers) if you have other suggestions for library best practices.
