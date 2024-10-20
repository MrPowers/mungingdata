---
title: "Publishing Spark Projects with JitPack"
date: "2018-05-21"
categories: 
  - "apache-spark"
---

JitPack is a package repository that provides easy access to your Spark projects that are checked into GitHub. JitPack is easier to use than Maven for open source projects and less hassle than maintaining a private artifact repository for closed source projects.

This episode will show how to access existing Spark projects in JitPack and how to publish your own Spark projects in JitPack.

## Access existing projects

Let's say you'd like to access the `v2.3.0_0.21.0` release of the [spark-daria open source project](https://jitpack.io/#mrpowers/spark-daria).

This library can be accessed by adding these two lines of code to your `build.sbt` file.

```
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-daria" % "v2.3.0_0.21.0"
```

## Publishing open source projects

JitPack can build JAR files based on GitHub releases, branches, or commits. It's best to work off JAR files that correspond to GitHub releases. Here is [an example of a GitHub release](https://github.com/MrPowers/spark-daria/releases/tag/v2.3.0_0.21.0) that's picked up by JitPack.

So all you need to do is make a GitHub release and JitPack will make the JAR file available for all consumers.

You can easily write a script that makes multiple GitHub releases for your project for each Spark version that you support.

## Publishing closed source projects

JitPack is also easy to use for closed source projects. You need to sign up for a plan and [approve JitPack as an OAuth app](https://help.github.com/articles/approving-oauth-apps-for-your-organization/) for your organization's GitHub account.

The JitPack auth credentials need to be added to your local machine, [as described on this page](https://jitpack.io/private#auth).

The credentials should be added to the `~/.sbt/.credentials` file as follows.

```
realm=JitPack
host=jitpack.io
user=AUTHENTICATION_TOKEN
password=.
```

The `AUTHENTICATION_TOKEN` is the JitPack personal authentication token that is supplied when you sign up for an account.

The password is literally a period - that doesn't need to change.

Closed source JitPack projects will only be accessible to developers with accounts in your organization's GitHub account.

## JitPack alternatives

Spark developers are often confounded by the Scala ecosystem that heavily leverages [Apache Maven](https://en.wikipedia.org/wiki/Apache_Maven) and [Apache Ivy](https://en.wikipedia.org/wiki/Apache_Ivy). XML files are used extensively in the Java ecosystem.

Creating a private artifact repository to host binary files is possible, but most data engineers don't want to provision a machine with Java and maintain an ec2 instance.

JitPack alternatives are approachable to engineers with a lot of Java experience, but should be avoided if you're new to the Java ecosystem.

## Next steps

Spark developers can use JitPack and bypass Maven / private artifactory headaches.

You'll still need to understand the principles outlined in the [building Spark JAR files with SBT](https://www.mungingdata.com/episodes/4-building-spark-jar-files-with-sbt) post so JitPack knows how to properly construct your JAR files, but you don't need to learn how to structure POM / XML files when working with Scala anymore.
