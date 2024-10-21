---
title: "Managing Multiple Java, SBT, and Scala Versions with SDKMAN"
date: "2020-11-09"
categories: 
  - "java"
---

# Managing Multiple Java, SBT, and Scala Versions with SDKMAN

[SDKMAN](https://sdkman.io/) is an amazing project that makes it easy to manage multiple versions of Java and Scala on a single computer.

It also provides support for other Java ecosystem projects like Maven, SBT, and Spark.

SDKMAN stands for Software Development Kit MANager.

SDKMAN is the best Java version management solution because it works out of the box for a variety of different programs.

## Install SDKMAN

Install SDKMAN with `curl -s "https://get.sdkman.io" | bash`.

Run `source "$HOME/.sdkman/bin/sdkman-init.sh"` to load SDKMAN (this step is optional - you can also just close your Terminal and reopen it).

Run `sdk version` and make sure `SDKMAN 5.9.1+575` is output to verify the installation was successful.

The installation script will append these lines to your `~/.zshrc` file (you don't need to add these lines - the installer will add them for you):

```
#THIS MUST BE AT THE END OF THE FILE FOR SDKMAN TO WORK!!!
export SDKMAN_DIR="/Users/powers/.sdkman"
[[ -s "/Users/powers/.sdkman/bin/sdkman-init.sh" ]] && source "/Users/powers/.sdkman/bin/sdkman-init.sh"
```

Run `echo $PATH` and see that .sdkman is prepended to the PATH as follows: `/Users/powers/.sdkman/candidates/java/current/bin:/Users/powers/.pyenv/shims:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin`.

SDKMAN uses the [shim design pattern](https://mungingdata.com/python/how-pyenv-works-shims/) to intercept Java relevant commands and route them to the correct software versions. This design pattern is also used in other version management tools like rbenv (Ruby) and pyenv (Python).

## Install Java

List the available Java versions with `sdk list java`:

```
================================================================================
Available Java Versions
================================================================================
 Vendor        | Use | Version      | Dist    | Status     | Identifier
--------------------------------------------------------------------------------
 AdoptOpenJDK  |     | 15.0.1.j9    | adpt    |            | 15.0.1.j9-adpt
               |     | 15.0.1.hs    | adpt    |            | 15.0.1.hs-adpt
               |     | 14.0.2.j9    | adpt    |            | 14.0.2.j9-adpt
               |     | 14.0.2.hs    | adpt    |            | 14.0.2.hs-adpt
               |     | 13.0.2.j9    | adpt    |            | 13.0.2.j9-adpt
               |     | 13.0.2.hs    | adpt    |            | 13.0.2.hs-adpt
               |     | 12.0.2.j9    | adpt    |            | 12.0.2.j9-adpt
               |     | 12.0.2.hs    | adpt    |            | 12.0.2.hs-adpt
               |     | 11.0.9.j9    | adpt    |            | 11.0.9.j9-adpt
               |     | 11.0.9.hs    | adpt    |            | 11.0.9.hs-adpt
               |     | 8.0.272.j9   | adpt    |            | 8.0.272.j9-adpt
               |     | 8.0.272.hs   | adpt    |            | 8.0.272.hs-adpt
 Amazon        |     | 15.0.1       | amzn    |            | 15.0.1-amzn
               |     | 11.0.9       | amzn    |            | 11.0.9-amzn
               |     | 8.0.275      | amzn    |            | 8.0.275-amzn
more versions not listed...
```

Install Java 8 with `sdk install java 8.0.272.hs-adpt`. Here's the terminal output when the installation is complete:

```
Repackaging Java 8.0.272.hs-adpt...

Done repackaging...
Cleaning up residual files...

Installing: java 8.0.272.hs-adpt
Done installing!
```

SDKMAN stores files it downloads in the `/Users/powers/.sdkman` directory.

Now install Java 11 with `sdk install java 11.0.9.hs-adpt`. Type `sdk list java` to show the Java versions that have been installed and the Java version that's currently being used.

```
================================================================================
Available Java Versions
================================================================================
 Vendor        | Use | Version      | Dist    | Status     | Identifier
--------------------------------------------------------------------------------
 AdoptOpenJDK  |     | 15.0.1.j9    | adpt    |            | 15.0.1.j9-adpt
               |     | 15.0.1.hs    | adpt    |            | 15.0.1.hs-adpt
               |     | 14.0.2.j9    | adpt    |            | 14.0.2.j9-adpt
               |     | 14.0.2.hs    | adpt    |            | 14.0.2.hs-adpt
               |     | 13.0.2.j9    | adpt    |            | 13.0.2.j9-adpt
               |     | 13.0.2.hs    | adpt    |            | 13.0.2.hs-adpt
               |     | 12.0.2.j9    | adpt    |            | 12.0.2.j9-adpt
               |     | 12.0.2.hs    | adpt    |            | 12.0.2.hs-adpt
               |     | 11.0.9.j9    | adpt    |            | 11.0.9.j9-adpt
               |     | 11.0.9.hs    | adpt    | installed  | 11.0.9.hs-adpt
               |     | 8.0.272.j9   | adpt    |            | 8.0.272.j9-adpt
               | >>> | 8.0.272.hs   | adpt    | installed  | 8.0.272.hs-adpt
```

You can also see the Java version that's being used by running `sdk current`:

```
Using java version 8.0.272.hs-adpt
```

Switch to Java 11 with `sdk use java 11.0.9.hs-adpt`. Run `sdk current` and confirm that Java 11 is being used.

The `sdk use java` command will only switch the Java version for the current shell. You'll lose those settings when the shell is closed.

You can set a default Java version for whenever shells are started. Set SDKMAN to default to Java 8 with `sdk default java 8.0.272.hs-adpt`.

Close the shell, reopen it, and run `sdk current java` to confirm SDKMAN is using Java version 8.0.272.hs-adpt by default now.

You can also check the version by running `java -version`. You should see output like this:

```
openjdk version "1.8.0_272"
OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_272-b10)
OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.272-b10, mixed mode)
```

## JAVA\_HOME

Lots of programs require the `JAVA_HOME` environment variable to be set.

Lucky for you, SDKMAN automatically sets the `JAVA_HOME` environment variable.

When you run `echo $JAVA_HOME`, you should see something like `/Users/powers/.sdkman/candidates/java/current`.

## Install SBT

View the available SBT versions with `sdk list sbt`:

```
================================================================================
Available Sbt Versions
================================================================================
     1.4.2               1.3.1               1.1.0
     1.4.1               1.3.0               1.0.4
     1.4.0               1.2.8               1.0.3
     1.4.0-RC2           1.2.7               1.0.2
     1.3.13              1.2.6               1.0.1
     1.3.12              1.2.5               1.0.0
     1.3.11              1.2.4               0.13.18
     1.3.10              1.2.3               0.13.17
     1.3.9               1.2.1
     1.3.8               1.2.0
     1.3.7               1.1.6
     1.3.6               1.1.5
     1.3.5               1.1.4
     1.3.4               1.1.2
     1.3.3               1.1.1
```

Install SBT 1.3.13 with `sdk install sbt 1.3.13`. After installing, respond with a "Y" to set 1.3.13 as your default SBT.

You can clone a SBT project and run the test suite if you'd like to verify that your SBT version was installed correctly. Here are the optional verification steps.

- Clone [spark-daria](https://github.com/MrPowers/spark-daria) with `git clone git@github.com:MrPowers/spark-daria.git`
- `cd` into the spark-daria directory
- Run `sbt test` and make sure the test suite finishes successfully

## Install Scala

List the available Scala versions with `sdk list scala`:

```
================================================================================
Available Scala Versions
================================================================================
     2.13.3              2.12.1
     2.13.2              2.12.0
     2.13.1              2.11.12
     2.13.0              2.11.11
     2.12.12             2.11.8
     2.12.11             2.11.7
     2.12.10             2.11.6
     2.12.9              2.11.5
     2.12.8              2.11.4
     2.12.7              2.11.3
     2.12.6              2.11.2
     2.12.5              2.11.1
     2.12.4              2.11.0
     2.12.3
     2.12.2
```

Install Scala 2.11.12 with `sdk install scala 2.11.12`.

Type `sdk use scala 2.11.12` to switch to the Scala version you just downloaded. Type `scala` in the Terminal and it'll open up an interactive Scala REPL with Scala 2.11.12. Type `:quit` in the REPL to exit.

As you might have guessed, you can set a default Scala version with `sdk default scala 2.11.12`.

SDKMAN's command line interface is consistent for all the SDKs. The commands to list, install, change, and set default versions follow the same syntax for each SDK. It's easy to memorize the commands.

## Install Spark

Run `sdk list spark` to see the versions of Spark you can download:

```
================================================================================
Available Spark Versions
================================================================================
     3.0.1               2.2.0
     3.0.0               2.1.3
     2.4.7               2.1.2
     2.4.6               2.1.1
     2.4.5               2.0.2
     2.4.4               1.6.3
     2.4.3               1.5.2
     2.4.2               1.4.1
     2.4.1
     2.4.0
     2.3.3
     2.3.2
     2.3.1
     2.3.0
     2.2.1
```

Run `sdk install spark 2.4.7` to install the latest version of Spark 2.

Spark is a great example of a project that needs specific Java, Scala, and SBT versions to work properly. Spark 2 projects should be run with Java 8, Scala 2.11, and SBT 1.3 for example. Run `sdk current` to show all the current versions that SDKMAN is currently using.

```
java: 8.0.272.hs-adpt
sbt: 1.3.13
scala: 2.11.12
spark: 2.4.7
```

This machine currently has the right dependencies setup for a Spark 2 project.

Run `spark-shell` to start a Spark REPL. You can see that it's using the right versions:

```
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.7
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_272)
Type in expressions to have them evaluated.
Type :help for more information.
```

## Other supported SDKs

SDKMAN also supports [a bunch of other SDKs](https://sdkman.io/sdks).

It supports projects such as Maven, Groovy, Kotlin, Spring Boot, and more.

You can use the same syntax patterns we've seen in this blog post to install the SDKs, change versions, and set defaults.

SDKMAN has an elegant, consistent public interface and the commands are easy to remember.

## Where SDKMAN stores packages

SDKMAN stores file in `$HOME/.sdkman/candidates/`.

To find where SBT 1.3.13 is installed, type `sdk home sbt 1.3.13`. It'll return something like `/Users/powers/.sdkman/candidates/sbt/1.3.13`.

The arguments to the `sdk install` command align with where the files are stored in `$HOME/.sdkman/candidates`.

- `sdk install java 8.0.272.hs-adpt` stores files in `$HOME/.sdkman/candidates/java/8.0.272.hs-adpt`.
- `sdk install sbt 1.3.13` stores files in `$HOME/.sdkman/candidates/sbt/1.3.13`.

When you run `sdk install`, the downloaded binaries get saved in `$HOME/.sdkman/archives`. For example, `$HOME/.sdkman/archives/java-8.0.272.hs-adpt.zip` and `$HOME/.sdkman/archives/sbt-1.3.13.zip`.

Some of the binaries are pretty big and can end up taking a lot of space on your computer. You should periodically delete them with the `sdk flush archives` command. Once you install the software, you don't need the binaries anymore.

## Other alternatives for managing Java / SDK versions

There are other ways to download and switch Java versions.

You can download Java versions with Homebrew and [switch versions with jenv](https://mungingdata.com/java/jenv-multiple-versions-java/). This approach is a little harder because jenv is not as user friendly and the Homebrew commands to download Java tend to change. It's easier to just use SDKMAN.

Jabba helps you download different version of Java and lets you switch between them, but it doesn't provide access to any of the SDK versioning. You'll rarely use Java in isolation and will often want to switch between versions for core libraries like SBT and Scala. SDKMAN saves you from reinventing the wheel by providing a single, consistent interface that works for a variety of different projects.

Jabba's main advantage is that's it's cross platform (because it's written in Go). Jabba provides a unified interface on Mac, Windows, and Linux. If you need a cross platform solution, go with Jabba.

## Conclusion

Managing Java versions was painful for me until I started using SDKMAN.

I struggled with manual installations. The Homebrew downloads / jenv switching workflow helped, but didn't solve versioning for SBT, Scala, or Spark. jenv is a great project, but it has some quirks that make it slightly more difficult to work with. Downloading Java versions via Homebrew is annoying because the commands change frequently so the Stackoverflow answers are often stale.

SDKMAN will let you manage multiple versions of Java and other core libraries on your machine with minimal hassle. Try it out and you'll be sure to love it.
