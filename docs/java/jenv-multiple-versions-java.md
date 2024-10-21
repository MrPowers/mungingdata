---
title: "Running Multiple Versions of Java on MacOS with jenv"
date: "2020-11-07"
categories: 
  - "java"
---

# Running Multiple Versions of Java on MacOS with jenv

[jenv](https://github.com/jenv/jenv) makes it easy to run multiple versions of Java on a Mac computer. It also makes it easy to seamlessly switch between Java versions when you switch projects.

Running multiple Java versions is important for Android and Apache Spark developers. Spark developers should use Java 8 for Spark 2 projects and Java 11 for Spark 3 projects for example.

This blog post shows you how to get jenv setup on your computer and how to use the important commands.

## jenv setup

Install jenv with `brew install jenv`. This is a Homebrew command.

jenv uses the [shim design pattern](https://mungingdata.com/python/how-pyenv-works-shims/) to route commands to the appropriate Java version. Run these commands to update your `PATH`:

```
echo 'export PATH="$HOME/.jenv/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(jenv init -)"' >> ~/.zshrc
```

Restart your Terminal, run `echo $PATH`, and verify the output contains `.jenv` paths that are prepended to the standard PATH directories. Here's the output on my machine `/Users/powers/.jenv/shims:/Users/powers/.jenv/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin`.

## Install Java 8

Here's the latest command to install Java 8: `brew cask install adoptopenjdk/openjdk/adoptopenjdk8`.

`brew cask install adoptopenjdk8` used to work, but now returns `Error: Cask 'adoptopenjdk8' is unavailable: No Cask with this name exists.`

`brew cask install caskroom/versions/adoptopenjdk8` also used to work, but now returns `Error: caskroom/versions was moved. Tap homebrew/cask-versions instead.`

Once Java is downloaded, we need to manually add it to jenv. List the Java virtual machines with `ls -1 /Library/Java/JavaVirtualMachines`.

Add Java 8 to jenv with `jenv add /Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/`.

Set the global Java version on your computer with `jenv global openjdk64-1.8.0.265`. The exact command on your machine might be something slightly different like `jenv global openjdk64-1.8.0.272`. Find the exact name of the version with `jenv versions`.

Check to make sure the `javac -version` and `java -version` commands are working.

## Set global Java version

Macs come with Java preinstalled. It's always good to avoid using system installed programming language versions (applies to Python and Ruby too). jenv makes it easy to avoid using the system Java.

Set the global Java version to Java 8 with `jenv global openjdk64-1.8.0.265`.

This command simply writes the version to the `/Users/powers/.jenv/version` file. Type `cat /Users/powers/.jenv/version` to see it's just a file with a single line

```
openjdk64-1.8.0.272
```

All Java commands will be routed to Java 8 now that the global version is set. This'll make sure we avoid hitting the system Java version.

## Set JAVA\_HOME

Lots of Java libraries depend on having a `JAVA_HOME` environment variable set. Set the environment variable by running these commands:

```
jenv enable-plugin export
exec $SHELL -l
```

Run `echo $JAVA_HOME` and verify that it returns something like `/Users/powers/.jenv/versions/openjdk64-1.8.0.272`. Now any library that's looking for the `JAVA_HOME` environment to be set won't error out.

Run `jenv doctor` to confirm your setup is good. You should get output like this:

```
[OK]    JAVA_HOME variable probably set by jenv PROMPT
[OK]    Java binaries in path are jenv shims
[OK]    Jenv is correctly loaded
```

## Install Java 11

Here's the command to install Java 11: `brew cask install adoptopenjdk/openjdk/adoptopenjdk11`.

Remember that Java versions need to be manually added to jenv. List the Java virtual machines with `ls -1 /Library/Java/JavaVirtualMachines`.

Add Java 11 to jenv with `jenv add /Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home/`.

The `jenv versions` command should now output this:

```
  system
  1.8
  1.8.0.272
  11
  11.0
  11.0.9
* openjdk64-1.8.0.272 (set by /Users/powers/.jenv/version)
  openjdk64-11.0.9
```

## Setting up Maven

`which mvn` returns `/usr/local/bin/mvn`, which is the system version of Maven. Similar to Java, we never want to run commands using the system Maven. Let's use jenv to get a different version of Maven.

Enable the jenv Maven plugin with `jenv enable-plugin maven` and then run `which mvn` to verify that the `mvn` commands are being properly captured by a jenv shim. The `which mvn` command should return something like `/Users/powers/.jenv/shims/mvn`.

You can verify that your Maven installation is working properly by cloning a project and running the test suite. Clone the [JavaSpark](https://github.com/MrPowers/JavaSpark) project with the `git clone git@github.com:MrPowers/JavaSpark.git` command.

`cd` into the project directory and run the test suite with `mvn test`.

You can type `mvn -v` to see the Maven version that's being used. My machine is using Maven 3.6.3 with Java 8.

You can also clone the [deequ](https://github.com/awslabs/deequ) repo and verify that `mvn test` is working on that repo as well.

## Setting local Java version for projects

Use the `jenv local openjdk64-11.0.9` command to set a given project to use Java 11 by default.

This will add a `.java-version` file in the root project folder. Here's [an example](https://github.com/MrPowers/delta-examples/blob/master/.java-version).

You can clone the [delta-examples](https://github.com/MrPowers/delta-examples) repo with `git clone git@github.com:MrPowers/delta-examples.git`, cd into the directory, and run `jenv versions` to verify that the project is automatically using Java 11.

Here's the `jenv versions` output from the delta-examples project root directory:

```
  system
  1.8
  1.8.0.272
  11
  11.0
  11.0.9
  openjdk64-1.8.0.272
* openjdk64-11.0.9 (set by /Users/powers/Documents/code/my_apps/delta-examples/.java-version)
```

jenv's ability to automatically switch Java versions for different projects is quite convenient. You don't need to think about manually setting the Java version when you change projects.

## Other ways to switch Java versions

The AdoptOpenJDK project provides [guidance](https://github.com/AdoptOpenJDK/homebrew-openjdk#switch-between-different-jdk-versions) on how to manually switch between Java versions if you don't want to use jenv. Here's the function they provide:

```
jdk() {
        version=$1
        export JAVA_HOME=$(/usr/libexec/java_home -v"$version");
        java -version
 }
```

Switching manually is possible, but who wants to waste mental energy thinking about Java versions every time they switch a project?

## Next steps

jenv will help you manage Java on your Mac, even if you only need to use a single version.

Managing different Java versions on a given machine was a huge pain before jenv came along. Now, you only need to run a few commands and your machine can be configured to run any Java version. jenv makes it easy to avoid accidentally using the system installed Java packages.

jenv has [built in plugins](https://github.com/jenv/jenv/tree/master/available-plugins) for SBT, Scala, Groovy, and more. Make sure to enable the plugins that are relevant for your workflows.
