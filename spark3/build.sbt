resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "spark3"

version := "0.0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview2" % "provided"

libraryDependencies += "mrpowers" % "spark-daria" % "0.37.1-s_2.12"

libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.21.1-s_2.12" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// JAR file settings

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
