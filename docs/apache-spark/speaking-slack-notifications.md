---
title: "Speaking Slack Notifications from Spark"
date: "2018-04-04"
categories: 
  - "apache-spark"
---

# Speaking Slack Notifications from Spark

The spark-slack library can be used to speak notifications to Slack from your Spark programs and handle Slack Slash command responses.

You can speak Slack notifications to alert stakeholders when an important job is done running or even speak counts from a Spark DataFrame.

This blog post will also show how to run Spark ETL processes from the Slack command line that will allow your organization to operate more transparently and efficiently.

## Slack Messages

Here’s how to speak a “You are amazing” message in the #general channel:

```scala
import com.github.mrpowers.spark.slack.Notifier

val webhookUrl = "https://hooks.slack.com/services/..."
val notifier = new Notifier(webhookUrl)
notifier.speak("You are amazing", "general", ":wink:", "Frank")
val notifier = new Notifier()
notifier.speak("You are amazing", "general", ":wink:", "Frank")
```

Here’s how to speak a count of all the records in a DataFrame (df) to a Slack channel.

```scala
val notifier = new Notifier()
val formatter = java.text.NumberFormat.getIntegerInstance
val message = s"Total Count: ${formatter.format(df.count)}"
notifier.speak(message, "general", ":wink:", "Frank")
```

You can add this code to a job that’s run periodically with cron and use this to speak updated counts for key metrics to stakeholders.

## Handling Slack Slash Command Responses

Slack lets you create custom Slash commands that can be used to kick off Spark jobs.

You can send the Slack Slash command POST response to AWS Lambda and write a Lamdba function that sends another POST request to the Databricks API to kick off a job.

The spark-slack SlashParser class converts a Slack Slash JSON string into a Scala object that’s much easier to work with in Spark.

Let’s say you create a Slack Slash command for /mcsc (my cool Slash command) and type in the following command:

```
/mcsc marketing extract quarterly_report
```

Slack will send a JSON response with this format:

```
token=gIkuvaNzQIHg97ATvDxqgjtO
team_id=T0001
team_domain=example
channel_id=C2147483705
channel_name=test
user_id=U2147483697
user_name=Steve
command=/mcsc
text=marketing extract quarterly_report
response_url=https://hooks.slack.com/commands/1234/5678
```

Here’s an example of code that will parse the text and run different Databricks notebooks based on the Slack Slash command arguments.

```scala
import com.github.mrpowers.spark.slack.slash_commands.SlashParser
import com.github.mrpowers.spark.slack.Notifier

val parser = new SlashParser(dbutils.widgets.get("slack-response"))
val arg0 = parser.textComponents(0)
val arg1 = parser.textComponents(1)
val arg2 = parser.textComponents(2)

arg1 match {
  case "extract" => dbutils.notebook.run("./Extracts", 0, Map("extractName" -> arg2))
  case "transform" => dbutils.notebook.run("./Transforms", 0, Map("transformationName" -> arg2))
  case "report" => dbutils.notebook.run("./Reports", 0, Map("reportName" -> arg2))
}

val notifier = new Notifier(WEBHOOK_URL)
notifier.speak(
  "Your extract ran successfully",
  parser.slashResponse.channel_name,
  ":bob:",
  "Bob"
)
```

Running Spark ETL processes from the Slack command line is a transparent way to keep all stakeholders updated.

## Next steps

Important notifications from Spark jobs should be spoken in shared Slack channels to increase the transparency of workflows.

Slack should be used as a shared terminal with custom commands to kick off important jobs.

Recurring analyses should be run with cron jobs and should notify stakeholders when completed.

Making Spark jobs easy to run is critical for organizational adoption of the technology. Most employees spend most of their day in Slack, so it’s the best place to make Spark jobs accessible.
