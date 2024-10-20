---
title: "DevRel Driven Development"
date: "2022-06-14"
categories: 
  - "devrel"
---

DevRel Driven Development is driving software development from developer advocacy activities like creating documentation, writing blog posts, and producing videos. Developers advocates frequently identify public interface warts when creating content. They can collaborate closely with devs build more intuitive APIs and give end users a better development experience.

When done right, DevRel Driven Development creates a symbiotic collaboration between core developers, dev advocates and end users. This virtuous cycle motivates developers and delights end users. Developers love exposing elegant APIs that are quickly adopted by users. A good dev advocate will help the devs expose the right interface.

This post explains how to perform DevRel Driven Development and provides real-world examples, so you can see the process in action.

## Create and improve documentation

When a developer advocate starts working on a project, they should go through all the existing docs, run the examples locally, and see if there are any small improvements that'll help onboard new users.

It's good to inject positivity and tackle low hanging fruit when you're first starting to contribute to an open source project. You want to build goodwill with the developers before suggesting large changes.

Suppose you read through the docs and notice some spelling and grammar issues. You can submit a pull request and add a description like this:

> This pull request fixes some minor typos in the docs. I was able to learn a lot about this project by going through the docs and running the examples locally. Thanks for making it easy for me to get started with this library!

If the project doesn't have any documentation, then you can open an issue like this:

> Can I make a pull request to add some basic usage instructions to the README? I'd like to make it easy for new users to get up-and-running with this library. Let me know if this sounds like a good idea.

Lots of developers prefer focusing their energy on software engineering and don't enjoy evangelizing their work. They appreciate developer advocacy activities that'll help them get users.

You can gradually step up the scope of proposed changes after you've built a relationship with the developers and have convinced them that they'll benefit from your involvement in the project.

You're likely to add new sections to the docs and reorganize content after a few iterations. You'll eventually find some gaps and unintuitive APIs and that's when you can start suggesting new features.

## Fixing bugs

When you're going through the docs and running the code locally, you're bound to spot certain sections that are missing, unintuitive, or overly complex.

Let's look at a real-world example to see how DevRel Driven Development can quickly squash bugs and add features.

I was reading through the [delta-rs docs](https://delta-io.github.io/delta-rs/python/usage.html#usage) and created a Jupyter Notebook, so other developers could easily follow along.

I tried to create a Delta Lake with delta-rs, but faced an unexpected error when reading the Delta Lake. I filed [a bug report](https://github.com/delta-io/delta-rs/issues/617) and a delta-rs dev fixed the issue within a day. Turns out the test suite only checked absolute paths and the code wasn't working for relative paths. I switched the notebook to absolute paths and continued creating the demo notebook.

The demo notebook made me realize that some features weren't implemented yet.

## Filling gaps

Delta Lake allows for schema enforcement, which prevents files with different schema from being added to the Delta Lake.

I noticed schema enforcement wasn't working in the demo notebook and pinged the delta-rs devs in Slack. They noticed this was an oversight, [created an issue](https://github.com/delta-io/delta-rs/issues/623), and [quickly fixed the bug](https://github.com/delta-io/delta-rs/pull/624).

Devs are highly motivated to fix oversights, especially if they feel ownership of the codebase.

## Suggesting interfaces

Developer advocates should also provide developers with suggestions on the public interfaces that should be exposed.

I tried to vacuum a Delta Lake with delta-rs and found that there isn't a way to bypass the retention period check. You can overcome this restriction with normal Delta Lake by setting `config("spark.databricks.delta.retentionDurationCheck.enabled", "false")` when creating the SparkSession. One of the delta-rs developers suggested adding an `enforce_retention_duration=True` parameter in `DeltaTable.vacuum()`. I countered [in an issue](https://github.com/delta-io/delta-rs/issues/635) by suggesting to add this parameter as `retention_duration_check_enabled=True`, so we're consistent with the Delta Lake wording.

A health back-and-forth on the optimal interface is likely to result in an API that's intuitive for users.

Developer advocates can take their suggesting to the next level by actually submitting pull requests to implement their suggestions.

## DevRel pull requests

I am writing [Dask: The Definitive Guide](https://learning.oreilly.com/library/view/dask-the-definitive/9781098117139/) and have been creating a lot of small Dask DataFrames in the book examples.

I've been creating Dask DataFrames as follows:

```
import pandas as pd

pandas_df = pd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
)
df = dd.from_pandas(pandas_df, npartitions=2)
```

This syntax confuses users. They're surprised they need to `import pandas` to create a small Dask DataFrame.

[I suggested](https://github.com/dask/dask/issues/9009) adding the following interface to avoid importing pandas:

```
import dask.dataframe as dd

ddf = dd.DataFrame(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]},
    npartitions=2,
)
```

The Dask DataFrame public interface is designed to mimic the pandas API and a core Dask developer suggested a `from_dict` class method, just like pandas:

```
dd.DataFrame.from_dict(
    {"num1": [1, 2, 3, 4], "num2": [7, 8, 9, 10]}, npartitions=2
)
```

I created [a pull request](https://github.com/dask/dask/pull/9017) to add this feature. I haven't contributed to Dask extensively and ended up pairing with a Dask core contributor to get the pull request in a mergeable state.

Actually creating pull requests is a great way to connect with developers at a deeper level. Developers typically appreciate they effort, even if they need to significantly refactor your code.

Developer advocates can of course add a lot of value to project, even if they're not writing any code.

## DevRel cheerleading

Dev advocates should strive to inject positivity to the project and guide the team to communicate in an upbeat manner. Everyone has their own communication style, but a little extra positive energy never hurts.

Reacting with emojis to pull requests and issues is the lowest hanging fruit. If a developer fixes a bug, adds a feature, or raises a good issue, it's easy to react with a thumbs up. When you start adding positive emojis, other developers are likely to follow suit and do the same.

You can also react with positive messages, like "this is a great feature, thank you!". You don't want to clutter inboxes with these types of messages, so use messages that generate notifications sparingly.

Now lets turn our attention to the main dev advocacy value-add - driving usage and adoption.

## Driving usage via content

A good dev advocate should help developers acquire new users and delight existing users.

Dev advocates should be creating content to help onboard new users. Hopefully the engineers can easily see how the dev advocates are helping their project grow.

Engineers are much more likely to support DevRel Driven Development if they feel like the developer advocates are adding value to the project. You can proactively share dev advocacy metrics (pageviews, button clicks, etc) with developers to help them quanity the value of your contributions to the project.

## Conclusion

DevRel Driven Development is a great way for developer advocates to forge strong connections with core library developers and end users and drive adoption of technologies. It's a more active type of advocacy that requires you to get your hands dirty instead of cheering on the sidelines.

It's only practical for highly technical dev advocates that are developers themselves. Developer advocates with a purely marketing background won't be technical enough to suggest public interfaces or submit pull requests.

It's best to use DevRel Driven Development in conjunction with engineering-lead feature prioritization. Engineers have plenty of features to build and technical debt to work on, independent of dev advocacy driven features. DevRel Driven Development is best layered on top of existing engineering workflows to encourage user-based focus and beautiful public interface design.

Software engineers can fall into the trap of spending 99% of their time on programming and only 1% of their time on creating READMEs, documentation, and SEO-optimized content for end users. It's hard to get users if the onboarding materials and messaging are not on point. DevRel Driven Development encourages engineers to spend a bit more time to build an amazing user experience, which is a great way to get users to fall in love with their beautiful code.
