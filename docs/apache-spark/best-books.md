---
title: "Best Apache Spark Books"
date: "2019-11-13"
categories: 
  - "apache-spark"
---

Apache Spark is a big data engine that has quickly become one of the biggest distributed processing frameworks in the world.

It's used by all the big financial institutions and technology companies.

Small teams also find Spark invaluable. Even small research teams can spin up a cluster of cloud computers with the computational powers of a super computer and pay by the second for resources used.

Learning how to use Spark effectively isn't easy…

Cluster computing is complex!

The best Spark book for you depends on your current level and what you'd like to do with Spark. If you're a data engineer and want to focus on using Spark to clean a massive dataset, then you should stay away from the Spark machine learning algorithms.

## High level review

[Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/), [Testing Spark Applications](https://leanpub.com/testing-spark/), and Advanced Analytics with Spark are the best Spark books (they all use the Scala API).

Full disclosure: I wrote [Beautiful Spark Code](https://leanpub.com/beautiful-spark/) and [Testing Spark Applications](https://leanpub.com/testing-spark/), mainly to give the community some good options.

I don't know of any good PySpark books. [Message me](https://github.com/mrpowers) if you know of a good book, so I can update this blog post!

- [Writing Beautiful Spark Code](https://leanpub.com/beautiful-spark/) teaches you how to become a proficient Spark programmer quickly. It focuses on the most used API functions and workflows you need to get your job done. It does not focus on low level implementation details or theory. This book will teach you all the practical design patterns you need to be productive with Spark.
- [Testing Spark Applications](https://leanpub.com/testing-spark/) teaches you how to test the public interface of your applications. You will see how testing encourages you to write higher quality code and identify bottlenecks. Code without tests is hard to debug and refactor. Messy code is also prone to production bugs. Learning to test will take some upfront time, but will save you from a lot of application errors.
- Apache Spark in 24 hours is a great book on the current state of big data technologies
- Advanced Analytics with Spark is great for learning how to run machine learning algorithms at scale
- Learning Spark is useful if you're using the RDD API (it's outdated for DataFrame users)

## Beginner books

### Apache Spark in 24 Hours, Sams Teach Yourself

Jeffrey Aven has 30 years of industry experience and is an experienced teacher / consultant in Australia. His experience and desire to teach topics in a logical manner makes his book a great place to learn about Spark and how it can fit into a production grade big data ecosystem.

The book focuses on PySpark, but also shows examples in Scala. It does a good job covering the different APIs, but not getting bogged down by showing every single example with all the programming languages supported by Spark.

Chapter 16 focuses on the R API for the few readers that are interested in R. The author doesn't bog down the entire text with R examples.

The author's extensive experience in the big data industry allows him to give a wonderful overview on how Spark fits in a big data tech stack. He talks about HDFS, YARN, Hadoop. AWS ec2, AWS EMR, Databricks, and MapReduce.

A tech stack that includes Spark won't include all of the aforementioned technologies, but it's easy to skip sections that aren't relevant. I loved learning about important technologies that I don't work with directly, like HDFS, at a high level.

The main downside of the book is the focus on RDDs instead of DataFrames. The DataFrame API is easier to work with and RDDs should generally be avoided by practitioners.

So if you're looking for a book that'll help you get your job done this week, this isn't the best book for you. But if you're looking for a book that'll make you a better data engineer or data scientist in the next few months, this book is perfect.

Most books that cover such a wide variety of technologies are terrible. They cover too much material and don't connect the dots so the reader feels lost and overwhelmed.

Jeffrey Aven's Apache Spark in 24 Hours does a masterful job introducing the reader to technologies important to the modern big data stack, with a focus on Spark. He's uniquely qualified to write such a high quality book because he's an experience teacher and has 30 years of experience in the industry.

This book will help you appreciate the history of big data and will make you a better data scientist / data engineer.

### Spark: The Definitive Guide

Spark: The Definitive Guide is 600 page book that introduces the main features of the Spark engine. This is the best beginner Spark book as of 2019.

If you're willing to slog through a big text, you'll be able to learn from this book, but it'll require some patience.

- Four programatic APIs are covered to a varying degree, so you're bound to see code snippets that aren't relevant for you. PySpark programmers only want to see the Python API, Scala programmers only want to see the Scala API, and Java programmers only want to see the Java API. Spark books should be released in different languages - there should be one Spark: The Definitive Guide book and another PySpark: The Definitive Guide book so readers don't need to wade through irrelevant code snippets.
- Spark is a vast data engine with packages for SQL, machine learning, streaming, and graphs. Spark also provides access to lower level RDD APIs. Beginner Spark programmers should master the SQL API before moving on to the other APIs. This book introduces advanced Spark APIs before covering the important foundational SQL concepts which will confuse some readers.
- Book feels like a reference manual at times. The discussion on Spark types starts with a quick discussion on how to access the ByteType in Scala, Java, and Python and then presents a full page reference table for all the Python types, another full page table for the Scala types and and third full page table for the Java types. They note that the API can change and even link to the API documentation. Sections like these feel like a book version of the API docs.
- Topics aren't presented in a logical order. A teacher that's using this book for a college class suggests reading Chapter 1 and then skipping to Chapter 4 because Chapters 2 and 3 will confuse you. Chapter 3 dives into the K-Means clustering way before the reader is perpared to handle a complex machine learning algorithm.
- Tangents upon tangents. On page 58, the Spark types discussion is interrupted with a section on the difference between the Dataset and DataFrame APIs. This distinction is only relevant for Scala / Java users. The tangent embeds a NOTE about the Catalyst Optimizer, a low level detail that's only relevant for advanced Spark users. Introductory texts shouldn't cover highly technical low level details, especially not at the beginning of the book.
- Code snippets are difficult to read in the printed version. In the electronic version, the code snippets are in color and are easy to read. The text wasn't converted to black and white before it was printed, so the code is printed as light grey text on white paper. For readers with bad eyesight, the paperback code is unreadable.
- Python 2 was used for the Python code which wasn't a great choice because Python 2 is end of life as of January 1, 2020.

### Learning Spark

Learning Spark is a great book if you're working with Spark 1 or would like to better understand RDDs in Spark 2.

Most data engineers and data scientists that are working with Spark 2 only use the DataFrame API and don't ever need to consider the lower level RDD API.

This book is outdated for most Spark users, but it's well written / organized and a great learning tool for RDD users.

## Machine Learning

### Advanced Analytics with Spark

Advanced Analytics with Spark will teach you how to implement common machine learning models at scale. There are a lot of Python libraries that make it easy to train models on smaller data sets. The implementations in this book can be run on Spark clusters and scaled to handle massive datasets.

The book starts with an introduction to data analysis with Scala and Spark. It lays out an unfortunate truth for some readers - most of the code in this book uses the Scala API. The authors make a compelling argument for using the Scala for data science, but it's unlikely Python developers will agree ;)

The book chapters are written by Sandy Ryza, Uri Laserson, Sean Owen, and Josh Wills. The authors didn't collaborate on the individual chapters - each chapter was written by a single author.

The introduction to Scala chapter uses Spark 2.1 and covers the basics on data munging, RDDs, and DataFrames. They introduce YARN and HDFS, two technologies that the book should have avoided in my opinion. Books should aim to use the minimum number of technologies required to teach readers and HDFS isn't needed.

If you don't have any experience with Spark or Scala, this introductory chapter probably won't suffice. Spark and Scala are complex and can't be taught in a single chapter. Leave a comment if you'd like me to write a data munging book that would be a good prerequisite to this book.

The recommending music chapter is written by Sean Owen and uses the audioscribbler data set and the alternating least squares recommender algorithm. The chapter is approachable for readers that aren't familiar with the data set or the algorthm.

The chapter even gives a nice high level overview of the matrix factorization model that underlies the algorithm. The code snippets are easy to follow and the datasets are easy to find in [the GitHub repo for the book](https://github.com/sryza/aas). After the model is built, the author walks you through evaluating recommendation quality with an area under curve analysis.

The next chapter, also by Sean Owen, uses decision trees to predict forest cover using a dataset of forest covering parcels of land in Colorado. The author asserts that the tree dataset to illustarte the decision tree algorith is a coincidence - he's obviously a punny guy!

Like the previous chapter, the author covers the algorithm at a high level before digging into the details. Regression algorithms with decision trees are examples of supervised learning for building predictions.

The chapter explains how to build feature vectors and how decision trees generalize into a more powerful algorithm called random decision forests. Once the model is built, the author shows you how it can be used to make predictions.

The next chapter uses the K-means clustering algorithm to build an anomoly detection in network traffic model. I especially like how this chapter shows how to plot three dimensional data with SparkR.

It's hard to imagine, but the book continues with several more amazing chapters that I won't cover in detail. The last two chapters in the book are on genomic and neuroimaging data and are difficult to follow because the data is complex. Those are the only chapters you might end up skipping.
