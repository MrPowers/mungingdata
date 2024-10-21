---
title: "Self Publishing High Quality Programming Books"
date: "2021-10-14"
categories: 
  - "books"
---

# Self Publishing High Quality Programming Books

This post describes a workflow for self publishing programming books that readers will love.

Writing a book seems like a daunting task, but it's less intimidating if each chapter is "pre-published" as a blog post. Validating user engagement based on blog post traffic metrics gives you confidence that your writing is valuable and you have what it takes to author a book.

This post explains how you can break up the book writing process into manageable chunks (blog posts), collect them in a well organized package, and delight readers with a finished product they'll actually read end-to-end.

Lots of developers don't read programming books cause they're structured like traditional school textbooks with big blocks of text. Self publishing is awesome because you can write a lightweight, easily readable book.

Let's take a look at the process.

## Validating content via blog posts

A book is a collection of chapters, most of which can be separately published as blog posts.

Publishing chapters as blog posts gives you valuable feedback on the quality of your content. Your blog posts should get ranked by search engines, accumulate backlinks, and have average time on page of at least 5 minutes.

This post on [broadcast joins with Spark](https://mungingdata.com/apache-spark/broadcast-joins/) is a good example of a blog post that's also a great book chapter. This blog is the #1 search result for "spark broadcast join", has tens of thousands of page views, and the average time spent on the page is 5.5 minutes. We know users value this content from objective metrics.

The broadcast joins with Spark blog post met the prerequisites to be included as a chapter in the [Beautiful Spark](https://leanpub.com/beautiful-spark/) book.

Don't bother writing a book till you're able to write blog posts that get good user engagement.

A portfolio of popular blog posts is a great starting point for a book.

## Book organization

Books should have flow, so you can't simply organize the chapters by their popularity on your blog.

The blog post popularity should be the primary prioritization factor, but the overall book layout should also make sense. You may need to add some chapters that aren't blog posts to fill gaps and avoid conceptual leaps.

Book authors don't usually prioritize chapters based on metrics like blog post views or the top voted StackOverflow questions for a topic. Authors organize the table of contents based on their subject matter expertise, which isn't usually the best.

It's better to prioritize content with metrics (top Stackoverflow questions or blog post page views) instead of "personal feel".

## Most important content first

A small percentage of a language / framework API is what accounts for the majority of production code.

Look at learning Spanish as an instructive example. The most popular 1,000 words account for 92% of spoken Spanish. There are 93,000 total Spanish words, but only a small fraction of them are used in conversations.

If you want to learn Spanish, it's best to focus on the top 1,000 words.

An alphabetical dictionary is of little use for language learners. What they need is a frequency dictionary that orders words based on how often they're used.

Similarly, programming language learners should only study the most commonly used parts of the language API when starting. They should focus on getting "conversational" with a programming language before becoming "fluent".

Book writers should help readers by only covering the API methods they need to become fluent.

Authors often cover API methods that aren't widely used. They fall into the trap of writing chapters like "Chapter 5: Arrays", so they end up writing about the less important array methods, before other content that's more relevant for the reader.

It's no wonder that books like these are often put down, never to be opened again. They're just unbearably boring.

Books should be unified with a thesis that'll motivate the readers to power on, even through the difficult technical content.

## Backing into a thesis

Once you have an idea of how your book chapters will be organized, you're ready to tie them together with a unifying thesis.

Programming books often do not have a thesis, so they're really boring. Don't fall into the trap of writing a book that's an "API documentation narrative".

Here's the thesis for the [Spark book I wrote](https://leanpub.com/beautiful-spark/): You should follow the design patterns outlined in this book to build production grade Spark applications that are maintainable, testable, and easily understandable, even by Scala newbies.

The thesis makes the book more engaging, even for readers that don't agree. A book that argues a point is better than a book that simply describes a language without any opinion at all.

## Special sauce

Your book should provide some deeper insights than what the audience can absorp from reading the blog post "chapters" individually.

Luckily this special sauce is a natural byproduct of organizing the most important content on a subject and neatly tying it together with a unifying thesis.

Just follow the process and the higher level insights will flow naturally.

## Writing style

Modern programming books should follow the same writing practices as blog posts.

Blog posts should be written with basic English, short sentences, and one or two sentence paragraphs.

Books generally use advanced language, longer sentences, and big paragraphs. Programming books that follow this writing style feel like university textbooks.

Formal writing makes a book "feel" more authoritative, but readers don't care about that anymore. If your a topic matter expert, you can write casually, skip making an appeal to authority, and keep the respect of your audience.

Attention spans are shrinking in the era of constant interruptions, so textbook style writing is even less desirable now.

Just stick with "blog like" writing and avoid creating something that feels like it's been assigned from a university professor.

## Limited scope

Programming books are usually too long, so readers feel like finishing them is an insurmountable task.

Make your book short, like a novel, so readers have hope they can finish the book some day.

Novels are usually 300-400 pages of light reading whereas programming books are often 500 pages+ of dense text.

It's better to give readers a 200-250 page programming book that's relatively easy to read. Write two books if you have 500 pages worth of content.

I separated [Testing Spark Applications](https://leanpub.com/testing-spark) to a separate book to avoid making [Beautiful Spark](https://leanpub.com/beautiful-spark/) too long. Separate books allow for distinct thesis statements, which makes the book more cohesive.

Try to make a book that's not intimidating and readers want to consume from cover-to-cover. Don't write a reference text with big sections that the user will skip.

## Teaching to fish

Give your readers enough fundamental insights so they're well prepared to learn additional details after reading your book. Your book doesn't need to teach everything, just enough so your readers are able to effectively learn more independently.

Here's an example of how I selected the material to be covered in Beautiful Spark. Spark has four main APIs: RDDs, DataFrames, streaming, and machine learning. I focused exclusively on the DataFrame API because that gives the foundational knowledge readers need to learn about the streaming and machine learning APIs. The RDD API isn't commonly used. The narrow scope allows for a small book that's still enlightening.

I'd write separate books for Spark streaming and machine learning if I was going to write more Spark content. Those books are comparatively less important because once readers have a good mastery of the Spark DataFrame API, they can figure out machine learning and streaming themselves.

Teach readers core topics and help them get good enough to Google wisely and become self sufficient.

## Marketing

As previously discussed, you should write chapters as blog posts that get traffic before writing a book.

After your book is published, you can add links to your book in the blog posts to make sales. This makes it really easy for readers that like your writing style to get more content.

## Underlying motivations

High quality programming books are a great way to give back to the programming community, make others more productive, and spread knowledge.

Programming books are not a great way to get recognition or make money.

Make sure your underlying motivations are aligned with realistic goals when writing the book.

I am interested in making data professionals working on important problems more efficient (e.g. climate change, oceanography, and health care). Writing a book on a data processing technology is aligned with one of my life goals. I don't recommend writing a book unless it's aligned with your core values.

## Conclusion

You can self publish books that add a lot of value to other programmers.

I wrote a book on a tiny sliver of the Spark API in an easily digestible manner. Many readers have told me that my book helped them "get over the hump" and finally learn Spark without feeling so intimidated by the technology.

I encourage more developers to write books that fill niches so technology is easier to learn. I'd rather read a 200 page book to get started on a new technology than scour API doc pages and try to decipher the important topics from StackOverflow questions.

More developers will start reading books if they're written with the lightweight style outlined in this post. Most programmers don't want to read "reference manual" style books.
