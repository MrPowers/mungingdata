---
title: "Creating open source software is a delight"
date: "2020-12-16"
categories: 
  - "oss"
---

Writing open source software gives you the opportunity to collaborate with highly motivated developers and build awesome code that's used by folks around the world.

You can influence community best practices and help others be more productive.

At random intervals, you're surprised with wonderful commits from internet strangers that make your code better or offer feedback on how to improve your library.

You're forced to write better code by clearly separating application logic from open-sourceable, generic functionality.

The open source community will give you great feedback on the quality of your code. You can iterate based on this feedback and build better interfaces (assuming you're psychologically capable of taking feedback).

Some folks think open sourcing code is a bad deal. You don't make any money, might be saddled with a difficult maintenance task, and will receive more complaints than positive feedback. This post teaches you how to manage the negatives.

## Great collaboration

The folks that contribute to open source code smart and motivated.

They help you make contributions to your libraries that you might not have the time or technical chops to add yourself.

Once establishing a positive working relationship with your contributors, you can bounce ideas off them, even if they aren't directly related to your project.

I just emailed one of my contributors a really hard Spark question earlier today. The type of question that is unlikely to get answered on Stackoverflow. Thanks to my open source work, I have great Spark friends who are always willing to help.

You can also turn your contributors into IRL friends. One of my Armenian contributors has already offered to give me a tour of Yerevan when I visit his country. He's the main organizer of the Armenian data science community and he'll be able to introduce me to a bunch of great devs.

When I go to conferences, I get to catch up with my library users around the world. It's awesome.

## Career growth

Developers that build popular open source libraries get high quality job offers, especially if their library is a core dependency.

I'm not talking out the "recruiters" that send Linkedin messages trying to trick you into applying for a job. I'm talking about messages from the CTO or engineers that are really trying to recruit you. They've seen your code and will actively assist you through the hiring process.

One of my open source friends created a great project, donated it to Apache, and was then recruited to a high-paying, interesting, prestigious job. His wonderful open source work is what gave him that opportunity.

## Forcing better abstractions

Open source code forces you to separate application logic from generic functionality.

An open source abstraction makes your private business logic cleaner because it removes all the generic logic. Private repos that mix generic functions with business logic are unnecessarily complex.

Your mileage may vary. [Prefer duplication over the wrong abstraction](https://sandimetz.com/blog/2016/1/20/the-wrong-abstraction). Developers are generally good at generating business value, but they're not great at making clean abstractions.

It hard to acquire library users if you don't provide them with an elegant API.

## Is open source extra work?

The open source code you write should be code you'd write in a private repo anyways.

Copy pasting generic functions to an open source repo isn't extra work.

The challenges you face in private repos should be the motivation for your open source code.

## Maintenance burden

Maintaining open source code ranges from a trivial task to a full-time job.

If you build an open source project on an unstable API, it'll be more work.

Some languages and frameworks are less stable than others. Don't depend on volatile code if you don't want to do a lot of maintenance work.

As the open source maintainer, you choose what versions you support. You don't need to support old versions if you don't want to.

If you leave an ecosystem or simply don't want to maintain the project anymore, you're free to archive it or tell people to fork it. You don't have any obligations to maintain a project for unpaid users.

The maintainer of faker.js recently made an announcement that companies [would have to start paying him or fork the lib](https://github.com/Marak/faker.js/issues/1046). That's great, maintainers can quit whenever they want.

## Money

You shouldn't build open source code with the intention of making money.

Open source code is to help other developers be more productive and to altruistically give back to the community.

An open source project that's vital for companies can be a great way to launch a successful company. Apache Spark was started as an open source project and the founder used the open source momentum to launch Databricks, a multi-billion dollar enterprise.

While an open source project might turn into a big company, that shouldn't be your motivation.

## Unhappy users

You won't make all users happy and the more popular your project, the more complaints you'll face.

Happy users will use your code and never say anything to you. The unhappy folks may open rude issues or send you nasty emails. Even if the happy/unhappy ratio is 100/1, all you'll get is bad feedback.

Open source developers need to accept this asymmetrical feedback as an unfortunate reality of life.

If you're happy with an open source project, go out of your way to email the creator to thank them. They put a lot of effort into the project and a nice email goes a long way.

## Hypersensitive developers

Some developers are really sensitive when it comes to feedback, especially feedback on their code.

When you submit a pull request on their code, they almost view it as an invasion of privacy. How dare you refactor their beautiful artwork!

If you really don't like people touching your code, then just keep it private.

I worked in finance for 5 years and we were always heavily "refactoring" presentations, documents, and spreadsheets of other team members. There wasn't a big "finance documents ownership" problem.

A greater percentage of developers seem to struggle with code ownership. If you struggle with feedback on your code, you might want to stick away from open source development.

## Why open source projects fail

Some developers are disenchanted with open source work because they tried to build a project and it wasn't successful. They worked hard, built something they thought was great, and the users never came.

Open source projects can fail for a variety of reasons.

The README needs to offer a compelling value proposition to users. Something like "this library will make your life better for XyzReason".

Most READMEs are not compelling. A lot of them are hard to follow. Most READMEs don't even state the problem the library is designed to solve.

Other READMEs give you a quick glimpse into a complicated user interface. Developers don't adopt libraries that are hard to use.

You need to market open source projects. You should have a reliable traffic source that guides users to your README, so they can learn all about your great code and start using it.

Open source failure is a difficult reality for some developers to face. You put yourself out there, thought you were enlightening the world, and were shunned by potential users.

It easier to succeed with closed source projects because you have a captive audience. You might build a component that's not particularity easy to use, but still get adopters because they're basically forced to use your solution. Open source users are not captive and will quickly click the back button on your README if they're not impressed.

## You don't need to be a genius developer

You don't need to be a great developer to build awesome open source code.

Simple functions that make common tasks easier are fine.

API wrappers work.

Creating revolutionary open source libraries might be your end goal. Start by building a trivial library that gets a few hundred stars.

## Conclusion

Open source development isn't for everyone. Hypersensitive developers don't appreciate feedback from strangers. Developers who think they're code is better than it actually is will be disappointed by lack of adoption.

A large chunk of developers can happily exist in the open source ecosystem. They'll find great collaborators to help them with hard questions. They'll find joy in seeing their code collect hundreds of stars and get downloaded thousands of times a day.

They'll also be ecstatic when the CTO of a company tracks them down and offers them their dream job out of the blue.

It takes years to get traction in open source. Learning how to build code that other developers love takes time. Start by trying to get a few stars, then a hundred, then thousands.

Writing open source code and participating in such a collaborative community is my favorite part of being a developer.
