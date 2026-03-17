# The Nervous System for Data Science

> Data teams don't have a context problem. They have a context *management* problem, and until now, they've been solving it with humans.

---

## First: Understanding Rules

Before we can talk about what Dante does, we need to talk about one foundational concept: **rules**.

If you've been deep in the LLM world for a while, you already know rules intimately. You use them constantly. But if you're a data scientist or analytics engineer who's been focused on your actual job rather than prompt engineering, this might be new. And it's essential to understanding why Dante works the way it does.

Rules are persistent instructions that shape how an AI behaves. Not a one-off prompt. Not a message you type once and hope the model remembers. A *system of behavior* that applies every time, automatically. Think of it as the difference between telling a new hire something once in passing versus handing them a handbook on their first day.

### Rules as Behavioral Systems

Here's the conceptual leap: rules aren't just about writing accurate SQL, though that's the most important function. They're about creating **repeatable and eventually centralizable systems of behavior**.

A rule isn't "help me write a query." A rule is "here's how we write queries, every time, for everyone." That distinction matters enormously, and it's the foundation of everything we're building.

### Example: Dante's Own Rules

When you install Dante Library, it immediately sets up a CLAUDE.md file (a rules document) that tells the AI system how to use Dante's tools. Search the knowledge base before writing SQL. Describe unfamiliar tables before querying them. Save validated patterns for future use. These aren't suggestions. They're the workflow, enforced automatically.

Every user who installs Dante Library gets the same behavior. The same workflow. The same access to the same tools. No training session. No onboarding deck. The rules *are* the onboarding.

### Example: Personal Consistency

You're a data scientist. You make charts, dashboards, presentations. Without rules, every output looks slightly different: different colors, different fonts, different layout choices. You're reinventing your own style every time you create something.

With rules, you define your preferences once. Your color palette. Your chart styling. Your preferred layout for HTML apps. Now every time you create something, whether through Dante, Gamma, or raw HTML, it comes out looking like *you*. Consistent. Professional. Without thinking about it.

This is already powerful for one person. Your AI assistant doesn't just write code. It writes code *your way*, every time.

### Example: Agent Personality

Rules also control how the AI *talks to you*. By default, AI assistants are verbose. Long explanations, caveats, preamble. That's fine for a one-off question. But when you're doing significant back-and-forth data work, iterating on queries, refining charts, debugging joins, verbosity kills the flow.

Dante's rules make the agent slightly more terse than default. This is deliberate. It makes the conversational rhythm feel like working with a sharp colleague, not reading a textbook. And there's a practical benefit too: fewer tokens means faster responses and lower cost. When you're doing fifty exchanges in a session, that adds up fast.

Rules give you control not just over *what* the AI does, but *how* it works with you. Personality as configuration.

And when we get to the company level later, imagine this consistency scaled across an entire team. But we'll get there.

---

## The SQL Your Company Already Has

Now that we understand rules and how to shape AI behavior, let's apply the same principle to data. Rules give the AI a handbook. But what gives the AI the *knowledge* to write accurate SQL for your specific company?

Here's the key realization: your company is already sitting on an enormous amount of well-written SQL. It's not in a document. It's not in a wiki. It's in the charts and dashboards your data team has already built and curated.

### Ingestion: Mining Your Existing Work

Dante connects to the BI systems you already use (Looker, Databricks, and others) and examines every chart on every dashboard. For each one, it extracts the underlying SQL and generates a plain-English question that describes what that query answers. Question-SQL pairs.

Then it creates embeddings from those pairs: vector representations that capture the semantic meaning of each question and its corresponding SQL.

The scale of this matters. In practice, at real companies, this generates *thousands* of highly valuable embeddings. Not synthetic training data. Real SQL that real analysts wrote, and that real stakeholders validated by putting it on a dashboard.

### What This Captures

These embeddings don't just capture which tables people use. They capture the *intricate, company-specific logic* that makes SQL actually correct at your company:

- The WHERE clauses that filter out internal and test accounts
- Which of two similarly named columns is the one you actually want (`user_id` vs `account_user_id`)
- The specific JOINs and date logic that define your company's metrics
- The business rules baked into existing dashboards that nobody ever documented but everyone relies on

This is tribal knowledge, extracted automatically, without anyone having to write it down.

### Beyond Dashboards

Dashboards are just the starting point. There are other rich sources of validated SQL that companies are already sitting on.

**Query history tables.** Most warehouses (Snowflake, Databricks, BigQuery) keep logs of every query run. Dante can examine these to build domain-specific knowledge of what each data scientist and data engineer is actually doing. The queries that ran successfully, that were run repeatedly, that touched the tables you care about: that's signal. We can create context from the successful work people are already doing, without them lifting a finger.

**GitHub repositories.** This is especially common with analytics engineering teams. dbt projects, scheduled pipelines, transformation scripts, all living in version control. That code is already reviewed, tested, and validated by the team. It's arguably the highest-quality SQL context a company has, and it's just sitting there in repos. Dante can ingest it and make it searchable.

The pattern is the same every time: find where good SQL already lives, extract it, create question-SQL pairs, embed it, make it searchable. The more sources you connect, the richer the context becomes.

### Vector Search During Analysis

Once ingested, Dante naturally performs vector search during every analysis. When you ask a question, before writing any SQL, it searches the embedding database for semantically similar questions that your team has already answered.

The AI isn't starting from scratch. It's looking at what your data scientists have already done and curated. It's referencing real, validated work.

This is what turns Claude from a very smart generalizer, an AI that knows SQL in the abstract, into a brilliant data scientist with deep knowledge of *your specific company*. Not because someone hand-wrote a context document, but because the system mined the work that was already done.

### Dante Library: Your Personal Foundation

This is what Dante Library gives the individual data scientist: the ability to build all of this context for yourself, right now, for free.

You install it. You connect it to your warehouse. You run the ingestion. And now you have a local knowledge base (embeddings, glossary terms, validated patterns) that lives with you.

The critical point: every time you start a new project or create a new folder, you're not starting from zero. You're starting from a foundation of data expertise. Your AI assistant already knows your tables, your business logic, your validated queries. Day one of a new analysis feels like day one hundred.

And it compounds. Every query you validate, every term you define, every pattern you save gets added to your local knowledge base. Your next project is smarter than your last one. Not because you got smarter (though you did), but because your *system* got smarter.

This is what we mean by a "node": a self-contained, individual knowledge system that makes one data scientist dramatically more productive. No team required. No centralized platform. Just you and your context, always available.

### Other Types of Context

Embeddings and vector search are the core. But there are other types of knowledge that round out the system.

**Notes and rules.** We introduced rules earlier. Now here's how Dante makes them practical. Dante distinguishes between two levels. Rules are global: always included, every project, every session. These are powerful because they're universal. Business-specific acronyms, names of domain experts, presentation standards, anything you want the AI to always know regardless of what you're working on. Notes are project-specific: context that matters for this analysis but not every analysis.

The key is that Dante automatically imports your global rules into every project. You define them once. They propagate everywhere. This is the callback to our earlier discussion: Dante doesn't just support rules. It makes them universal for you.

**Keyword-based knowledge.** Keywords are a different mechanism: context that gets injected automatically based on what you're talking about. Not always-on like rules. Not similarity-based like embeddings. Triggered by a specific word or phrase.

Here's a real example. I've been working with Polymarket data recently. It's fairly complex, with multiple tables, non-obvious relationships, and specific conventions for how markets and outcomes are structured. After working with it for a while, I had the agent write a detailed plain-English description of the entire data model. Not just SQL, but a written explanation of how everything fits together. Then I saved that as a keyword trigger on "Polymarket."

Now, anytime I type the word "Polymarket" in a question, that entire context document gets injected automatically. The AI immediately knows the data model, the conventions, the gotchas, all before I've even finished asking my question. It kickstarts the project.

This is especially useful for complex or recurring domains. You do the deep work once, capture it as a keyword, and every future session starts with that understanding built in.

### The Payoff

After a couple of weeks of working with Dante Library (ingesting your dashboards, validating queries, defining terms, saving keywords) something shifts. Your computer becomes an expert data scientist. Not a general-purpose AI that knows SQL in the abstract. An expert that has *your* context, accesses it as easily as you do, and writes SQL as accurately as you would write it.

That's the promise of Dante Library at the individual level. And it's available now, for free.

---

## Why This Has to Be Intelligent

Everything we've described so far, Dante Library and the individual node, is already powerful. But here's the critical question: what happens at the company level?

The tools that exist today (Databricks semantic layers, Looker's governed metrics, dbt docs) all require the same thing: **humans monitoring and maintaining context**. Data analysts assigned to curate rule sets. Engineers building tiered context layers. Documentation committees. Governance processes.

That's a step behind where this has to be.

The failure mode isn't that these tools are bad. It's that they require human intervention to work. And human-maintained context decays. The moment someone stops tending the semantic layer, it starts falling behind. New tables appear. Business logic changes. Metrics get redefined. The golden source of truth is only golden as long as someone is polishing it.

What you actually need is **intelligent context management that is always occurring**: always generalizing, always simplifying, always learning from the work of your team. Not a system that waits for someone to update a wiki. A system that watches work happen and becomes smarter from it.

Context management should not require human intervention. We have intelligence now. The AI itself should be doing this work.

---

## What's Next

Thank you for taking the time to read this. This is just the beginning, the first in a series where we'll go progressively deeper into how this system works and what it makes possible.

**Data Applications.** We mentioned presentations and dashboards in passing. We're going to show how Dante creates what we call Data Apps, and how beautiful they can be. Interactive, shareable, built from your data with the consistent styling that rules give you.

**The Brain: Dante Studio.** We'll do a deep dive on the company-wide system, exploring how shared context works and how the brain consolidates, deduplicates, and simplifies knowledge. The key insight: the system doesn't learn as fast as one person. It learns as fast as the *aggregate work of all your data scientists and engineers combined*.

**Rules vs. Claude.md.** A technical deep dive on the difference between Claude.md files and rules, how they interact, and how to think about structuring them for data work.

**Skills.** What skills are, how they differ from rules, and how they can be used to create powerful, reusable workflows for data science.

For now, Dante Library is available, it's free, and it will make you a dramatically more productive data scientist. Try it.
