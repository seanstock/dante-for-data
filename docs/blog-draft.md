# The Nervous System for Data Science

> Data teams don't have a context problem. They have a context *management* problem — and until now, they've been solving it with humans.

---

## First: Understanding Rules

[TODO: This is the on-ramp for the video/post. Before we can talk about what Dante does, the audience needs to understand one foundational concept: **rules** (aka Claude.md files, system instructions, etc.).]

[TODO: Address the audience split — power users of LLMs already know rules deeply and use them constantly. But data scientists and analytics engineers often aren't up to speed on this. And rules are *essential* to understanding why Dante works the way it does.]

[TODO: What rules are — persistent instructions that shape AI behavior. Not a one-off prompt. A *system of behavior* that applies every time. The difference between telling someone something once and giving them a handbook.]

### Rules as Behavioral Systems

[TODO: The key reframe — rules aren't just about writing accurate SQL (though that's the most important function). They're about creating **repeatable and eventually centralizable systems of behavior**.]

[TODO: This is the conceptual leap. A rule isn't "help me write a query." A rule is "here's how this company writes queries, every time, for everyone."]

### Example: Dante's Own Rules

[TODO: When you install Dante Library, it immediately sets up rules (CLAUDE.md) that tell the AI system how to access Dante's tools — search knowledge first, describe tables before querying, save validated patterns, etc. That's one core example of rules in action.]

### Example: Personal Consistency

[TODO: This is the individual-level payoff. You're a data scientist. You make charts, dashboards, presentations. Without rules, every output looks slightly different — different colors, different fonts, different layout choices. You're reinventing your own style every time.]

[TODO: With rules, you define your preferences once — your color palette, your chart styling, your preferred layout for HTML apps. Now every time you create something, whether through Dante, Gamma, or raw HTML, it comes out looking like *you*. Consistent. Professional. Without thinking about it.]

[TODO: This is already powerful for one person. Your AI assistant doesn't just write code — it writes code *your way*, every time. That's what rules give you at the individual level.]

### Example: Agent Personality

[TODO: Rules also control how the AI *talks to you*. By default, AI assistants are verbose — long explanations, caveats, preamble. That's fine for a one-off question. But when you're doing significant back-and-forth data work — iterating on queries, refining charts, debugging joins — verbosity kills the flow.]

[TODO: Dante's rules make the agent slightly more terse than default. This is a deliberate choice. It makes the conversational rhythm feel like working with a sharp colleague, not reading a textbook. And it has a practical benefit too: fewer tokens means faster responses and lower cost. When you're doing 50 exchanges in a session, that adds up fast.]

[TODO: This is another thing rules give you — not just *what* the AI does, but *how* it works with you. Personality as configuration.]

[TODO: (Tease the later payoff — and when we get to the company level, imagine this consistency scaled across an entire team. But we'll get there.)]

---

## The SQL Your Company Already Has

[TODO: Transition — now that we understand rules (how to shape AI behavior), let's apply the same principle to *data*. Rules give the AI a handbook. But what gives the AI the *knowledge* to write accurate SQL for your specific company?]

[TODO: The key realization — your company is already sitting on an enormous amount of well-written SQL. It's not in a document. It's not in a wiki. It's in the charts and dashboards your data team has already built and curated.]

### Ingestion: Mining Your Existing Work

[TODO: What Dante does — it connects to the BI systems you already use (Looker, Databricks, etc.) and examines every chart on every dashboard. For each one, it extracts the underlying SQL and generates a plain-English question that describes what that SQL answers. Question-SQL pairs.]

[TODO: Then it creates embeddings from those pairs — vector representations that capture the semantic meaning of each question and its corresponding SQL.]

[TODO: The scale of this — in practice, at real companies, this generates *thousands* of highly valuable embeddings. Not synthetic training data. Real SQL that real analysts wrote and real stakeholders validated by putting it on a dashboard.]

### What This Captures

[TODO: This is where it gets powerful. These embeddings don't just capture which tables people use. They capture the *intricate, company-specific logic* that makes SQL actually correct at your company:]

[TODO: Examples:
- The WHERE clauses that filter out internal/test accounts
- Which of two similarly named columns is the one you actually want (e.g., `user_id` vs `account_user_id`)
- The specific JOINs and date logic that define your company's metrics
- The business rules baked into existing dashboards that nobody ever documented but everyone relies on]

[TODO: This is tribal knowledge — extracted automatically, without anyone having to write it down.]

### Beyond Dashboards: Other Sources of Context

[TODO: Dashboards are just the starting point. There are other rich sources of validated SQL that companies are already sitting on:]

[TODO: **Query history tables** — Most warehouses (Snowflake, Databricks, BigQuery) keep logs of every query run. Dante can examine these to build domain-specific knowledge of what each data scientist and data engineer is actually doing. The queries that ran successfully, that were run repeatedly, that touched the tables you care about — that's signal. We can create context from the successful work people are already doing, without them lifting a finger.]

[TODO: **GitHub repositories** — This is especially common with analytics engineering teams. dbt projects, scheduled pipelines, transformation scripts — all living in version control. That code is already reviewed, tested, and validated by the team. It's arguably the highest-quality SQL context a company has, and it's just sitting there in repos. Dante can ingest that and make it searchable.]

[TODO: The pattern is the same every time — find where good SQL already lives, extract it, create question-SQL pairs, embed it, make it searchable. The more sources you connect, the richer the context becomes.]

### Vector Search During Analysis

[TODO: Once ingested, Dante naturally performs vector search during every analysis. When you ask a question, before writing SQL, it searches the embedding database for semantically similar questions that your team has already answered.]

[TODO: The effect — the AI isn't starting from scratch. It's looking at what your data scientists have already done and curated. It's referencing real, validated work.]

[TODO: This is what turns Claude from a very smart generalizer — an AI that knows SQL in the abstract — into a brilliant data scientist with deep knowledge of *your specific company*. Not because someone hand-wrote a context document, but because the system mined the work that was already done.]

### Dante Library: Your Personal Foundation

[TODO: This is what Dante Library (dante-ds) gives the individual data scientist — the ability to build all of this context for yourself, right now, for free.]

[TODO: You install it. You connect it to your warehouse. You run the ingestion. And now you have a local knowledge base — embeddings, glossary terms, validated patterns — that lives with you.]

[TODO: The critical point — every time you start a new project or create a new folder, you're not starting from zero. You're starting from a foundation of data expertise. Your AI assistant already knows your tables, your business logic, your validated queries. Day one of a new analysis feels like day 100.]

[TODO: And it compounds. Every query you validate, every term you define, every pattern you save — it gets added to your local knowledge base. Your next project is smarter than your last one. Not because you got smarter (though you did), but because your *system* got smarter.]

[TODO: This is what we mean by a "node" — a self-contained, individual knowledge system that makes one data scientist dramatically more productive. No team required. No centralized platform. Just you and your context, always available.]

### Other Types of Context

[TODO: Transition — embeddings and vector search are the core. But there are other types of knowledge that round out the system.]

#### Notes and Rules

[TODO: We introduced rules earlier — now here's how Dante makes them practical. Dante distinguishes between two levels:]

[TODO: **Rules (global)** — Always included, every project, every session. These are powerful because they're universal. Examples:
- Business-specific acronyms and their meanings
- Names of domain experts ("for revenue questions, Sarah Chen is the authority")
- Presentation standards — your company colors, chart defaults, formatting preferences
- Anything you want the AI to *always* know, regardless of what you're working on]

[TODO: **Notes (project-specific)** — Context that matters for *this* analysis but not every analysis. Project-scoped knowledge that stays relevant within a folder.]

[TODO: The key — Dante automatically imports your global rules into every project. You define them once. They propagate everywhere. This is the callback to our earlier discussion about rules: Dante doesn't just support them, it makes them universal for you.]

#### Keyword-Based Knowledge

[TODO: Keywords are a different mechanism — context that gets injected automatically based on what you're talking about. Not always-on like rules. Not similarity-based like embeddings. Triggered by a specific word or phrase.]

[TODO: **The Polymarket example** — Real story. Polymarket data is fairly complex — multiple tables, non-obvious relationships, specific conventions for how markets and outcomes are structured. After working with Polymarket data for a while, I had the agent write a detailed plain-English description of the data model — not just SQL, but a written explanation of how everything fits together. Then I saved that as a keyword trigger on "Polymarket."]

[TODO: Now, anytime I type the word "Polymarket" in a question, that entire context document gets injected automatically. The AI immediately knows the data model, the conventions, the gotchas — before I've even finished my question. It kickstarts the project.]

[TODO: This is especially useful for complex or recurring domains. You do the deep work once, capture it as a keyword, and every future session starts with that understanding built in.]

### The Payoff

[TODO: Bring it home for the individual. After a couple of weeks of working with Dante Library — ingesting your dashboards, validating queries, defining terms, saving keywords — something shifts. Your computer becomes an expert data scientist. Not a general-purpose AI that knows SQL in the abstract. An expert that has *your* context, accesses it as easily as you do, and writes SQL as accurately as you would write it.]

[TODO: That's the promise of Dante Library at the individual level. And it's available now, for free.]

---

## Why This Has to Be Intelligent

[TODO: Now zoom out. Everything we've described so far — Dante Library, the individual node — is already powerful. But here's the critical question: what happens at the company level?]

[TODO: The tools that exist today — Databricks semantic layers, Looker's governed metrics, dbt docs — they all require the same thing: **humans monitoring and maintaining context**. Data analysts assigned to curate rule sets. Engineers building tiered context layers. Documentation committees. Governance processes.]

[TODO: That's a step behind where this has to be.]

[TODO: The failure mode isn't that these tools are bad. It's that they require human intervention to work. And human-maintained context decays. The moment someone stops tending the semantic layer, it starts falling behind. New tables appear. Business logic changes. Metrics get redefined. The golden source of truth is only golden as long as someone is polishing it.]

[TODO: What you actually need is **intelligent context management that is always occurring** — always generalizing, always simplifying, always learning from the work of your team. Not a system that waits for someone to update a wiki. A system that watches work happen and *becomes smarter from it*.]

[TODO: Context management should not require human intervention. We have intelligence now. The AI itself should be doing this work.]

---

## A Nervous System, Not a Brain

[TODO: The metaphor. A company's data infrastructure needs a nervous system — sensory nodes that capture context where work happens, and a central brain that synthesizes it all.]

[TODO: Most AI data tools try to be the brain. They want to be the single place you go. But context doesn't live in one place — it lives where the work happens. In the terminal. In the notebook. In the IDE. In the BI tool.]

[TODO: What you actually need is a nervous system — nodes everywhere, connected to a central intelligence.]

---

## Dante Library: The Nodes

[TODO: Introduce Dante Library — free, open, individual-level.]

[TODO: What it does: turns Claude Code (or any AI coding environment) into a data science workbench with a persistent, searchable knowledge base. SQL patterns, business glossaries, keyword triggers, semantic embeddings — all captured effortlessly as you work.]

[TODO: The key insight — it doesn't ask you to maintain documentation. It captures context as a byproduct of doing your actual work. You write a query, validate it, thumbs-up it — and it becomes searchable knowledge. No extra steps.]

[TODO: For an individual data scientist, this is already transformative. Your AI assistant remembers every validated query, every business term you've defined, every pattern you've confirmed. It compounds over time.]

[TODO: Dante Library is free because the node-level value proposition should have zero friction.]

---

## Dante Studio: The Brain

[TODO: Introduce Dante Studio — not just centralized storage. It's an *intelligent curator*. The brain doesn't just hold knowledge — it actively processes, consolidates, and improves it.]

[TODO: The network effect problem — 10 data scientists each build brilliant local context, but the company doesn't benefit. Studio connects the nodes.]

### The Feedback Loop

[TODO: The primary mechanism. A user works in their Library node, gets a good result, and upvotes it. That signal flows to Studio. Studio integrates the validated SQL + question into the central semantic database (pgvector). Now every node benefits from that one person's work.]

[TODO: This is the key — the only human action is "this worked." Everything after that is automatic.]

### Intelligent Consolidation

[TODO: This is where Studio earns the word "brain." It doesn't just collect queries — it *thinks about them*.]

[TODO: **Deduplication & merging** — Two analysts write queries that do the same thing but use different columns. A dumb system stores both. Studio recognizes they're semantically identical, merges them into one richer pattern with more explanation and more plain-language context. Two entries become one better entry.]

[TODO: **Grouping related queries** — Studio finds queries that touch the same domain and groups them into comprehensive documents. Not just "here's a query" but "here's everything we know about revenue calculations" — a living document that gets found during semantic search because it has density of context.]

[TODO: **Simplification** — Looker generates monstrous SQL. Studio simplifies it. Strips the unnecessary nesting, the redundant CTEs, the auto-generated aliases. Turns machine SQL into human-readable SQL that an analyst can actually learn from and adapt.]

[TODO: **Enrichment** — Studio adds plain-language descriptions, tags tables and columns with business meaning, connects queries to glossary terms. It builds the connective tissue that makes raw SQL into actual knowledge.]

### The Semantic Database

[TODO: At the core of Studio is a vector database (pgvector) that doesn't just store embeddings — it's continuously refined by the brain. Better consolidation = better embeddings = better search = better context delivered to every node.]

[TODO: This is the flywheel. More usage → more upvotes → more knowledge → better consolidation → better search → better results → more upvotes.]

---

## How It Works Together

[TODO: The full picture. Library nodes capture context at the edges. Studio synthesizes and distributes it centrally.]

```
              ┌──────────────────────────────────┐
              │       Dante Studio (Brain)        │
              │                                    │
              │  Consolidate · Deduplicate · Merge │
              │  Simplify · Enrich · Reindex       │
              │                                    │
              │     [ Semantic Vector Database ]    │
              └──┬──────────┬──────────┬───────────┘
                 │          │          │
            curated    curated    curated
            context    context    context
              ↓          ↓          ↓
        ┌─────┴──┐ ┌────┴───┐ ┌───┴─────┐
        │Library │ │Library │ │Library  │
        │(Alice) │ │(Bob)   │ │(Carol)  │
        └───┬────┘ └───┬────┘ └───┬─────┘
            │          │          │
         upvote     upvote     upvote
         signals    signals    signals
            ↑          ↑          ↑
        [ work ]   [ work ]   [ work ]
```

[TODO: The flow — work happens locally. The only human signal is "this worked" (upvote). That signal flows up to Studio. Studio does the hard work: deduplication, merging, simplification, enrichment. Curated, consolidated context flows back down to every node. No documentation. No maintenance. No human intervention in the pipeline.]

---

## The Vision: Effortless Context

[TODO: Paint the future. A new analyst joins the company. They install Dante Library, connect to Studio. Immediately, every validated query, every business definition, every pattern their predecessors discovered is available to their AI assistant. No onboarding docs. No "ask Sarah." The nervous system already knows.]

[TODO: Context management becomes infrastructure, not process. Like how you don't think about DNS — it just works. Context should just work.]

[TODO: The companies that figure this out first will have a compounding advantage. Every query makes the system smarter. Every analyst makes every other analyst more productive. That's the network effect of context.]

---

## What's Next

[TODO: Closing — thank the reader/viewer for their time. Set up the series.]

[TODO: This is just the beginning. In future posts/videos, we'll go deeper on:]

[TODO: **Data Applications** — We mentioned presentations and dashboards in passing. We're going to show how Dante creates what we call Data Apps — and how beautiful they can be. Interactive, shareable, built from your data with the consistent styling that rules give you.]

[TODO: **The Brain: Dante Studio** — We'll do a deep dive on the company-wide system. How shared context works. How the brain consolidates, deduplicates, and simplifies. And the key insight: the system doesn't learn as fast as one person — it learns as fast as the *aggregate work of all your data scientists and engineers combined*.]

[TODO: **Rules vs. Claude.md** — A technical deep dive on the difference between Claude.md files and rules, how they interact, and how to think about structuring them.]

[TODO: **Skills** — What skills are, how they differ from rules, and how they can be used to create powerful, reusable workflows for data science.]

[TODO: For now — Dante Library is available, it's free, and it will make you a dramatically more productive data scientist. Try it.]

---

*[Footer / author info]*
