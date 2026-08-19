# Product

## Register

brand

## Platform

web

## Users

The primary audience is cinephiles and Criterion Collection fans, including viewers of the *Closet Picks* YouTube series who want to look up exactly what a specific guest pulled off the shelves — the film, the verbatim quote, and the timestamped moment in the video. They arrive two ways: browsing for discovery (who's been in the closet, what gets picked most) and hunting a specific name or film. Their context is unhurried and self-directed; nobody is here under deadline.

The site also publishes a plain-text export for LLMs and agents (`/llm-export`), so machine readers are treated as a real consumer of the data, not an afterthought.

## Product Purpose

This aggregates every episode of the Criterion Collection's *Closet Picks* YouTube series into one searchable, cross-linked database: every guest, every film they picked, with verbatim quotes, timestamped links to the exact moment on video, and the connections between guests and films. It exists because that information is otherwise trapped across hundreds of hours of unindexed video.

Success, in order: be the **definitive, complete, and accurate** record of the series; be a piece of **craft** worth having made; and be a **delightful place to get lost in** for people who love film. Completeness and correctness come first — a missing or wrong pick is a worse failure than an unpolished layout. This is deliberately not a traffic or SEO play.

## Positioning

The complete, accurate, searchable record of what filmmakers and artists actually pulled from the Criterion Closet — every pick quoted and timestamped back to its source. Nothing else indexes the series this way.

## Conversion & proof

- **Primary action:** find what a specific guest picked (search) or move through a guest or film page. **Secondary fallback:** Random Pick and Most Popular, for undirected discovery when the visitor doesn't arrive with a name in mind.
- **The line a visitor remembers after 10 seconds:** every Closet Picks pick, quoted and timestamped, in one searchable place.
- **Belief ladder:** (1) this actually covers the series — established immediately by the live counts in the stats bar; (2) it's accurate and sourced — proven by verbatim quotes, timestamped video links, and provenance back to Criterion.com and TMDB; (3) it's a pleasure to move through — the reading experience earns continued browsing and trust as *the* reference.
- **Proof on hand:** the live guests / films / picks counts in the stats bar; verbatim quotes paired with timestamped video links; links back to primary sources (Criterion collection pages, TMDB, the YouTube series); and the experience itself — static, fast, ad-free, no tracking.

## Brand Personality

Scholarly, restrained, timeless — this should read like a well-made reference book, not a media site. Print-like typography (Tufte's et-book serif on paper-white), unhurried spacing, quiet confidence. The voice is precise and understated: it frames the material and then gets out of the way, letting the picks and the guests' own words carry the page. No hype, no exclamation, no manufactured urgency. The emotional target is the calm pleasure of a source you trust, plus the small thrill of one pick leading you to the next.

## Anti-references

- **IMDb-style clutter** — dense, ad-laden data dumps with weak hierarchy and everything competing for attention.
- **Generic AI/SaaS landing pages** — gradient heroes, identical feature-card grids, tiny tracked-uppercase eyebrows above every section, the 2026 AI-slop look.
- **Social feeds** — infinite scroll, engagement bait, algorithmic framing.

## Design Principles

- **Data integrity is the product.** Completeness and accuracy come before visual polish; the pipeline's obsession with getting every pick right is the whole point, and the interface should never paper over gaps or overstate what's there.
- **Let the source speak.** Verbatim quotes, exact timestamps, and links to primary sources do the persuading. The site frames the material; it doesn't editorialize on top of it.
- **Read like a reference book.** Reward calm, unhurried browsing — density and clear hierarchy over decoration, no urgency, no chrome fighting the content.
- **Every screen is content-first.** The guest, the film, and the quote are the interface. Nothing markets a feature or competes with the picks.
- **Respect the reader's attention and access.** Fast, static, ad-free, and navigable by keyboard and screen reader. No dark patterns, no tracking theater.

## Accessibility & Inclusion

The existing code already targets **WCAG 2.1 AA** as the floor, and new work should hold that line: visible `:focus-visible` outlines, a skip link, semantic heading order, screen-reader-only labels, and full honoring of `prefers-reduced-motion`. Body text stays at or above 4.5:1 against the paper-white background (the muted grays are the thing to watch on any new surface). Static-first: guest and film pages are fully readable without JavaScript; search (Pagefind) and the browse/filter controls are progressive enhancements on top of readable content.
