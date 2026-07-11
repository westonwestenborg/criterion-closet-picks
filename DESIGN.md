---
name: Closet Picks
description: A searchable database of every Criterion Closet Pick, quoted and timestamped to its source.
colors:
  paper: "#fffff8"
  white: "#ffffff"
  ink: "#111111"
  ink-quote: "#333333"
  ink-secondary: "#555555"
  ink-muted: "#666666"
  muted-aa: "#767676"
  placeholder: "#767676"
  accent-blue: "#2b5797"
  hairline: "#e0e0d8"
  border-hover: "#aaaaaa"
  link-underline: "#777777"
  tint-fill: "#f0f0ea"
  row-hover: "#f4f4ee"
  highlight: "#fff2a8"
typography:
  display:
    fontFamily: "et-book, Palatino, 'Palatino Linotype', Georgia, serif"
    fontSize: "3rem"
    fontWeight: 400
    lineHeight: 1.15
    letterSpacing: "normal"
  headline:
    fontFamily: "et-book, Palatino, 'Palatino Linotype', Georgia, serif"
    fontSize: "2.2rem"
    fontWeight: 400
    lineHeight: 1.2
    letterSpacing: "normal"
  title:
    fontFamily: "et-book, Palatino, 'Palatino Linotype', Georgia, serif"
    fontSize: "1.4rem"
    fontWeight: 400
    lineHeight: 1.3
    letterSpacing: "normal"
  body:
    fontFamily: "et-book, Palatino, 'Palatino Linotype', Georgia, serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: "2rem"
    letterSpacing: "normal"
  label:
    fontFamily: "et-book, Palatino, Georgia, serif"
    fontSize: "0.85rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "0.05em"
  mono:
    fontFamily: "'Courier New', Courier, monospace"
    fontSize: "0.85em"
    fontWeight: 400
    lineHeight: 1.2
    letterSpacing: "normal"
  # Editorial scale — a content site with stats, ranks, quotes, captions, and
  # spine numbers legitimately spans more steps than a minimal app. These are
  # the real, in-use sizes between the named roles above.
  root:
    fontSize: "15px"
    lineHeight: "2rem"
  stat:
    fontSize: "2rem"
    lineHeight: 1.2
  rank:
    fontSize: "1.8rem"
    lineHeight: 1
  section:
    fontSize: "1.5rem"
    lineHeight: 1.3
  subtitle:
    fontSize: "1.2rem"
    lineHeight: 1.2
  lead:
    fontSize: "1.1rem"
    lineHeight: 1.3
  detail:
    fontSize: "1.05rem"
    lineHeight: 1.5
  quote:
    fontSize: "0.95rem"
    lineHeight: "1.8rem"
  small:
    fontSize: "0.9rem"
    lineHeight: 1.5
  caption:
    fontSize: "0.8rem"
    lineHeight: 1.4
  fine:
    fontSize: "0.75rem"
    lineHeight: 1.4
rounded:
  none: "0"
  xs: "2px"
  full: "50%"
spacing:
  xs: "0.5rem"
  sm: "0.75rem"
  md: "1rem"
  lg: "1.5rem"
  xl: "2rem"
  xxl: "2.5rem"
components:
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.75rem 2rem"
  button-primary-hover:
    backgroundColor: "{colors.ink-quote}"
    textColor: "{colors.paper}"
  filter-chip:
    textColor: "{colors.ink-secondary}"
    typography: "{typography.label}"
    rounded: "{rounded.none}"
    padding: "0.3rem 0.8rem"
  filter-chip-active:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
  card:
    backgroundColor: "{colors.paper}"
    rounded: "{rounded.none}"
    padding: "{spacing.lg}"
  input-search:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.none}"
    padding: "0.75rem 1rem"
  badge:
    backgroundColor: "{colors.accent-blue}"
    textColor: "{colors.white}"
    rounded: "{rounded.xs}"
    padding: "0.15rem 0.6rem"
---

# Design System: Closet Picks

## 1. Overview

**Creative North Star: "The Criterion Booklet"**

This system is the printed essay booklet that ships inside a Criterion release, rebuilt for the web. It is set, not styled — the way a good booklet is typeset around the film it accompanies and never competes with it. The interface's job is to disappear behind the material: the guest, the film, and the guest's own verbatim words. Everything here defers to that content. When in doubt, the design does less.

The surface is a single sheet of warm paper (`#fffff8`) with near-black ink (`#111`), set almost entirely in one serif family — Tufte's *et-book*. There is no chrome, no card-grid scaffolding, no color used for decoration. Structure is drawn with one hairline rule (`#e0e0d8`) and the occasional pale tint fill (`#f0f0ea`); depth is never faked with shadow. The one chromatic note, a muted steel blue (`#2b5797`), is rationed to focus rings and a single solid badge — it reads as an editor's mark, not a brand color. Density is unhurried: generous line-height, wide margins, room to read.

This explicitly rejects the media-site reflexes named in PRODUCT.md. It is not IMDb — no ad-laden data dump, no weak hierarchy with everything competing at once. It is not a generic AI/SaaS page — no gradient hero, no identical feature-card grid, no tracked-uppercase eyebrow over every section. It is not a social feed — no infinite scroll, no engagement bait, no algorithmic framing. A visitor should feel they are reading a trustworthy reference, and occasionally following one pick to the next.

**Key Characteristics:**
- One paper background, one ink, one serif; color is functional, never decorative.
- Flat by construction — hairline rules and tint fills instead of shadows or radii.
- Editorial density: serif body, 2rem line-height, ~65–75ch measure, small-caps labels.
- Content-first: the quote and the guest are the interface; the frame recedes.

## 2. Colors

A warm-paper monochrome carrying a single rationed accent — the palette of print, not of a screen UI.

### Primary
- **Editor's Blue** (`#2b5797`): the only chromatic color in the system. Reserved for `:focus-visible` outlines (3px) and the single solid `.badge`. It is never used for body links, backgrounds, or decoration. Its rarity is the entire point.

### Neutral
- **Booklet Paper** (`#fffff8`): the body, card, and input background. A warm off-white — the system's only surface. Pure white is never used as a surface.
- **Ink** (`#111111`): primary text, the solid primary button, the active filter chip, and the spine-number placeholder block. The workhorse foreground.
- **Quote Ink** (`#333333`): blockquote body text and the primary-button hover state — a half-step lift off pure ink for long-form quoted passages.
- **Secondary Ink** (`#555555`): supporting metadata — professions, pick counts, detail links, box-set film lists.
- **Muted Ink** (`#666666`): the quietest legible text — captions, footer, status lines, stat labels.
- **Placeholder Ink** (`#999999`): input placeholder text only. The floor of the ramp; never used for real content.
- **Hairline** (`#e0e0d8`): every border, divider, and rule in the system — card edges, the stats bar, section separators, input strokes. Warm gray, tuned to the paper.
- **Tint Fill** (`#f0f0ea`): the subtle-badge and box-set-tag background — a barely-there warm fill for secondary labels that shouldn't earn a border.

### Tertiary
- **Highlight** (`#fff2a8`): pale-yellow search-result `<mark>` background — the one place text is highlighted, echoing a reader's pencil.

### Named Rules
**The Rare Blue Rule.** `#2b5797` is a functional accent, never a decorative one. It appears only on focus rings and the single solid badge, on well under 10% of any screen. If blue is being used to "add color," delete it.

**The Paper Rule.** Every surface is Booklet Paper (`#fffff8`). Pure white (`#ffffff`) is permitted only as the badge's text color, never as a background. No section, card, or panel ever sits on a whiter or grayer plane than the page itself.

## 3. Typography

**Display Font:** et-book (fallback: Palatino, Palatino Linotype, Georgia, serif)
**Body Font:** et-book (same family throughout)
**Label/Mono Font:** Courier New (spine numbers only)

**Character:** One serif does nearly all the work. *et-book* — Edward Tufte's book face — gives the whole site the register of a printed page: old-style figures, a true italic, warmth without decoration. The only companion is Courier, used exclusively for Criterion spine numbers, where a monospace catalog-number feel is correct. Headings are set at weight 400, not bold: size and space create hierarchy, not heaviness.

### Hierarchy
- **Display** (400, `3rem`/`2.2rem` mobile, line-height 1.15): page titles (`h1`). Set with `text-wrap: balance`.
- **Headline** (400, `2.2rem`, line-height 1.2): section headings (`h2`).
- **Title** (400, `1.4rem`, line-height 1.3): sub-headings and card titles (`h3`); also the italic site title.
- **Body** (400, `1rem` on a 15px root, line-height `2rem`): all prose. Measure capped at ~65–75ch on detail pages (`max-width: 650px`).
- **Label** (400, `0.85rem`, letter-spacing `0.05em`, small-caps): navigation, filter chips, stat labels, buttons, eyebrows. Set with `font-variant: small-caps`.
- **Mono** (400, `0.85em`, Courier): spine numbers only.

### Named Rules
**The One-Family Rule.** *et-book* carries display through body. The only other typeface in the system is Courier, and only for spine numbers. Never introduce a second display or sans face.

**The Small-Caps Label Rule.** Labels, nav, and eyebrows are set in small-caps at `0.05em` tracking — never in tracked ALL-CAPS. The small-cap is the booklet's voice; the tracked uppercase kicker is the AI-slop tell this system rejects.

**The Quiet-Heading Rule.** Headings are weight 400. Hierarchy comes from scale and whitespace, never from bold weight or color.

## 4. Elevation

This system has **no shadows**. There is no `box-shadow` anywhere in the codebase, and none should be added. Depth and grouping are conveyed entirely by (1) a single 1px Hairline (`#e0e0d8`) border or rule, and (2) the pale Tint Fill (`#f0f0ea`) for secondary labels. The page is one flat sheet of paper; elements are distinguished by line and space, exactly as they would be in print.

### Named Rules
**The Flat-Paper Rule.** No shadows, no blur, no glass, ever. If an element needs to feel separated, give it a 1px hairline or more whitespace — not elevation.

**The Border-Reacts Rule.** State is expressed by shifting a border's color, never by adding a shadow or lifting the element. A card's border moves `#e0e0d8 → #aaa` on hover and `→ #2b5797` on focus-within; inputs move `#e0e0d8 → #666` on focus; chips move their border and text toward ink. The geometry never moves.

## 5. Components

### Buttons
- **Shape:** sharp corners (`0` radius). Never rounded.
- **Primary:** solid Ink background (`#111`), Paper text (`#fffff8`), small-caps label type, `0.75rem 2rem` padding. Used for the Random Pick action and the LLM-export download.
- **Hover / Focus:** background lifts to Quote Ink (`#333`) over `0.15s ease`; focus shows the 3px Editor's Blue outline with `3px` offset.
- **No secondary button variant exists** — secondary actions are text links or filter chips, not a second button style.

### Chips (filter / sort controls)
- **Style:** transparent background, 1px Hairline border, Secondary Ink text, small-caps at `0.03em`, `0.3rem 0.8rem` padding, sharp corners.
- **State:** unselected reacts on hover (border `#666`, text `#111`). Selected/active inverts to solid Ink background with Paper text — the same ink-fill logic as the primary button, so "active" always reads as "filled with ink."

### Cards / Containers
- **Corner Style:** sharp (`0` radius).
- **Background:** Booklet Paper (`#fffff8`) — identical to the page; the border alone defines the card.
- **Shadow Strategy:** none (see Elevation).
- **Border:** 1px Hairline (`#e0e0d8`), reacting to state per the Border-Reacts Rule (`#aaa` hover, `#2b5797` focus-within).
- **Internal Padding:** `1.5rem` (`{spacing.lg}`).
- Guest and film cards place a circular photo/poster (`50%` radius on avatars) beside a Title + small-caps profession + muted pick count. Cards are used sparingly, for genuine grid affordances (recent guests, guest/film listings), never as generic content scaffolding.

### Inputs / Fields
- **Style:** full-width, 1px Hairline stroke, Paper background, Ink text, serif body type, `0.75rem 1rem` padding, sharp corners.
- **Focus:** border shifts to `#666` (plus the global 3px blue focus ring); no glow, no shadow.
- **Placeholder:** Placeholder Ink (`#999`), italic.
- The search field is the site's signature input — it sits high on the home page and drives the Pagefind results list.

### Navigation
- **Style:** a single top header row — italic site title on the left, small-caps text links separated by hairline `|` glyphs on the right.
- **States:** links are Ink with no underline at rest; hover adds a 1px `#777` underline. On mobile (≤768px) the header stacks and the nav wraps.

### Blockquote (signature component)
- The system's defining element: guest quotes set as a `blockquote` with a 3px Hairline rule on the left edge, `1.5rem` left padding, Quote Ink text at `0.95rem`/`1.8rem` line-height, followed by a muted-ink attribution line. When a timestamp exists, the quote becomes a link whose underline animates on hover. This left rule is a deliberate typographic convention for quoted matter — it is the one place a >1px left border is correct (see Don'ts).

### Stats bar (proof component)
- Home-page guests / films / picks counts: a flex row bounded top and bottom by hairline rules, each stat a large (`2rem`) serif number over a small-caps muted label. This is the booklet's frontispiece — the breadth proof, stated plainly, once.

## 6. Do's and Don'ts

### Do:
- **Do** set everything on Booklet Paper (`#fffff8`) in *et-book*; reach for a second typeface only for spine numbers (Courier).
- **Do** draw structure with a single 1px Hairline (`#e0e0d8`) and whitespace. Convey state by shifting that border's color, never by adding elevation.
- **Do** keep Editor's Blue (`#2b5797`) rationed to focus rings and the one solid badge — under 10% of any screen.
- **Do** keep headings at weight 400; build hierarchy from scale and space.
- **Do** hold body text at ≥4.5:1 on paper. Watch the muted grays (`#666`/`#999`) on any new small-text surface — they are the contrast risk in this palette.
- **Do** keep the measure at ~65–75ch (`max-width: 650px` on detail pages) and honor `prefers-reduced-motion` on every transition.
- **Do** set labels, nav, and eyebrows in small-caps at `0.05em`.

### Don't:
- **Don't** build an **IMDb-style clutter** page — no ad-laden data dumps, no weak hierarchy with everything competing at once.
- **Don't** import the **generic AI/SaaS** look — no gradient hero, no identical feature-card grid, no tiny tracked-uppercase eyebrow over every section, none of the 2026 AI-slop scaffold.
- **Don't** frame anything as a **social feed** — no infinite scroll, no engagement bait, no algorithmic surfacing.
- **Don't** add `box-shadow`, `backdrop-filter`, glassmorphism, or blur. This system is flat, always.
- **Don't** use gradient text (`background-clip: text`), rounded buttons/cards, or a second accent color.
- **Don't** use a `border-left`/`border-right` >1px as a colored accent stripe on cards, list items, callouts, or alerts. The *only* sanctioned thick left rule is the editorial `blockquote` (see Components) — it is quoted matter, not a decorative stripe.
- **Don't** set labels in tracked ALL-CAPS; small-caps is the house voice.
- **Don't** introduce pure white (`#ffffff`) as a surface, or a whiter/grayer panel than the page.
