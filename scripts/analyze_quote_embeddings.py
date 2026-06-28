#!/usr/bin/env python3
"""Embedding-based semantic clustering for Criterion Closet pick quotes."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import hdbscan
import numpy as np
import pandas as pd
import umap
from openai import OpenAI
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import normalize


DEFAULT_MODEL = "text-embedding-3-small"

STOPWORDS = {
    "criterion",
    "closet",
    "collection",
    "pick",
    "picked",
    "picks",
    "picking",
    "film",
    "films",
    "movie",
    "movies",
    "really",
    "just",
    "like",
    "know",
    "think",
    "thing",
    "things",
    "yeah",
    "yes",
    "okay",
    "well",
    "going",
    "gonna",
    "take",
    "taking",
}

LEXICAL_TAGS = {
    "evangelism/superlative": r"\b(favou?rite|best|greatest|masterpiece|perfect|incredible|amazing|extraordinary|essential|all[- ]time)\b",
    "autobiographical memory": r"\b(first saw|first time|remember|childhood|kid|teen|young|grew up|years ago|when i was|changed my life|formative)\b",
    "love language": r"\b(love|loved|fell in love|adore|heart)\b",
    "craft talk": r"\b(performance|performances|acting|actor|actress|director|directed|filmmaker|cinematography|camera|shot|shots|editing|writing|script|screenplay|lighting|visual|image|images)\b",
    "watchlist confession": r"\b(haven[’']?t seen|never seen|haven[’']?t watched|never watched|need to watch|want to watch|heard so much|on my list|looking forward)\b",
    "rewatch ritual": r"\b(rewatch|watch it again|watched it again|watching it again|seen it.*times|watch.*every|if it[’']?s on|over and over)\b",
    "object/edition culture": r"\b(criterion|blu[- ]?ray|dvd|disc|edition|restoration|restored|commentary|box set|collection|extras)\b",
    "family/community": r"\b(family|mother|father|mom|dad|parents|friend|friends|community|wife|husband|daughter|son|kids|children)\b",
    "music/dance": r"\b(music|musical|score|song|songs|dance|dancing|singer|band)\b",
    "fear/transgression": r"\b(scary|terrifying|horror|disturbing|nightmare|violent|violence|sex|sexy|dangerous|forbidden|transgressive)\b",
    "representation/identity": r"\b(black|queer|gay|lesbian|trans|women|woman|female|native|mexican|asian|japanese|french|italian|community|color|identity)\b",
}


@dataclass(frozen=True)
class Inputs:
    picks: Path
    guests: Path
    catalog: Path


def load_rows(inputs: Inputs, min_chars: int) -> pd.DataFrame:
    picks = json.loads(inputs.picks.read_text())
    guests = {g["slug"]: g for g in json.loads(inputs.guests.read_text())}
    catalog = {c["film_id"]: c for c in json.loads(inputs.catalog.read_text())}

    rows = []
    for index, pick in enumerate(picks):
        quote = (pick.get("quote") or "").strip()
        if len(quote) < min_chars:
            continue
        if pick.get("extraction_confidence") == "none":
            continue

        guest = guests.get(pick["guest_slug"], {})
        film = catalog.get(pick["film_id"], {})
        genres = film.get("genres") or []
        mode_text = build_mode_text(
            quote=quote,
            guest_name=pick.get("guest_name") or guest.get("name") or "",
            film_title=pick.get("catalog_title") or pick.get("film_title") or film.get("title") or "",
            box_set_name=pick.get("box_set_name") or "",
            director=film.get("director") or "",
        )
        rows.append(
            {
                "row_id": len(rows),
                "pick_index": index,
                "guest_slug": pick["guest_slug"],
                "guest_name": pick.get("guest_name") or guest.get("name") or "",
                "profession": guest.get("profession") or "unknown",
                "film_id": pick["film_id"],
                "film_title": pick.get("catalog_title") or pick.get("film_title") or film.get("title") or "",
                "film_year": film.get("year"),
                "director": film.get("director") or "",
                "genres": "; ".join(genres),
                "box_set_name": pick.get("box_set_name") or "",
                "is_box_set": bool(pick.get("is_box_set")),
                "visit_index": pick.get("visit_index"),
                "start_timestamp": pick.get("start_timestamp"),
                "youtube_timestamp_url": pick.get("youtube_timestamp_url") or "",
                "extraction_confidence": pick.get("extraction_confidence") or "",
                "quote_length": len(quote),
                "translated_flag": "[Translated]" in quote or "[translated]" in quote,
                "duplicate_quote_hash": hashlib.sha256(
                    re.sub(r"\s+", " ", quote.lower()).strip().encode("utf-8")
                ).hexdigest()[:16],
                "quote": quote,
                "mode_text": mode_text,
            }
        )

    return pd.DataFrame(rows)


def mask_phrase(text: str, phrase: str, replacement: str) -> str:
    phrase = (phrase or "").strip()
    if len(phrase) < 3:
        return text
    pattern = re.escape(phrase)
    return re.sub(pattern, replacement, text, flags=re.IGNORECASE)


def build_mode_text(quote: str, guest_name: str, film_title: str, box_set_name: str, director: str) -> str:
    text = quote.replace("[Translated]", " ").replace("[translated]", " ")
    for name, replacement in [
        (guest_name, "[GUEST]"),
        (film_title, "[FILM]"),
        (box_set_name, "[BOX_SET]"),
        (director, "[DIRECTOR]"),
    ]:
        text = mask_phrase(text, name, replacement)
        for part in re.split(r"\s+(?:and|&|/)\s+", name or ""):
            text = mask_phrase(text, part, replacement)
    return re.sub(r"\s+", " ", text).strip()


def embed_texts(
    texts: list[str],
    cache_path: Path,
    model: str,
    batch_size: int,
    sleep_seconds: float,
) -> np.ndarray:
    text_hash = hashlib.sha256("\n".join(texts).encode("utf-8")).hexdigest()
    if cache_path.exists():
        cached = np.load(cache_path, allow_pickle=False)
        if (
            cached["model"].item() == model
            and cached["count"].item() == len(texts)
            and "text_hash" in cached.files
            and cached["text_hash"].item() == text_hash
        ):
            return cached["embeddings"]

    client = OpenAI()
    embeddings: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        response = client.embeddings.create(model=model, input=batch)
        embeddings.extend([item.embedding for item in response.data])
        print(f"embedded {min(start + batch_size, len(texts))}/{len(texts)}")
        if sleep_seconds:
            time.sleep(sleep_seconds)

    arr = np.array(embeddings, dtype=np.float32)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        cache_path,
        embeddings=arr,
        model=np.array(model),
        count=np.array(len(texts)),
        text_hash=np.array(text_hash),
    )
    return arr


def choose_k(vectors: np.ndarray, low: int = 7, high: int = 16) -> tuple[int, pd.DataFrame]:
    rows = []
    for k in range(low, high + 1):
        model = KMeans(n_clusters=k, n_init=50, random_state=42)
        labels = model.fit_predict(vectors)
        score = silhouette_score(vectors, labels, metric="cosine")
        rows.append({"k": k, "silhouette_cosine": score})
    scores = pd.DataFrame(rows)
    best = int(scores.sort_values(["silhouette_cosine", "k"], ascending=[False, True]).iloc[0]["k"])
    return best, scores


def top_terms_by_cluster(df: pd.DataFrame, labels: np.ndarray, max_terms: int = 12) -> dict[int, list[str]]:
    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words="english",
        ngram_range=(1, 2),
        min_df=5,
        max_df=0.35,
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z']{2,}\b",
    )
    matrix = vectorizer.fit_transform(df["quote"].tolist())
    terms = np.array(vectorizer.get_feature_names_out())
    cluster_terms: dict[int, list[str]] = {}
    corpus_mean = np.asarray(matrix.mean(axis=0)).ravel() + 1e-9

    for label in sorted(set(labels)):
        mask = labels == label
        cluster_mean = np.asarray(matrix[mask].mean(axis=0)).ravel()
        lift = cluster_mean / corpus_mean
        score = lift * np.sqrt(cluster_mean + 1e-9)
        chosen = []
        for term in terms[np.argsort(score)[::-1]]:
            if term in STOPWORDS:
                continue
            if any(part in STOPWORDS for part in term.split()):
                continue
            chosen.append(term)
            if len(chosen) == max_terms:
                break
        cluster_terms[int(label)] = chosen

    return cluster_terms


def lexical_tag_counts(quotes: pd.Series) -> Counter[str]:
    counts: Counter[str] = Counter()
    for quote in quotes:
        for label, pattern in LEXICAL_TAGS.items():
            if re.search(pattern, quote, flags=re.IGNORECASE):
                counts[label] += 1
    return counts


def lexical_tag_frame(df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    corpus = lexical_tag_counts(df["quote"])
    total = len(df)
    rows = []
    for label in sorted(set(labels)):
        subset = df[labels == label]
        cluster_counts = lexical_tag_counts(subset["quote"])
        for tag in LEXICAL_TAGS:
            count = cluster_counts[tag]
            cluster_share = count / len(subset) if len(subset) else 0
            corpus_share = corpus[tag] / total if total else 0
            lift = cluster_share / corpus_share if corpus_share else math.nan
            rows.append(
                {
                    "cluster": int(label),
                    "tag": tag,
                    "count": int(count),
                    "cluster_share": cluster_share,
                    "corpus_share": corpus_share,
                    "lift": lift,
                }
            )
    return pd.DataFrame(rows)


def nearest_examples(vectors: np.ndarray, labels: np.ndarray, df: pd.DataFrame, label: int, limit: int = 5) -> pd.DataFrame:
    idx = np.flatnonzero(labels == label)
    centroid = vectors[idx].mean(axis=0)
    centroid = centroid / max(np.linalg.norm(centroid), 1e-12)
    sims = vectors[idx] @ centroid
    chosen_idx = idx[np.argsort(sims)[::-1][:limit]]
    cols = ["guest_name", "film_title", "profession", "quote"]
    out = df.iloc[chosen_idx][cols].copy()
    out["similarity_to_centroid"] = sims[np.argsort(sims)[::-1][:limit]]
    out["quote_excerpt"] = out["quote"].str.replace(r"\s+", " ", regex=True).str.slice(0, 140)
    return out.drop(columns=["quote"])


def summarize_clusters(df: pd.DataFrame, vectors: np.ndarray, labels: np.ndarray, terms: dict[int, list[str]]) -> pd.DataFrame:
    rows = []
    total = len(df)
    for label in sorted(set(labels)):
        subset = df[labels == label]
        tags = lexical_tag_counts(subset["quote"])
        top_tags = "; ".join(f"{name} ({count})" for name, count in tags.most_common(4))
        professions = "; ".join(f"{name} ({count})" for name, count in Counter(subset["profession"]).most_common(4))
        genres = Counter()
        for value in subset["genres"]:
            for genre in str(value).split("; "):
                if genre:
                    genres[genre] += 1
        years = pd.to_numeric(subset["film_year"], errors="coerce").dropna()
        rows.append(
            {
                "cluster": int(label),
                "quotes": int(len(subset)),
                "share": len(subset) / total,
                "top_terms": "; ".join(terms[int(label)]),
                "top_lexical_tags": top_tags,
                "professions": professions,
                "top_genres": "; ".join(f"{name} ({count})" for name, count in genres.most_common(5)),
                "median_film_year": int(years.median()) if len(years) else None,
                "box_set_share": float(subset["box_set_name"].astype(bool).mean()),
                "translated_share": float(subset["translated_flag"].mean()),
                "duplicate_quote_hashes": int(subset["duplicate_quote_hash"].nunique()),
                "top_films": "; ".join(f"{name} ({count})" for name, count in Counter(subset["film_title"]).most_common(5)),
            }
        )
    return pd.DataFrame(rows).sort_values("quotes", ascending=False)


def guest_quote_profiles(df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    rows = []
    labeled = df.copy()
    labeled["embedding_cluster"] = labels
    cluster_count = len(set(labels))

    for (guest_slug, guest_name), subset in labeled.groupby(["guest_slug", "guest_name"], sort=True):
        counts = Counter(subset["embedding_cluster"])
        total = len(subset)
        shares = np.array([count / total for count in counts.values()])
        entropy = -float(np.sum(shares * np.log(shares))) / math.log(cluster_count) if cluster_count > 1 else 0.0
        dominant_cluster, dominant_count = counts.most_common(1)[0]
        rows.append(
            {
                "guest_slug": guest_slug,
                "guest_name": guest_name,
                "quotes": int(total),
                "dominant_cluster": int(dominant_cluster),
                "dominant_cluster_quotes": int(dominant_count),
                "dominant_cluster_share": dominant_count / total,
                "quote_mode_entropy": entropy,
                "cluster_mix": "; ".join(
                    f"{int(cluster)}:{int(count)}" for cluster, count in sorted(counts.items())
                ),
            }
        )

    return pd.DataFrame(rows).sort_values(["quotes", "dominant_cluster_share"], ascending=[False, False])


def write_markdown(
    path: Path,
    df: pd.DataFrame,
    scores: pd.DataFrame,
    cluster_summary: pd.DataFrame,
    density_summary: pd.DataFrame,
    examples: dict[int, pd.DataFrame],
    hdbscan_counts: Counter[int],
    validation: pd.DataFrame,
    model: str,
) -> None:
    validation_top = (
        validation.sort_values(["cluster", "lift"], ascending=[True, False])
        .groupby("cluster")
        .head(4)
        .assign(
            cluster_share=lambda x: (x["cluster_share"] * 100).round(1).astype(str) + "%",
            corpus_share=lambda x: (x["corpus_share"] * 100).round(1).astype(str) + "%",
            lift=lambda x: x["lift"].round(2),
        )
    )
    lines = [
        "# Quote Embedding Semantic Analysis",
        "",
        f"Generated from {len(df):,} quotes using OpenAI `{model}` embeddings.",
        "",
        "## Method",
        "",
        "- Embedded entity-masked quote text only; film titles, guest names, directors, genres, and professions were carried as metadata but not included as literal names in the embedding text.",
        "- Normalized embedding vectors and selected a global k-means segmentation by cosine silhouette over k = 7..16.",
        "- Ran HDBSCAN on a 12-dimensional UMAP projection as a density check, not as the primary segmentation.",
        "- Labeled clusters from high-lift TF-IDF terms, lexical-tag enrichment, metadata summaries, and centroid-nearest examples.",
        "",
        "## K Selection",
        "",
        scores.to_markdown(index=False, floatfmt=".4f"),
        "",
        "## Primary Clusters",
        "",
        cluster_summary.assign(share=lambda x: (x["share"] * 100).round(1).astype(str) + "%").to_markdown(index=False),
        "",
        "## HDBSCAN Density Check",
        "",
        "HDBSCAN is useful as a check for tight islands, but it does not force every quote into a topic. Counts below include `-1` as noise/unassigned.",
        "",
        pd.DataFrame(
            [{"label": int(label), "quotes": int(count)} for label, count in sorted(hdbscan_counts.items())]
        ).to_markdown(index=False),
        "",
        "Dense islands with HDBSCAN label `-1` removed:",
        "",
        density_summary.assign(share=lambda x: (x["share"] * 100).round(1).astype(str) + "%").to_markdown(index=False),
        "",
        "## Lexical Theme Validation",
        "",
        "Top lexical-tag enrichments per embedding cluster. Tags are non-exclusive.",
        "",
        validation_top[["cluster", "tag", "count", "cluster_share", "corpus_share", "lift"]].to_markdown(index=False),
        "",
        "## Centroid Examples",
        "",
        "Excerpts are truncated; use them as labels/helpers, not as long quoted source text.",
        "",
    ]

    for cluster_id in cluster_summary["cluster"]:
        lines.append(f"### Cluster {cluster_id}")
        lines.append("")
        lines.append(examples[int(cluster_id)].to_markdown(index=False, floatfmt=".3f"))
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--min-chars", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=96)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--output-dir", type=Path, default=Path("docs/semantic-analysis"))
    parser.add_argument("--force-k", type=int)
    args = parser.parse_args()

    inputs = Inputs(
        picks=Path("data/picks.json"),
        guests=Path("data/guests.json"),
        catalog=Path("data/criterion_catalog.json"),
    )
    df = load_rows(inputs, min_chars=args.min_chars)
    cache_path = args.output_dir / f"quote_embeddings_{args.model.replace('-', '_')}.npz"
    embeddings = embed_texts(df["mode_text"].tolist(), cache_path, args.model, args.batch_size, args.sleep_seconds)
    vectors = normalize(embeddings, norm="l2")

    best_k, k_scores = choose_k(vectors)
    k = args.force_k or best_k
    kmeans = KMeans(n_clusters=k, n_init=100, random_state=42)
    labels = kmeans.fit_predict(vectors)

    cluster_reducer = umap.UMAP(
        n_components=12,
        n_neighbors=30,
        min_dist=0.05,
        metric="cosine",
        random_state=42,
    )
    cluster_coords = cluster_reducer.fit_transform(vectors)
    density_labels = hdbscan.HDBSCAN(
        min_cluster_size=45,
        min_samples=10,
        cluster_selection_method="eom",
        metric="euclidean",
    ).fit_predict(cluster_coords)
    viz_reducer = umap.UMAP(n_components=2, n_neighbors=30, min_dist=0.05, metric="cosine", random_state=42)
    coords = viz_reducer.fit_transform(vectors)

    terms = top_terms_by_cluster(df, labels)
    cluster_summary = summarize_clusters(df, vectors, labels, terms)
    density_mask = density_labels >= 0
    density_terms = top_terms_by_cluster(df[density_mask], density_labels[density_mask])
    density_summary = summarize_clusters(df[density_mask], vectors[density_mask], density_labels[density_mask], density_terms)
    validation = lexical_tag_frame(df, labels)
    examples = {
        int(cluster_id): nearest_examples(vectors, labels, df, int(cluster_id), limit=5)
        for cluster_id in cluster_summary["cluster"]
    }
    example_rows = []
    for cluster_id, frame in examples.items():
        cluster_examples = frame.copy()
        cluster_examples.insert(0, "cluster", cluster_id)
        example_rows.append(cluster_examples)

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    enriched = df.copy()
    enriched["embedding_cluster"] = labels
    enriched["umap_x"] = coords[:, 0]
    enriched["umap_y"] = coords[:, 1]
    enriched["hdbscan_cluster"] = density_labels
    enriched["quote_excerpt"] = enriched["quote"].str.replace(r"\s+", " ", regex=True).str.slice(0, 180)
    enriched.drop(columns=["quote", "mode_text"]).to_csv(out_dir / "quote_embedding_rows.csv", index=False)
    k_scores.to_csv(out_dir / "quote_embedding_k_scores.csv", index=False)
    cluster_summary.to_csv(out_dir / "quote_embedding_cluster_summary.csv", index=False)
    density_summary.to_csv(out_dir / "quote_embedding_hdbscan_cluster_summary.csv", index=False)
    validation.to_csv(out_dir / "quote_embedding_cluster_validation.csv", index=False)
    guest_quote_profiles(df, labels).to_csv(out_dir / "guest_quote_profiles.csv", index=False)
    pd.concat(example_rows, ignore_index=True).to_csv(out_dir / "quote_cluster_examples.csv", index=False)
    write_markdown(
        out_dir / "quote-embedding-semantic-analysis.md",
        df,
        k_scores,
        cluster_summary,
        density_summary,
        examples,
        Counter(density_labels),
        validation,
        args.model,
    )

    print(f"quotes={len(df)} model={args.model} selected_k={k} best_k={best_k}")
    print(f"wrote {out_dir / 'quote-embedding-semantic-analysis.md'}")


if __name__ == "__main__":
    main()
