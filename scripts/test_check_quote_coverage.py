#!/usr/bin/env python3
"""Fixture tests for scripts.check_quote_coverage."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.check_quote_coverage import check_visit, dropped_run, held_by_transcript, words

# A miniature closet walk: two picks, with a story about the first one in
# between that an extraction can drop (as it dropped Kim Cattrall's story about
# her mother on Heaven Can Wait).
SEGMENTS = [
    {"start": 0, "text": "Heaven Can Wait anything by Lubitsch just brilliant"},
    {"start": 5, "text": "but it was kind of a time for me to be with my mom"},
    {"start": 9, "text": "she was an usherette in a theater in Liverpool selling ice creams"},
    {"start": 14, "text": "Fanny and Alexander this one I watch every Christmas with my family"},
]
HEAVEN = "Heaven Can Wait. Anything by Lubitsch, just brilliant."
STORY = ("But it was kind of a time for me to be with my mom. She was an usherette "
         "in a theater in Liverpool, selling ice creams.")
FANNY = "Fanny and Alexander. This one I watch every Christmas with my family."


def picks(*quotes):
    titles = ["Heaven Can Wait", "Fanny and Alexander"]
    return [{"film_title": t, "quote": q} for t, q in zip(titles, quotes)]


class CheckVisitTest(unittest.TestCase):
    def test_reports_a_dropped_story_between_two_quotes(self):
        lines = check_visit(SEGMENTS, picks(HEAVEN, FANNY), min_words=10)
        self.assertEqual(len(lines), 1)
        self.assertIn("between Heaven Can Wait and Fanny and Alexander", lines[0])
        self.assertIn("usherette", lines[0])

    def test_reports_nothing_when_the_quotes_cover_the_transcript(self):
        lines = check_visit(SEGMENTS, picks(f"{HEAVEN} {STORY}", FANNY), min_words=10)
        self.assertEqual(lines, [])

    def test_reports_words_a_quote_skips_inside_itself(self):
        # The quote keeps its opening and its last line but drops the middle.
        skipping = f"{HEAVEN} She was an usherette in a theater in Liverpool, selling ice creams."
        lines = check_visit(SEGMENTS, picks(skipping, FANNY), min_words=50)
        self.assertEqual(len(lines), 1)
        self.assertIn("inside Heaven Can Wait", lines[0])
        self.assertIn("with my mom", lines[0])


class DroppedRunTest(unittest.TestCase):
    """extract_quotes.py keeps an existing quote when a re-run would lose words."""

    TRANSCRIPT = [w for seg in SEGMENTS for w in words(seg["text"])]

    def test_a_shorter_new_quote_loses_the_story(self):
        lost = dropped_run(self.TRANSCRIPT, f"{HEAVEN} {STORY}", [HEAVEN, FANNY])
        self.assertIn("usherette", lost)

    def test_words_that_moved_to_another_pick_are_not_lost(self):
        # The story is now on Fanny and Alexander's quote: a reattribution, so
        # the re-run may replace Heaven Can Wait's quote.
        self.assertEqual(
            dropped_run(self.TRANSCRIPT, f"{HEAVEN} {STORY}", [HEAVEN, f"{STORY} {FANNY}"]), ""
        )

    def test_a_quote_from_another_video_is_not_protected(self):
        # Words that are not in this transcript (the guest had the wrong video)
        # align to nothing, so the correction goes through.
        old = "Night of the Hunter is the scariest film I saw as a child, and I still dream of it."
        self.assertEqual(dropped_run(self.TRANSCRIPT, old, [HEAVEN, FANNY]), "")

    def test_a_two_word_quote_counts_as_held(self):
        self.assertTrue(held_by_transcript(self.TRANSCRIPT, "Lubitsch, just brilliant."))
        self.assertFalse(held_by_transcript(self.TRANSCRIPT, "Tony Richardson."))


if __name__ == "__main__":
    unittest.main()
