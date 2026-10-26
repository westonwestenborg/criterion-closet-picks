#!/usr/bin/env python3
"""Fixture tests for the model-free parts of scripts.fill_quote_gaps."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.check_quote_coverage import words
from scripts.fill_quote_gaps import as_pick, locate, parse_object, splice

TRANSCRIPT = words(
    "Heaven Can Wait anything by Lubitsch just brilliant but it was a time for me to be with "
    "my mom she was an usherette in Liverpool Fanny and Alexander I watch every Christmas"
)
STORY_START = TRANSCRIPT.index("but")
STORY_END = TRANSCRIPT.index("fanny")


class LocateTest(unittest.TestCase):
    def test_padding_from_the_quote_is_cut_off(self):
        # Models pad with words the quote already has (Sara Dosa's Varda sentence).
        text = "Just brilliant. But it was a time for me to be with my mom. She was an usherette in Liverpool."
        cut, lo, hi = locate(text, TRANSCRIPT, STORY_START, STORY_END)
        self.assertEqual(cut, "But it was a time for me to be with my mom. She was an usherette in Liverpool.")
        self.assertEqual((lo, hi), (STORY_START, STORY_END))

    def test_part_of_a_stretch_is_accepted(self):
        cut, lo, hi = locate("She was an usherette in Liverpool.", TRANSCRIPT, STORY_START, STORY_END)
        self.assertEqual(cut, "She was an usherette in Liverpool.")
        self.assertEqual(TRANSCRIPT[lo], "she")

    def test_words_the_guest_never_said_are_refused(self):
        self.assertIsNone(locate("She hated every minute of it.", TRANSCRIPT, STORY_START, STORY_END))


class SpliceTest(unittest.TestCase):
    def test_a_later_passage_is_appended(self):
        quote, in_front = splice("Heaven Can Wait. Anything by Lubitsch, just brilliant.", TRANSCRIPT,
                                 STORY_START, STORY_END, "But it was a time for me to be with my mom.")
        self.assertEqual(quote, "Heaven Can Wait. Anything by Lubitsch, just brilliant. But it was a time for me to be with my mom.")
        self.assertFalse(in_front)

    def test_an_earlier_passage_goes_in_front(self):
        quote, in_front = splice("Anything by Lubitsch, just brilliant.", TRANSCRIPT, 0, 3, "Heaven Can Wait.")
        self.assertEqual(quote, "Heaven Can Wait. Anything by Lubitsch, just brilliant.")
        self.assertTrue(in_front)


class ParsingTest(unittest.TestCase):
    def test_fenced_and_listed_replies_parse(self):
        self.assertEqual(parse_object('```json\n{"pick": 2}\n```'), {"pick": 2})
        self.assertEqual(parse_object('[{"pick": 3}]'), {"pick": 3})

    def test_out_of_range_pick_counts_as_none(self):
        self.assertEqual(as_pick({"pick": 9}, 4), 0)
        self.assertEqual(as_pick({"pick": "2"}, 4), 2)
        self.assertEqual(as_pick({}, 4), 0)


if __name__ == "__main__":
    unittest.main()
