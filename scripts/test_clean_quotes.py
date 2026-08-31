#!/usr/bin/env python3
"""Fixture tests for scripts.clean_quotes title capitalization."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.clean_quotes import fix_film_titles

# A catalog holding the two kinds of single-word title that matter here: one that
# is an ordinary English word ("House", "Mother", "Performance") and one that is
# not ("Godzilla"), plus a multi-word title.
CATALOG_TITLE_MAP = {
    "house": "House",
    "mother": "Mother",
    "performance": "Performance",
    "godzilla": "Godzilla",
    "do the right thing": "Do the Right Thing",
}


class FixFilmTitlesTest(unittest.TestCase):
    def test_ordinary_word_is_left_alone_when_it_is_not_the_guests_pick(self):
        # Criterion's catalog holds a film called House, so matching the title
        # anywhere used to rewrite the common noun: "at that friend's House".
        text = "I went to have Thanksgiving at that friend's house"

        self.assertEqual(fix_film_titles(text, CATALOG_TITLE_MAP, "Blue Velvet"), text)

    def test_ordinary_word_is_left_alone_when_there_is_no_own_title(self):
        text = "I grew up learning my craft watching my mother on the stage"

        self.assertEqual(fix_film_titles(text, CATALOG_TITLE_MAP, None), text)

    def test_ordinary_word_is_still_fixed_when_it_is_the_guests_own_pick(self):
        # The guest naming their own pick is the one case where the match is
        # reliably the film and not the noun.
        text = "house. This is one of the great haunted house movies"

        self.assertEqual(
            fix_film_titles(text, CATALOG_TITLE_MAP, "House"),
            "House. This is one of the great haunted House movies",
        )

    def test_multi_word_title_is_fixed_regardless_of_own_title(self):
        text = "this and do the right thing came out the same year"

        self.assertEqual(
            fix_film_titles(text, CATALOG_TITLE_MAP, "Sex, Lies, and Videotape"),
            "this and Do the Right Thing came out the same year",
        )

    def test_single_word_title_that_is_not_an_english_word_is_still_fixed(self):
        text = "I love godzilla so much"

        self.assertEqual(
            fix_film_titles(text, CATALOG_TITLE_MAP, "Blue Velvet"),
            "I love Godzilla so much",
        )

    def test_own_title_match_is_substring_tolerant(self):
        # picks carry the canonical catalog title, which can be longer than the
        # bare word ("Mother" inside "Mother and Son"), so the own-title test has
        # to be a containment check rather than equality.
        text = "mother is the one I keep coming back to"

        self.assertEqual(
            fix_film_titles(text, CATALOG_TITLE_MAP, "Mother and Son"),
            "Mother is the one I keep coming back to",
        )


if __name__ == "__main__":
    unittest.main()
