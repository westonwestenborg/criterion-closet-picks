#!/usr/bin/env python3
"""Fixture tests for scripts.clean_quotes title capitalization."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.clean_quotes import add_trailing_ellipsis, clean_quote, deduplicate_words, fix_film_titles

# A catalog holding the two kinds of single-word title that matter here: one that
# is an ordinary English word ("House", "Mother", "Performance") and one that is
# not ("Godzilla"), plus a multi-word title and a multi-word everyday phrase.
CATALOG_TITLE_MAP = {
    "house": "House",
    "mother": "Mother",
    "performance": "Performance",
    "godzilla": "Godzilla",
    "do the right thing": "Do the Right Thing",
    "the most beautiful": "The Most Beautiful",
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

    def test_multi_word_title_that_is_an_everyday_phrase_is_left_alone(self):
        # Kurosawa's The Most Beautiful is in the catalog, and multi-word titles
        # were once assumed safe to substitute anywhere, so 27 quotes read "one
        # of The Most Beautiful films".
        text = "it's one of the most beautiful films ever made"

        self.assertEqual(fix_film_titles(text, CATALOG_TITLE_MAP, "Revanche"), text)

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


class FillerAndPunctuationTest(unittest.TestCase):
    """Gemini 4 Argon keeps the guest's "uh"s; removing one must not leave its commas."""

    def test_filler_between_commas_takes_both_commas(self):
        self.assertEqual(
            clean_quote("that amazing, uh, scenery and the set.", {}),
            "That amazing scenery and the set.",
        )

    def test_filler_after_a_full_stop_takes_its_comma(self):
        self.assertEqual(
            clean_quote("The man, the myth. Uh, I love this movie.", {}),
            "The man, the myth. I love this movie.",
        )

    def test_filler_opening_quoted_speech_keeps_the_capital(self):
        self.assertEqual(
            clean_quote("He said, “Uh, you have to take Love Streams.”", {}),
            "He said, “You have to take Love Streams.”",
        )

    def test_long_quotes_are_not_cut(self):
        # The 500-character cap is gone: it cut 76 quotes mid-word.
        long = "I love this film. " * 60
        self.assertEqual(clean_quote(long, {}), long.strip())

    def test_ellipsis_and_closing_quotes_end_a_sentence(self):
        self.assertEqual(add_trailing_ellipsis("And if you have Crash…"), "And if you have Crash…")
        self.assertEqual(add_trailing_ellipsis("He said, “Yes.”"), "He said, “Yes.”")
        self.assertEqual(add_trailing_ellipsis("He said, “Yes.”..."), "He said, “Yes.”")
        self.assertEqual(add_trailing_ellipsis("and then he"), "and then he...")

    def test_grammatical_repeats_survive(self):
        self.assertEqual(deduplicate_words("I had had enough"), "I had had enough")
        self.assertEqual(deduplicate_words("I think that that is it"), "I think that that is it")
        self.assertEqual(deduplicate_words("the the movie"), "the movie")

    def test_doubled_title_words_survive(self):
        # Collapsing repeats turned 18 guests' "Yi Yi" into "Yi".
        titles = {"yi yi": "Yi Yi", "jo jo dancer, your life is calling": "Jo Jo Dancer, Your Life Is Calling"}
        self.assertEqual(clean_quote("Yi Yi. No explanation needed.", titles), "Yi Yi. No explanation needed.")
        self.assertEqual(clean_quote("the the movie is Yi Yi.", titles), "The movie is Yi Yi.")


if __name__ == "__main__":
    unittest.main()
