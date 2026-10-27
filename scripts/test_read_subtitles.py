#!/usr/bin/env python3
"""Fixture tests for the model-free parts of scripts.read_subtitles."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.read_subtitles import fix_run_together, looks_untranslated, seconds, to_segments


class TimesTest(unittest.TestCase):
    def test_minutes_seconds_strings_parse(self):
        self.assertEqual(seconds("4:01"), 241.0)
        self.assertEqual(seconds("1:02:03"), 3723.0)
        self.assertEqual(seconds(75.5), 75.5)

    def test_run_together_times_are_undone(self):
        # Claude Lelouch's 259-second video came back ending at "401" for 4:01.
        self.assertEqual(fix_run_together([5, 59, 101, 159, 401]), [5, 59, 61, 119, 241])

    def test_real_seconds_are_left_alone(self):
        # Any time whose last two digits are 60-99 proves these are seconds.
        self.assertEqual(fix_run_together([5, 75, 140, 230]), [5, 75, 140, 230])

    def test_segments_get_durations_in_time_order(self):
        segs = to_segments([{"time": "0:10", "text": "second"}, {"time": "0:02", "text": "first"},
                            {"time": "0:20", "text": ""}])
        self.assertEqual([s["text"] for s in segs], ["first", "second"])
        self.assertEqual(segs[0]["duration"], 8.0)


class LanguageTest(unittest.TestCase):
    def test_untranslated_quotes_are_caught(self):
        self.assertTrue(looks_untranslated("On va commencer par Fritz Lang, c'est M le maudit. Alors pour moi Lang"))
        self.assertTrue(looks_untranslated("Pickpocket è stato un mio amore di gioventù, sempre molto ammirato"))
        self.assertTrue(looks_untranslated("الان گشتم و دزد دوچرخه رو اینجا دیدم و میخوام اینو بردارم"))

    def test_english_naming_foreign_titles_is_not(self):
        self.assertFalse(looks_untranslated(
            "Le silence de la mer, another director I deeply respect. It's Jean-Pierre Melville's first film."))


if __name__ == "__main__":
    unittest.main()
