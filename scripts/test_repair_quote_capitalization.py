#!/usr/bin/env python3
"""Fixture tests for scripts.repair_quote_capitalization."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.repair_quote_capitalization import repair_quote

AMBIGUOUS = {"dreams": "Dreams"}
TRANSCRIPT = "dreams come true they can happen to you and I had dreams about it"


class RepairQuoteTest(unittest.TestCase):
    def test_mid_sentence_false_capital_is_lowercased(self):
        quote, changed = repair_quote(
            "And I had Dreams about it.", "Punch-Drunk Love", TRANSCRIPT, AMBIGUOUS
        )
        self.assertEqual(quote, "And I had dreams about it.")
        self.assertEqual(changed["Dreams"], 1)

    def test_capital_at_the_start_of_the_quote_is_kept(self):
        quote = "Dreams come true, they can happen to you. Punch-Drunk Love."
        repaired, changed = repair_quote(quote, "Punch-Drunk Love", TRANSCRIPT, AMBIGUOUS)
        self.assertEqual(repaired, quote)
        self.assertFalse(changed)

    def test_capital_after_a_full_stop_is_kept(self):
        quote = "Punch-Drunk Love. Dreams come true."
        repaired, changed = repair_quote(quote, "Punch-Drunk Love", TRANSCRIPT, AMBIGUOUS)
        self.assertEqual(repaired, quote)
        self.assertFalse(changed)


if __name__ == "__main__":
    unittest.main()
