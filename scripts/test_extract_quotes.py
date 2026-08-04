#!/usr/bin/env python3
"""Fixture tests for scripts.extract_quotes helpers."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.extract_quotes import GeminiModel, pick_index_key


class FakeModels:
    def __init__(self):
        self.calls = []

    def generate_content(self, *, model, contents, config):
        self.calls.append({"model": model, "contents": contents, "config": config})
        return object()


class FakeFiles:
    def __init__(self):
        self.uploaded = []

    def upload(self, *, file):
        self.uploaded.append(file)
        return f"uploaded:{file}"


class FakeClient:
    def __init__(self):
        self.models = FakeModels()
        self.files = FakeFiles()


class GeminiModelTest(unittest.TestCase):
    """The adapter binds model name and config, which the new SDK takes per call."""

    def test_generate_content_passes_bound_model_and_config(self):
        client = FakeClient()
        model = GeminiModel(client, "gemini-3-flash-preview", {"temperature": 0.1})

        model.generate_content("a prompt")

        self.assertEqual(len(client.models.calls), 1)
        call = client.models.calls[0]
        self.assertEqual(call["model"], "gemini-3-flash-preview")
        self.assertEqual(call["contents"], "a prompt")
        self.assertEqual(call["config"], {"temperature": 0.1})

    def test_generate_content_accepts_multipart_contents(self):
        # The audio fallback passes [prompt, uploaded_file].
        client = FakeClient()
        model = GeminiModel(client, "m", {})

        model.generate_content(["prompt", "file-handle"])

        self.assertEqual(client.models.calls[0]["contents"], ["prompt", "file-handle"])

    def test_upload_file_uses_keyword_only_file_argument(self):
        client = FakeClient()
        model = GeminiModel(client, "m", {})

        result = model.upload_file("/tmp/audio.mp3")

        self.assertEqual(client.files.uploaded, ["/tmp/audio.mp3"])
        self.assertEqual(result, "uploaded:/tmp/audio.mp3")


class ExtractQuotesTest(unittest.TestCase):
    def test_pick_index_key_keeps_duplicate_titles_distinct(self):
        base = {
            "guest_slug": "guillermo-del-toro",
            "film_title": "Roma",
            "visit_index": 2,
            "source": "criterion",
        }
        roma_1972 = {
            **base,
            "film_id": "roma-1972",
            "catalog_spine": 848,
            "pick_order": 6,
            "criterion_film_url": "https://www.criterion.com/films/28039-roma",
        }
        roma_2018 = {
            **base,
            "film_id": "roma-2018",
            "catalog_spine": 1014,
            "pick_order": 10,
            "criterion_film_url": "https://www.criterion.com/films/30124-roma",
        }

        self.assertNotEqual(pick_index_key(roma_1972), pick_index_key(roma_2018))

    def test_pick_index_key_treats_order_drift_as_same_pick(self):
        existing = {
            "guest_slug": "wim-wenders",
            "film_id": "the-complete-jacques-tati",
            "film_title": "The Complete Jacques Tati",
            "visit_index": 1,
            "pick_order": 10,
        }
        raw = {
            **existing,
            "pick_order": 11,
        }

        self.assertEqual(pick_index_key(existing), pick_index_key(raw))


if __name__ == "__main__":
    unittest.main()
