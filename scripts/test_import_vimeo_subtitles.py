#!/usr/bin/env python3
"""Fixture tests for scripts.import_vimeo_subtitles."""

import tempfile
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.import_vimeo_subtitles import (
    build_transcript,
    import_vimeo_subtitle,
    parse_timestamp,
    parse_vtt,
)


SAMPLE_VTT = """WEBVTT

1
00:00:02.625 --> 00:00:03.845
Hello <i>there</i>.

2
00:01:04.500 --> 00:01:06.000 align:start position:0%
This &amp; that
on two lines.
"""


class ImportVimeoSubtitlesTest(unittest.TestCase):
    def test_parse_timestamp(self):
        self.assertEqual(parse_timestamp("00:01:04.500"), 64.5)
        self.assertEqual(parse_timestamp("01:02:03.250"), 3723.25)

    def test_parse_vtt(self):
        segments = parse_vtt(SAMPLE_VTT)

        self.assertEqual(
            segments,
            [
                {"text": "Hello there.", "start": 2.625, "duration": 1.22},
                {"text": "This & that on two lines.", "start": 64.5, "duration": 1.5},
            ],
        )

    def test_build_transcript_requires_vimeo_id(self):
        with self.assertRaisesRegex(ValueError, "no vimeo_video_id"):
            build_transcript(guest={"slug": "guest", "name": "Guest"}, vtt_text=SAMPLE_VTT)

    def test_import_writes_guest_transcript(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            vtt_path = tmp_path / "guest.vtt"
            vtt_path.write_text(SAMPLE_VTT, encoding="utf-8")
            output_dir = tmp_path / "transcripts"

            with patch(
                "scripts.import_vimeo_subtitles.load_json",
                return_value=[
                    {
                        "slug": "guest",
                        "name": "Guest",
                        "vimeo_video_id": "123",
                    }
                ],
            ):
                result = import_vimeo_subtitle(
                    guest_slug="guest",
                    vtt_path=vtt_path,
                    output_dir=output_dir,
                )

            self.assertEqual(result["segments"], 2)
            self.assertTrue((output_dir / "123.json").exists())

    def test_import_refuses_to_overwrite_without_force(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            vtt_path = tmp_path / "guest.vtt"
            vtt_path.write_text(SAMPLE_VTT, encoding="utf-8")
            output_dir = tmp_path / "transcripts"
            output_dir.mkdir()
            (output_dir / "123.json").write_text("{}", encoding="utf-8")

            with patch(
                "scripts.import_vimeo_subtitles.load_json",
                return_value=[
                    {
                        "slug": "guest",
                        "name": "Guest",
                        "vimeo_video_id": "123",
                    }
                ],
            ):
                with self.assertRaises(FileExistsError):
                    import_vimeo_subtitle(
                        guest_slug="guest",
                        vtt_path=vtt_path,
                        output_dir=output_dir,
                    )


if __name__ == "__main__":
    unittest.main()
