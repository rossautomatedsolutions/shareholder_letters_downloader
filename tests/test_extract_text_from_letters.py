import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.extract_text_from_letters import run


class ExtractTextFromLettersTests(unittest.TestCase):
    def test_run_writes_text_and_json_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir) / "output"
            output_root = Path(tmpdir) / "output_text"
            pdf_dir = input_root / "acme_inc" / "shareholder_letter"
            pdf_dir.mkdir(parents=True)
            (pdf_dir / "2022.pdf").write_bytes(b"%PDF-1.4\n")

            with patch(
                "scripts.extract_text_from_letters.extract_pdf_text",
                return_value="Hello shareholders from Acme.",
            ):
                processed = run(
                    input_root=input_root,
                    output_root=output_root,
                    document_type="shareholder_letter",
                )

            self.assertEqual(processed, 1)
            self.assertEqual(
                (output_root / "acme_inc" / "2022.txt").read_text(encoding="utf-8"),
                "Hello shareholders from Acme.",
            )

            metadata = json.loads((output_root / "acme_inc" / "2022.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata["company_id"], "acme_inc")
            self.assertEqual(metadata["year"], "2022")
            self.assertEqual(metadata["word_count"], 4)
            self.assertEqual(metadata["char_count"], len("Hello shareholders from Acme."))

    def test_run_skips_already_processed_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_root = Path(tmpdir) / "output"
            output_root = Path(tmpdir) / "output_text"
            pdf_dir = input_root / "acme_inc" / "shareholder_letter"
            pdf_dir.mkdir(parents=True)
            (pdf_dir / "2022.pdf").write_bytes(b"%PDF-1.4\n")

            company_output_dir = output_root / "acme_inc"
            company_output_dir.mkdir(parents=True)
            (company_output_dir / "2022.txt").write_text("existing text", encoding="utf-8")
            (company_output_dir / "2022.json").write_text(
                json.dumps({"company_id": "acme_inc", "year": "2022"}),
                encoding="utf-8",
            )

            with patch("scripts.extract_text_from_letters.extract_pdf_text") as mock_extract:
                processed = run(
                    input_root=input_root,
                    output_root=output_root,
                    document_type="shareholder_letter",
                )

            self.assertEqual(processed, 0)
            mock_extract.assert_not_called()


if __name__ == "__main__":
    unittest.main()
