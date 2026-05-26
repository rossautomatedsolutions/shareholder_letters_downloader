from __future__ import annotations

import pandas as pd
import pytest

from scripts.export_philo_nlp_inputs import OUTPUT_COLUMNS, run, validate_export_schema


def write_text_sidecar(
    text_root,
    company_id: str,
    year: str,
    text: str,
    document_type: str = "shareholder_letter",
) -> None:
    company_dir = text_root / company_id
    company_dir.mkdir(parents=True, exist_ok=True)
    (company_dir / f"{year}.txt").write_text(text, encoding="utf-8")
    (company_dir / f"{year}.json").write_text(
        (
            "{\n"
            f'  "company_id": "{company_id}",\n'
            f'  "year": "{year}",\n'
            f'  "document_type": "{document_type}"\n'
            "}\n"
        ),
        encoding="utf-8",
    )


def test_successful_export(tmp_path) -> None:
    text_root = tmp_path / "output_text"
    features_dir = tmp_path / "features"
    manifest_path = tmp_path / "manifests" / "letters_manifest.cleaned.csv"
    output_path = features_dir / "philo_nlp_input.csv"
    features_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    write_text_sidecar(text_root, "beta_corp", "2023", "Beta 2023 text.", document_type="annual_report")
    write_text_sidecar(text_root, "acme_inc", "2024", "Acme 2024 text.")

    pd.DataFrame(
        [
            {"company_id": "acme_inc", "company_name": "Acme Inc.", "year": "2024", "document_type": "shareholder_letter"},
            {"company_id": "beta_corp", "company_name": "Beta Corp", "year": "2023", "document_type": "annual_report"},
        ]
    ).to_csv(manifest_path, index=False)
    pd.DataFrame(
        [
            {"company_id": "beta_corp", "year": "2023", "sentiment_score": -0.25},
            {"company_id": "acme_inc", "year": "2024", "sentiment_score": 0.75},
        ]
    ).to_csv(features_dir / "sentiment_features.csv", index=False)
    pd.DataFrame(
        [
            {"company_id": "beta_corp", "year": "2023", "sentiment_deviation": 0.5},
            {"company_id": "acme_inc", "year": "2024", "sentiment_deviation": 0.1},
        ]
    ).to_csv(features_dir / "sentiment_stability.csv", index=False)

    result = run(
        text_root=text_root,
        sentiment_features_path=features_dir / "sentiment_features.csv",
        sentiment_stability_path=features_dir / "sentiment_stability.csv",
        manifest_path=manifest_path,
        output_path=output_path,
    )

    assert result.columns.tolist() == OUTPUT_COLUMNS
    assert result.to_dict("records") == [
        {
            "company_id": "acme_inc",
            "company_name": "Acme Inc.",
            "year": "2024",
            "document_type": "shareholder_letter",
            "text": "Acme 2024 text.",
            "sentiment_score": 0.75,
            "sentiment_deviation": 0.1,
        },
        {
            "company_id": "beta_corp",
            "company_name": "Beta Corp",
            "year": "2023",
            "document_type": "annual_report",
            "text": "Beta 2023 text.",
            "sentiment_score": -0.25,
            "sentiment_deviation": 0.5,
        },
    ]

    written = pd.read_csv(output_path)
    assert written.columns.tolist() == OUTPUT_COLUMNS
    assert written["company_id"].tolist() == ["acme_inc", "beta_corp"]


def test_duplicate_detection_raises_clear_error(tmp_path) -> None:
    text_root = tmp_path / "output_text"
    nested_dir = text_root / "acme_inc" / "supplemental"
    nested_dir.mkdir(parents=True, exist_ok=True)
    (text_root / "acme_inc").mkdir(parents=True, exist_ok=True)
    (text_root / "acme_inc" / "2024.txt").write_text("Primary text", encoding="utf-8")
    (nested_dir / "2024.txt").write_text("Duplicate text", encoding="utf-8")

    features_dir = tmp_path / "features"
    features_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=["company_id", "year", "sentiment_score"]).to_csv(
        features_dir / "sentiment_features.csv",
        index=False,
    )
    pd.DataFrame(columns=["company_id", "year", "sentiment_deviation"]).to_csv(
        features_dir / "sentiment_stability.csv",
        index=False,
    )

    with pytest.raises(ValueError, match=r"duplicate company/year rows: acme_inc/2024"):
        run(
            text_root=text_root,
            sentiment_features_path=features_dir / "sentiment_features.csv",
            sentiment_stability_path=features_dir / "sentiment_stability.csv",
            manifest_path=None,
            output_path=features_dir / "philo_nlp_input.csv",
        )


def test_missing_feature_rows_preserve_nan_values(tmp_path) -> None:
    text_root = tmp_path / "output_text"
    features_dir = tmp_path / "features"
    output_path = features_dir / "philo_nlp_input.csv"
    features_dir.mkdir(parents=True, exist_ok=True)

    write_text_sidecar(text_root, "acme_inc", "2024", "Acme text.")
    pd.DataFrame(columns=["company_id", "year", "sentiment_score"]).to_csv(
        features_dir / "sentiment_features.csv",
        index=False,
    )
    pd.DataFrame(columns=["company_id", "year", "sentiment_deviation"]).to_csv(
        features_dir / "sentiment_stability.csv",
        index=False,
    )

    result = run(
        text_root=text_root,
        sentiment_features_path=features_dir / "sentiment_features.csv",
        sentiment_stability_path=features_dir / "sentiment_stability.csv",
        manifest_path=None,
        output_path=output_path,
    )

    assert result.loc[0, "company_name"] == "Acme Inc"
    assert pd.isna(result.loc[0, "sentiment_score"])
    assert pd.isna(result.loc[0, "sentiment_deviation"])


def test_schema_validation_rejects_missing_columns() -> None:
    invalid_frame = pd.DataFrame(
        [
            {
                "company_id": "acme_inc",
                "company_name": "Acme Inc.",
                "year": "2024",
                "document_type": "shareholder_letter",
                "text": "Acme text.",
                "sentiment_score": 0.1,
            }
        ]
    )

    with pytest.raises(ValueError, match="Export schema mismatch"):
        validate_export_schema(invalid_frame)
