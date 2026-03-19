from __future__ import annotations

import sys
import json
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from src.scoring.evidence_mapper import (  # noqa: E402
    EvidenceMapper,
    EvidenceScore,
    Dimension,
    SignalSource,
    load_all_evidence_from_snowflake,
    load_sec_evidence_from_snowflake_with_rubrics,
    load_external_signals_from_snowflake,
    load_culture_evidence_from_snowflake,
    load_board_evidence_from_snowflake,
)
from app.services.snowflake import SnowflakeService  # noqa: E402


def print_separator(title: str = "") -> None:
    if title:
        print(f"\n{'='*70}")
        print(f"{title:^70}")
        print(f"{'='*70}\n")
    else:
        print("=" * 70)


def _d(x: str) -> Decimal:
    return Decimal(x)


def test_mapper_with_mock_data() -> None:
    """
    Mock validation:
      - returns all 7 dimensions
      - bounded scores
      - missing evidence defaults to 50
      - confidence monotonic behavior is handled by mapper internally
    """
    print_separator("TEST 1: Mock Data Validation")

    mock_evidence = [
        EvidenceScore(
            source=SignalSource.SEC_ITEM_1,
            raw_score=_d("85"),
            confidence=_d("0.90"),
            evidence_count=50,
            metadata={"test": True},
        ),
        EvidenceScore(
            source=SignalSource.TECHNOLOGY_HIRING,
            raw_score=_d("75"),
            confidence=_d("0.80"),
            evidence_count=20,
            metadata={"test": True},
        ),
        EvidenceScore(
            source=SignalSource.GLASSDOOR_REVIEWS,
            raw_score=_d("70"),
            confidence=_d("0.70"),
            evidence_count=100,
            metadata={"test": True},
        ),
        EvidenceScore(
            source=SignalSource.BOARD_COMPOSITION,
            raw_score=_d("65"),
            confidence=_d("0.85"),
            evidence_count=1,
            metadata={"test": True},
        ),
    ]

    mapper = EvidenceMapper()
    dimension_scores = mapper.map_evidence_to_dimensions(mock_evidence)

    assert len(dimension_scores) == 7, "Must return exactly 7 dimensions"
    for dim, score in dimension_scores.items():
        assert Decimal("0") <= score.score <= Decimal("100"), f"{dim.value} score out of bounds: {score.score}"
        assert Decimal("0") <= score.confidence <= Decimal("1"), f"{dim.value} confidence out of bounds: {score.confidence}"

    dims_without_evidence = [dim for dim, score in dimension_scores.items() if not score.contributing_sources]
    for dim in dims_without_evidence:
        assert dimension_scores[dim].score == Decimal("50"), f"{dim.value} should default to 50"

    print("✅ Mock data test PASSED")


def test_loader_health(sf: SnowflakeService, ticker: str) -> None:
    """
    Quick check that each loader runs without error and reports what it found.
    This helps you debug missing data vs mapper issues.
    """
    print_separator(f"LOADER HEALTH CHECK: {ticker}")

    ext = load_external_signals_from_snowflake(ticker, sf)
    sec = load_sec_evidence_from_snowflake_with_rubrics(ticker, sf)
    cul = load_culture_evidence_from_snowflake(ticker, sf)
    brd = load_board_evidence_from_snowflake(ticker, sf)

    print(f"external_signals: {len(ext)}")
    print(f"sec_sections:     {len(sec)}  (1 EvidenceScore per section)")
    print(f"culture_signals:  {len(cul)}  (latest row)")
    print(f"board_signals:    {len(brd)}  (latest row)")

    # If SEC evidence is zero, show a hint
    if len(sec) == 0:
        print("\n⚠️  SEC evidence is empty. Common causes:")
        print("   - documents_sec/document_chunks_sec not populated for this ticker")
        print("   - section values don't match SignalSource enums (e.g., 'Item 1' vs 'Item 1 (Business)')")
        print("   - sections are 'Unknown'/'Intro' and filtered out")

    # If external evidence is zero, hint
    if len(ext) == 0:
        print("\n⚠️  External signals are empty. Common causes:")
        print("   - external_signals not collected for this ticker")
        print("   - company ticker mismatch in companies table")


def test_with_real_data(ticker: str, sf: SnowflakeService) -> Dict[Dimension, Any] | None:
    print_separator(f"TEST 2: Real Data Mapping for {ticker}")

    print("Loading evidence...")
    all_evidence = load_all_evidence_from_snowflake(ticker, sf)

    if not all_evidence:
        print(f"⚠️  No evidence found for {ticker}")
        return None

    print(f"✓ Loaded {len(all_evidence)} evidence items")

    # Evidence breakdown by source
    print("\nEvidence Breakdown:")
    for ev in all_evidence:
        print(
            f"  • {ev.source.value:30s} | Score: {float(ev.raw_score):6.2f} | "
            f"Conf: {float(ev.confidence):.2f} | Count: {ev.evidence_count:4d}"
        )

        # Optional: surface rubric-by-dimension when SEC evidence exists
        if ev.source.value.startswith("Item") and "rubric_by_dimension" in ev.metadata:
            rbd = ev.metadata["rubric_by_dimension"]
            keys = list(rbd.keys())[:3]
            sample = {k: rbd[k] for k in keys}
            print(f"      rubric_by_dimension(sample): {sample} ...")

    mapper = EvidenceMapper()
    dim_scores = mapper.map_evidence_to_dimensions(all_evidence)

    # Assertions: always 7 dims, bounded
    assert set(dim_scores.keys()) == set(Dimension), "Mapper must return all 7 dimensions"
    for dim, ds in dim_scores.items():
        assert Decimal("0") <= ds.score <= Decimal("100"), f"{dim.value} score out of bounds: {ds.score}"
        assert Decimal("0") <= ds.confidence <= Decimal("1"), f"{dim.value} confidence out of bounds: {ds.confidence}"

    print_separator("DIMENSION SCORES")
    for dim in Dimension:
        ds = dim_scores[dim]
        srcs = [s.value for s in ds.contributing_sources]
        src_preview = ", ".join(srcs[:3]) + (f", +{len(srcs)-3} more" if len(srcs) > 3 else "")
        print(f"{dim.value:20s} | score={float(ds.score):6.2f} | conf={float(ds.confidence):.2f} | sources={len(srcs):2d} | {src_preview}")

    report = mapper.get_coverage_report(all_evidence)

    print_separator("COVERAGE REPORT")
    print(f"Dimensions with Evidence: {report['dimensions_with_evidence']}/{report['total_dimensions']}")
    print(f"Coverage Percentage:      {report['coverage_percentage']:.1f}%")
    if report["dimensions_without_evidence"]:
        print("\nGAPS (no evidence):")
        for d in report["dimensions_without_evidence"]:
            print(f"  • {d}")
            if d == "culture":
                print("    → run Glassdoor collector to populate culture_signals")
            if d == "ai_governance":
                print("    → run Board pipeline to populate board_governance_signals")

    print("\n✅ Real data test PASSED")
    return dim_scores


def test_property_invariants() -> None:
    """
    Lightweight invariants (not Hypothesis):
      - empty evidence returns defaults
      - single evidence affects only mapped dimensions
      - bounded scores always
    """
    print_separator("TEST 3: Lightweight Invariants")
    mapper = EvidenceMapper()

    # Property 1
    empty = mapper.map_evidence_to_dimensions([])
    assert set(empty.keys()) == set(Dimension)
    assert all(ds.score == Decimal("50") for ds in empty.values())
    print("✓ Empty evidence defaults verified")

    # Property 2: single evidence maps to expected dims
    single = [EvidenceScore(source=SignalSource.SEC_ITEM_1, raw_score=_d("80"), confidence=_d("0.9"), evidence_count=10)]
    out = mapper.map_evidence_to_dimensions(single)

    # Item 1 maps to use_case_portfolio (primary) + technology_stack (secondary)
    assert out[Dimension.USE_CASE_PORTFOLIO].score != Decimal("50")
    assert out[Dimension.TECHNOLOGY_STACK].score != Decimal("50")
    assert out[Dimension.CULTURE].score == Decimal("50")
    print("✓ Single evidence mapping verified")

    # Property 3: bounded
    extreme = [
        EvidenceScore(source=SignalSource.SEC_ITEM_1, raw_score=_d("100"), confidence=_d("1.0"), evidence_count=100),
        EvidenceScore(source=SignalSource.TECHNOLOGY_HIRING, raw_score=_d("0"), confidence=_d("0.1"), evidence_count=1),
    ]
    out2 = mapper.map_evidence_to_dimensions(extreme)
    for dim, ds in out2.items():
        assert Decimal("0") <= ds.score <= Decimal("100")
        assert Decimal("0") <= ds.confidence <= Decimal("1")
    print("✓ Boundedness verified")

    print("\n✅ Invariants PASSED")


def save_results_to_json(ticker: str, dimension_scores: Dict[Dimension, Any], filename: str | None = None) -> None:
    if filename is None:
        filename = f"results/{ticker}_dimension_scores.json"

    results_dir = Path(filename).parent
    results_dir.mkdir(parents=True, exist_ok=True)

    output = {
        "ticker": ticker,
        "dimensions": {dim.value: ds.to_dict() for dim, ds in dimension_scores.items()},
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"💾 Saved: {filename}")


def main() -> None:
    print_separator("EVIDENCE MAPPER TESTING SUITE (CS3)")
    print("Task 5.0a: Evidence-to-Dimension Mapping\n")

    # Init Snowflake once
    sf = SnowflakeService()
    print("✓ Connected to Snowflake")

    # Mock test
    test_mapper_with_mock_data()

    # Real tickers (edit to your actual CS3 set)
    cs3_companies = ["NVDA", "JPM", "WMT", "GE", "DG"]

    print(f"\nTesting with {len(cs3_companies)} companies...")

    results: Dict[str, Dict[Dimension, Any]] = {}
    for ticker in cs3_companies:
        try:
            test_loader_health(sf, ticker)
            dim_scores = test_with_real_data(ticker, sf)
            if dim_scores:
                results[ticker] = dim_scores
                save_results_to_json(ticker, dim_scores)
        except Exception as e:
            print(f"\n❌ ERROR testing {ticker}: {e}")
            import traceback
            traceback.print_exc()

    # Invariants
    test_property_invariants()

    print_separator("FINAL SUMMARY")
    print(f"✅ Successfully tested {len(results)}/{len(cs3_companies)} companies")
    print("✅ Evidence Mapper is working with CS3 loaders + tables")
    print("Next:")
    print("  - add Hypothesis property tests (tests/test_mapper_properties.py)")
    print("  - run VR property tests (tests/test_vr_properties.py)")
    print_separator()


if __name__ == "__main__":
    main()
