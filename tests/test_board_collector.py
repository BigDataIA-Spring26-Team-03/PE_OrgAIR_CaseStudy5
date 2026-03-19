"""
Tests for the LLM-powered board composition pipeline:
- BoardCompositionCollector (Stage 1: text extraction)
- board_chunker (Stage 2: chunking)
- board_llm_extractor (Stage 3-4: extraction + merge)
- BoardCompositionAnalyzer integration (Stage 5: scoring)
"""

import sys
import json
import tempfile
from pathlib import Path
from decimal import Decimal
from unittest.mock import patch, MagicMock

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from app.pipelines.board_collector import BoardCompositionCollector
from app.pipelines.board_chunker import chunk_proxy_text, _is_garbage_chunk
from app.pipelines.board_llm_extractor import (
    DirectorExtraction,
    ChunkExtraction,
    merge_extractions,
    quality_check,
    _normalize_name,
    _merge_into,
)
from scoring.board_analyzer import BoardCompositionAnalyzer, BoardMember


# ── HTML fixtures ────────────────────────────────────────────────────

SAMPLE_PROXY_HTML = """
<html>
<body>
<h2>Proposal 1 — Election of Directors</h2>
<div>
    <p><b>Dr. Jane Smith</b></p>
    <p>Independent Director. Chief Technology Officer of Acme Corp.
        Expertise in artificial intelligence and machine learning.
        Member of the Technology Committee and Audit Committee.
        Appointed since 2018.</p>
    <p><b>John Doe</b></p>
    <p>Independent Director. Former President and CEO.
        Member of the Compensation Committee and Risk Committee.
        Serving since 2015.</p>
    <p><b>Alice Johnson</b></p>
    <p>Chief Data Officer. Leads data science and analytics initiatives.
        Member of the Audit Committee. Joined 2020.</p>
    <p><b>Bob Williams</b></p>
    <p>Director. Background in finance and operations.
        Member of the Finance Committee. Since 2019.</p>
    <p><b>Carol Davis</b></p>
    <p>Independent Director. Expertise in cybersecurity and digital strategy.
        Member of the Risk Committee. Appointed 2021.</p>
</div>

<h3>Committees of the Board</h3>
<p>The board has the following committees: audit committee, compensation committee,
technology committee, risk management committee, and finance committee.</p>

<h3>Strategy</h3>
<p>Our company is investing in artificial intelligence to drive innovation.
We are also leveraging machine learning across our operations.</p>
</body>
</html>
"""

EMPTY_PROXY_HTML = """
<html><body><p>No relevant board information.</p></body></html>
"""


# ── Mock LLM extractions for testing ─────────────────────────────────

MOCK_EXTRACTIONS = [
    ChunkExtraction(directors=[
        DirectorExtraction(
            name="Dr. Jane Smith",
            title="Chief Technology Officer",
            committees=["Technology Committee", "Audit Committee"],
            is_independent=True,
            tenure_since_year=2018,
            bio="Chief Technology Officer of Acme Corp with expertise in artificial intelligence and machine learning.",
            evidence=["Expertise in artificial intelligence and machine learning"],
        ),
        DirectorExtraction(
            name="John Doe",
            title="Former President and CEO",
            committees=["Compensation Committee", "Risk Committee"],
            is_independent=True,
            tenure_since_year=2015,
            bio="Former President and CEO. Independent Director serving on Compensation and Risk Committees.",
            evidence=[],
        ),
    ]),
    ChunkExtraction(directors=[
        DirectorExtraction(
            name="Alice Johnson",
            title="Chief Data Officer",
            committees=["Audit Committee"],
            is_independent=False,
            tenure_since_year=2020,
            bio="Chief Data Officer leading data science and analytics initiatives.",
            evidence=[],
        ),
        DirectorExtraction(
            name="Bob Williams",
            title="Director",
            committees=["Finance Committee"],
            is_independent=False,
            tenure_since_year=2019,
            bio="Director with background in finance and operations.",
            evidence=[],
        ),
        DirectorExtraction(
            name="Carol Davis",
            title="Director",
            committees=["Risk Committee"],
            is_independent=True,
            tenure_since_year=2021,
            bio="Independent Director with expertise in cybersecurity and digital strategy.",
            evidence=[],
        ),
    ]),
]


# ── Stage 1: Collector Tests ─────────────────────────────────────────

def _make_collector(tmp_dir=None):
    d = tmp_dir or tempfile.mkdtemp()
    return BoardCompositionCollector(data_dir=d)


def test_collector_extracts_committees():
    """Committee names are extracted from text."""
    collector = _make_collector()
    from bs4 import BeautifulSoup
    from app.pipelines.document_chunker_s3 import normalize_ws

    soup = BeautifulSoup(SAMPLE_PROXY_HTML, "html.parser")
    for tag in soup(["script", "style", "meta", "link"]):
        tag.decompose()
    raw_text = normalize_ws(soup.get_text(separator="\n", strip=True))
    text_lower = raw_text.lower()

    committees = collector._extract_committees(text_lower)
    committee_lower = [c.lower() for c in committees]
    assert any("technology" in c for c in committee_lower), f"No technology committee in {committees}"
    assert any("audit" in c for c in committee_lower)
    print("PASS test_collector_extracts_committees")


def test_collector_extracts_strategy_text():
    """Strategy passages mentioning AI/ML are extracted."""
    collector = _make_collector()
    from bs4 import BeautifulSoup
    from app.pipelines.document_chunker_s3 import normalize_ws

    soup = BeautifulSoup(SAMPLE_PROXY_HTML, "html.parser")
    for tag in soup(["script", "style", "meta", "link"]):
        tag.decompose()
    raw_text = normalize_ws(soup.get_text(separator="\n", strip=True))
    text_lower = raw_text.lower()

    strategy = collector._extract_strategy_text(text_lower)
    assert "artificial intelligence" in strategy
    print("PASS test_collector_extracts_strategy_text")


def test_cache_round_trip():
    """Data saved to cache can be loaded back when members are present."""
    tmp = tempfile.mkdtemp()
    collector = BoardCompositionCollector(data_dir=tmp)

    original = {
        "members": [
            {
                "name": "Test Person",
                "title": "Director",
                "bio": "Some biographical information about this person.",
                "is_independent": True,
                "committees": ["Audit Committee"],
                "tenure_years": 5.0,
            }
        ],
        "committees": ["Audit Committee", "Technology Committee"],
        "strategy_text": "artificial intelligence is key",
        "source_meta": {},
    }

    collector.cache_with_members("TST", original)
    loaded = collector.load_from_cache("TST")

    assert loaded is not None
    assert loaded["members"] == original["members"]
    assert loaded["committees"] == original["committees"]
    assert loaded["strategy_text"] == original["strategy_text"]
    print("PASS test_cache_round_trip")


def test_cache_miss_returns_none():
    """Loading from cache with no file returns None."""
    tmp = tempfile.mkdtemp()
    collector = BoardCompositionCollector(data_dir=tmp)
    assert collector.load_from_cache("NONEXISTENT") is None
    print("PASS test_cache_miss_returns_none")


def test_cache_without_members_returns_none():
    """Cache without members triggers re-extraction."""
    tmp = tempfile.mkdtemp()
    collector = BoardCompositionCollector(data_dir=tmp)

    data = {
        "members": [],
        "committees": [],
        "strategy_text": "",
    }
    collector._cache_results("TST", data)
    assert collector.load_from_cache("TST") is None
    print("PASS test_cache_without_members_returns_none")


# ── Stage 2: Chunker Tests ──────────────────────────────────────────

def test_chunk_proxy_text_produces_chunks():
    """Chunking produces non-empty results for a long text."""
    text = "This is a test sentence. " * 500
    chunks = chunk_proxy_text(text)
    assert len(chunks) > 0
    for c in chunks:
        assert "chunk_text" in c
        assert "chunk_index" in c
        assert len(c["chunk_text"]) > 0
    print("PASS test_chunk_proxy_text_produces_chunks")


def test_chunk_empty_text():
    """Empty text returns no chunks."""
    assert chunk_proxy_text("") == []
    assert chunk_proxy_text("   ") == []
    print("PASS test_chunk_empty_text")


def test_garbage_chunk_filtering():
    """Garbage chunks are identified correctly."""
    assert _is_garbage_chunk("") is True
    assert _is_garbage_chunk("short") is True
    assert _is_garbage_chunk("12345 67890 " * 100) is True  # low alpha
    assert _is_garbage_chunk("table of contents\n" + "x " * 100) is True
    assert _is_garbage_chunk("This is a normal paragraph about board directors " * 5) is False
    print("PASS test_garbage_chunk_filtering")


# ── Stage 3-4: Extractor + Merge Tests ──────────────────────────────

def test_merge_extractions_deduplicates():
    """Duplicate directors across chunks are merged."""
    extractions = [
        ChunkExtraction(directors=[
            DirectorExtraction(name="Jane Smith", bio="Short bio", is_independent=True),
        ]),
        ChunkExtraction(directors=[
            DirectorExtraction(
                name="Jane Smith",
                bio="A much longer bio with more detail about her career.",
                committees=["Audit Committee"],
            ),
        ]),
    ]
    members = merge_extractions(extractions)
    assert len(members) == 1
    assert members[0]["is_independent"] is True  # True overrides
    assert "longer bio" in members[0]["bio"]  # longest bio wins
    assert "Audit Committee" in members[0]["committees"]
    print("PASS test_merge_extractions_deduplicates")


def test_merge_extractions_multiple_directors():
    """Multiple distinct directors are preserved."""
    members = merge_extractions(MOCK_EXTRACTIONS)
    assert len(members) == 5
    names = [m["name"] for m in members]
    assert "Dr. Jane Smith" in names
    assert "John Doe" in names
    assert "Alice Johnson" in names
    print("PASS test_merge_extractions_multiple_directors")


def test_merge_tenure_conversion():
    """tenure_since_year is converted to tenure_years."""
    members = merge_extractions(MOCK_EXTRACTIONS)
    jane = next(m for m in members if "Jane" in m["name"])
    assert jane["tenure_years"] > 0
    assert "tenure_since_year" not in jane
    print("PASS test_merge_tenure_conversion")


def test_normalize_name():
    """Name normalization handles suffixes and prefixes."""
    assert _normalize_name("Dr. Jane Smith") == "jane smith"
    assert _normalize_name("John Doe Jr.") == "john doe"
    assert _normalize_name("  Alice  Johnson  III ") == "alice johnson"
    assert _normalize_name("Mr. Bob Williams") == "bob williams"
    print("PASS test_normalize_name")


def test_merge_into_committees_union():
    """Merging accumulates unique committees."""
    existing = {"committees": ["Audit Committee"], "bio": "", "evidence": []}
    new = DirectorExtraction(
        name="Test",
        committees=["Audit Committee", "Risk Committee"],
    )
    _merge_into(existing, new)
    assert len(existing["committees"]) == 2
    assert "Risk Committee" in existing["committees"]
    print("PASS test_merge_into_committees_union")


def test_merge_into_bio_longest():
    """Merging keeps the longest bio."""
    existing = {"bio": "Short", "committees": [], "evidence": []}
    new = DirectorExtraction(name="Test", bio="This is a much longer biography.")
    _merge_into(existing, new)
    assert existing["bio"] == "This is a much longer biography."
    print("PASS test_merge_into_bio_longest")


def test_quality_check_pass():
    """Quality check passes with enough good members."""
    members = [
        {"name": f"Director {i}", "bio": "x" * 60} for i in range(8)
    ]
    ok, reason = quality_check(members)
    assert ok is True
    assert reason == "ok"
    print("PASS test_quality_check_pass")


def test_quality_check_too_few():
    """Quality check fails with too few members."""
    members = [{"name": "A B", "bio": "x" * 60}]
    ok, reason = quality_check(members)
    assert ok is False
    assert "too_few" in reason
    print("PASS test_quality_check_too_few")


def test_quality_check_low_bio():
    """Quality check fails with low bio quality."""
    members = [{"name": f"Director {i}", "bio": ""} for i in range(8)]
    ok, reason = quality_check(members)
    assert ok is False
    assert "bio" in reason
    print("PASS test_quality_check_low_bio")


# ── Stage 5: Scoring Integration ────────────────────────────────────

def test_mock_members_feed_analyzer():
    """LLM-extracted members can be scored by BoardCompositionAnalyzer."""
    members_raw = merge_extractions(MOCK_EXTRACTIONS)

    members = [
        BoardMember(
            name=m["name"],
            title=m.get("title", "Director"),
            committees=m.get("committees", []),
            bio=m.get("bio", ""),
            is_independent=m.get("is_independent", False),
            tenure_years=m.get("tenure_years", 0.0),
        )
        for m in members_raw
    ]

    analyzer = BoardCompositionAnalyzer()
    signal = analyzer.analyze_board(
        company_id="test-id",
        ticker="TST",
        members=members,
        committees=["Technology Committee", "Audit Committee", "Risk Management Committee"],
        strategy_text="artificial intelligence is central to our strategy",
    )

    assert Decimal("0") <= signal.governance_score <= Decimal("100")
    assert Decimal("0") <= signal.confidence <= Decimal("0.95")
    assert signal.ticker == "TST"
    print("PASS test_mock_members_feed_analyzer")


def test_mock_members_score_above_base():
    """Rich mock data should produce a score above the base 20."""
    members_raw = merge_extractions(MOCK_EXTRACTIONS)

    members = [
        BoardMember(
            name=m["name"],
            title=m.get("title", "Director"),
            committees=m.get("committees", []),
            bio=m.get("bio", ""),
            is_independent=m.get("is_independent", False),
            tenure_years=m.get("tenure_years", 0.0),
        )
        for m in members_raw
    ]

    analyzer = BoardCompositionAnalyzer()
    signal = analyzer.analyze_board(
        company_id="test-id",
        ticker="TST",
        members=members,
        committees=["Technology Committee", "Risk Committee"],
        strategy_text="artificial intelligence and machine learning",
    )

    assert signal.governance_score > Decimal("40"), (
        f"Expected >40, got {signal.governance_score}"
    )
    print("PASS test_mock_members_score_above_base")


def test_empty_members_score_base():
    """Empty members should produce only the base score of 20."""
    analyzer = BoardCompositionAnalyzer()
    signal = analyzer.analyze_board(
        company_id="test-id",
        ticker="TST",
        members=[],
        committees=[],
        strategy_text="",
    )

    assert signal.governance_score == Decimal("20")
    print("PASS test_empty_members_score_base")


def test_signal_evidence_populated():
    """GovernanceSignal.evidence is populated with descriptions."""
    members_raw = merge_extractions(MOCK_EXTRACTIONS)

    members = [
        BoardMember(
            name=m["name"],
            title=m.get("title", "Director"),
            committees=m.get("committees", []),
            bio=m.get("bio", ""),
            is_independent=m.get("is_independent", False),
            tenure_years=m.get("tenure_years", 0.0),
        )
        for m in members_raw
    ]

    analyzer = BoardCompositionAnalyzer()
    signal = analyzer.analyze_board(
        company_id="test-id",
        ticker="TST",
        members=members,
        committees=["Technology Committee"],
        strategy_text="artificial intelligence",
    )

    assert len(signal.evidence) > 0
    print("PASS test_signal_evidence_populated")


# ── Runner ────────────────────────────────────────────────────────────

def main():
    tests = [
        # Stage 1
        test_collector_extracts_committees,
        test_collector_extracts_strategy_text,
        test_cache_round_trip,
        test_cache_miss_returns_none,
        test_cache_without_members_returns_none,
        # Stage 2
        test_chunk_proxy_text_produces_chunks,
        test_chunk_empty_text,
        test_garbage_chunk_filtering,
        # Stage 3-4
        test_merge_extractions_deduplicates,
        test_merge_extractions_multiple_directors,
        test_merge_tenure_conversion,
        test_normalize_name,
        test_merge_into_committees_union,
        test_merge_into_bio_longest,
        test_quality_check_pass,
        test_quality_check_too_few,
        test_quality_check_low_bio,
        # Stage 5
        test_mock_members_feed_analyzer,
        test_mock_members_score_above_base,
        test_empty_members_score_base,
        test_signal_evidence_populated,
    ]
    passed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"FAIL {t.__name__}: {e}")

    print(f"\n{passed}/{len(tests)} tests passed")
    if passed < len(tests):
        sys.exit(1)


if __name__ == "__main__":
    main()
