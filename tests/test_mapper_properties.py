from decimal import Decimal

from hypothesis import given, settings, strategies as st

from src.scoring.evidence_mapper import EvidenceMapper, EvidenceScore, Dimension, SignalSource


def dec_0_100():
    return st.decimals(min_value=Decimal("0"), max_value=Decimal("100"), places=2)

def dec_0_1():
    return st.decimals(min_value=Decimal("0"), max_value=Decimal("1"), places=3)


evidence_strategy = st.builds(
    EvidenceScore,
    source=st.sampled_from(list(SignalSource)),
    raw_score=dec_0_100(),
    confidence=dec_0_1(),
    evidence_count=st.integers(min_value=0, max_value=2000),
    metadata=st.dictionaries(st.text(min_size=1, max_size=10), st.text(max_size=30), max_size=5),
)


@settings(max_examples=500)
@given(evs=st.lists(evidence_strategy, min_size=0, max_size=30))
def test_all_dimensions_returned(evs):
    out = EvidenceMapper().map_evidence_to_dimensions(evs)
    assert set(out.keys()) == set(Dimension)


def test_missing_evidence_defaults_to_50():
    out = EvidenceMapper().map_evidence_to_dimensions([])
    assert set(out.keys()) == set(Dimension)
    assert all(ds.score == Decimal("50") for ds in out.values())


@settings(max_examples=500)
@given(
    base=st.lists(evidence_strategy, min_size=1, max_size=20),
    extra=st.lists(evidence_strategy, min_size=1, max_size=20),
)
def test_more_evidence_higher_or_equal_confidence(base, extra):
    """
    Lab property: More sources -> higher confidence.
    We enforce monotonicity (non-decreasing), which is the safe/provable form.
    """
    mapper = EvidenceMapper()
    out1 = mapper.map_evidence_to_dimensions(base)
    out2 = mapper.map_evidence_to_dimensions(base + extra)

    for dim in Dimension:
        assert out2[dim].confidence >= out1[dim].confidence
