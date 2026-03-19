from decimal import Decimal
from src.scoring.evidence_mapper import (
    EvidenceMapper,
    EvidenceScore,
    SignalSource
)

def main():
    print("="*80)
    print("EVIDENCE MAPPER DEMO")
    print("="*80)
    
    # Create sample evidence from multiple sources
    evidence = [
        EvidenceScore(
            source=SignalSource.TECHNOLOGY_HIRING,
            raw_score=Decimal("85"),
            confidence=Decimal("0.9"),
            evidence_count=20,
            metadata={"ai_jobs": 42}
        ),
        EvidenceScore(
            source=SignalSource.INNOVATION_ACTIVITY,
            raw_score=Decimal("75"),
            confidence=Decimal("0.8"),
            evidence_count=15,
            metadata={"patents": 12}
        ),
        EvidenceScore(
            source=SignalSource.GLASSDOOR_REVIEWS,
            raw_score=Decimal("82"),
            confidence=Decimal("0.7"),
            evidence_count=25,
            metadata={"avg_rating": 4.5}
        ),
        EvidenceScore(
            source=SignalSource.SEC_ITEM_1A,
            raw_score=Decimal("70"),
            confidence=Decimal("0.9"),
            evidence_count=1,
            metadata={"ai_risks_mentioned": True}
        )
    ]
    
    # Map to dimensions
    mapper = EvidenceMapper()
    dimensions = mapper.map_evidence_to_dimensions(evidence)
    
    print("\nDIMENSION SCORES:")
    print("-"*80)
    
    for dim, score in dimensions.items():
        print(f"\n{dim.value.upper()}")
        print(f"  Score:    {score.score:.1f}/100")
        print(f"  Sources:  {len(score.contributing_sources)}")
        print(f"  Weight:   {score.total_weight:.2f}")
        print(f"  Conf:     {score.confidence:.2f}")
        if score.contributing_sources:
            print(f"  From:     {', '.join(s.value for s in score.contributing_sources)}")
    
    # Coverage report
    print("\n" + "="*80)
    print("COVERAGE REPORT")
    print("="*80)
    
    report = mapper.get_coverage_report(evidence)
    print(f"\nTotal Dimensions: {report['total_dimensions']}")
    print(f"With Evidence: {report['dimensions_with_evidence']}")
    print(f"Coverage: {report['coverage_percentage']:.1f}%")
    
    if report['dimensions_without_evidence']:
        print(f"\nMissing Evidence For:")
        for dim in report['dimensions_without_evidence']:
            print(f"  - {dim}")

if __name__ == "__main__":
    main()
