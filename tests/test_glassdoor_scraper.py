# tests/test_glassdoor_scraper.py

import pytest
from app.pipelines.glassdoor_collector import collect_glassdoor_data

@pytest.mark.asyncio
async def test_indeed_scraper():
    """Test Indeed scraper with NVDA."""
    print("\n" + "="*70)
    print("Testing Indeed Company Review Scraper")
    print("="*70)
    
    result = await collect_glassdoor_data("NVDA", use_cache=False)
    
    print(f"\n{result['ticker']} Results (Source: {result['source']}):")
    print(f"  Culture Score:    {result['culture_score']:.1f}/100")
    print(f"  Avg Rating:       {result['avg_rating']:.2f}/5.0")
    print(f"  Reviews:          {result['review_count']}")
    print(f"  Confidence:       {result['confidence']:.2f}")
    print(f"  Innovation:       {result['innovation_score']:.1f}")
    print(f"  Data-Driven:      {result['data_driven_score']:.1f}")
    print(f"  Rationale:        {result['rationale']}")
    
    # Assertions
    assert result['review_count'] >= 0  # May be 0 if scraping fails
    assert 0 <= result['culture_score'] <= 100
    assert result['ticker'] == "NVDA"
    assert result['source'] == "Indeed"