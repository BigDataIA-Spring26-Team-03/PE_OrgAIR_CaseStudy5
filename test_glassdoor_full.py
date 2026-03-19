import asyncio
from app.pipelines.glassdoor_collector import collect_glassdoor_data

async def test_single_company():
    """Test with one company."""
    print("="*80)
    print("Testing Single Company (NVDA)")
    print("="*80)
    
    result = await collect_glassdoor_data("NVDA", use_cache=False)
    
    print(f"\n{result['ticker']}:")
    print(f"  Culture Score:        {result['culture_score']:.1f}/100")
    print(f"  Reviews Collected:    {result['review_count']}")
    print(f"  Avg Rating:           {result['avg_rating']:.2f}/5.0")
    print(f"  Confidence:           {result['confidence']:.2f}")
    print(f"\n  Dimension Scores:")
    print(f"    Innovation:         {result['innovation_score']:.1f}")
    print(f"    Data-Driven:        {result['data_driven_score']:.1f}")
    print(f"    Change Readiness:   {result['change_readiness_score']:.1f}")
    print(f"    AI Awareness:       {result['ai_awareness_score']:.1f}")
    print(f"\n  Rationale: {result['rationale']}")


async def test_all_companies():
    """Test all 5 companies."""
    print("\n" + "="*80)
    print("Testing All 5 Companies")
    print("="*80)
    
    companies = ["NVDA", "JPM", "WMT", "GE", "DG"]
    results = []
    
    for ticker in companies:
        print(f"\nCollecting {ticker}...")
        
        try:
            result = await collect_glassdoor_data(ticker, use_cache=False)
            results.append(result)
            
            print(f"  ✅ Score: {result['culture_score']:.1f}/100")
            print(f"  ✅ Reviews: {result['review_count']}")
            print(f"  ✅ Confidence: {result['confidence']:.2f}")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    print(f"\n{'Ticker':<8} {'Score':<10} {'Reviews':<10} {'Avg Rating':<12} {'Confidence'}")
    print("-"*80)
    
    for result in sorted(results, key=lambda x: x['culture_score'], reverse=True):
        print(
            f"{result['ticker']:<8} "
            f"{result['culture_score']:<10.1f} "
            f"{result['review_count']:<10} "
            f"{result['avg_rating']:<12.2f} "
            f"{result['confidence']:.2f}"
        )
    
    print(f"\nTotal Reviews Collected: {sum(r['review_count'] for r in results)}")
    print(f"API Requests Used: ~{len(companies) * 3}")  # search + 2 pages per company


async def main():
    # Test single first
    await test_single_company()
    
    # Then test all
    await test_all_companies()

if __name__ == "__main__":
    asyncio.run(main())
