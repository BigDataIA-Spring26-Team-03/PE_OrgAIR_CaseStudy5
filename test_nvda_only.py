import asyncio
from app.pipelines.glassdoor_collector import collect_glassdoor_data

async def debug_nvda():
    """Debug NVDA scoring in detail."""
    
    print("="*80)
    print("NVDA CULTURE SCORE DEBUG")
    print("="*80)
    
    # Collect fresh data
    print("\n📥 Collecting NVDA reviews...")
    result = await collect_glassdoor_data("NVDA", use_cache=False)
    
    print(f"\n✅ RESULTS:")
    print(f"  Culture Score:        {result['culture_score']:.1f}/100")
    print(f"  Reviews:              {result['review_count']}")
    print(f"  Avg Rating:           {result['avg_rating']:.2f}/5.0")
    print(f"  Confidence:           {result['confidence']:.2f}")
    
    print(f"\n📊 DIMENSION SCORES:")
    print(f"  Innovation:           {result['innovation_score']:.1f}")
    print(f"  Data-Driven:          {result['data_driven_score']:.1f}")
    print(f"  Change Readiness:     {result['change_readiness_score']:.1f}")
    print(f"  AI Awareness:         {result['ai_awareness_score']:.1f}")
    
    print(f"\n📈 METADATA:")
    print(f"  Current Employee %:   {result['current_employee_ratio']:.0%}")
    print(f"  Individual Mentions:  {result['individual_mentions']}")
    
    print(f"\n💬 RATIONALE:")
    print(f"  {result['rationale']}")

asyncio.run(debug_nvda())
