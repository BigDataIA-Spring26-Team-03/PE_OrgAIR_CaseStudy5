import asyncio
from app.pipelines.glassdoor_collector import collect_glassdoor_data

async def main():
    print("Testing RapidAPI Glassdoor Collector...")
    result = await collect_glassdoor_data("NVDA", use_cache=False)
    
    print(f"\n{result['ticker']}:")
    print(f"  Culture Score: {result['culture_score']:.1f}/100")
    print(f"  Reviews: {result['review_count']}")
    print(f"  Avg Rating: {result['avg_rating']:.2f}")
    print(f"  Confidence: {result['confidence']:.2f}")
    print(f"  Rationale: {result['rationale']}")

asyncio.run(main())
