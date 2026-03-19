import asyncio
from app.pipelines.glassdoor_collector import IndeedCultureCollector

async def main():
    # Run with headless=False to see what's happening
    collector = IndeedCultureCollector(headless=False)
    reviews = await collector.scrape_reviews("NVDA", max_reviews=20)
    
    print(f"\nFound {len(reviews)} reviews")
    for i, review in enumerate(reviews[:3]):
        print(f"\nReview {i+1}:")
        print(f"  Rating: {review.rating}")
        print(f"  Title: {review.title}")
        print(f"  Pros: {review.pros[:100]}...")
        print(f"  Cons: {review.cons[:100]}...")

asyncio.run(main())
