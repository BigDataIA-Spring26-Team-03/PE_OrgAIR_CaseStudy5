import asyncio
import httpx
from app.config import settings

async def test_search():
    url = "https://real-time-glassdoor-data.p.rapidapi.com/company-search"
    
    headers = {
        "x-rapidapi-host": "real-time-glassdoor-data.p.rapidapi.com",
        "x-rapidapi-key": settings.RAPIDAPI_KEY
    }
    
    companies = ["NVIDIA", "WALMART", "JPMorgan Chase"]
    
    for company_name in companies:
        params = {"query": company_name}
        
        print(f"\nSearching: {company_name}")
        print(f"API Key: {settings.RAPIDAPI_KEY[:20]}...")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url, headers=headers, params=params)
                
                print(f"Status: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Keys in response: {data.keys()}")
                    print(f"Data: {data}")
                    
            except Exception as e:
                print(f"Error: {e}")

asyncio.run(test_search())
