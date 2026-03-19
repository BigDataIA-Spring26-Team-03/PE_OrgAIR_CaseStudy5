# app/pipelines/glassdoor_collector.py

import httpx
import json
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime
import re
import logging

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class CompanyReview:
    """Single company review from Glassdoor."""
    company_id: str
    review_id: str
    rating: float
    title: str
    pros: str
    cons: str
    advice_to_management: Optional[str]
    is_current_employee: bool
    job_title: str
    review_date: datetime
    
    @property
    def full_text(self) -> str:
        """Combine all text for analysis - INCLUDING job title!"""
        text = f"{self.title} {self.pros} {self.cons} {self.job_title}"  # ADD job_title
        if self.advice_to_management:
            text += f" {self.advice_to_management}"
        return text.lower()


@dataclass
class CultureSignals:
    """Analyzed culture signals."""
    innovation_score: Decimal
    data_driven_score: Decimal
    change_readiness_score: Decimal
    ai_awareness_score: Decimal
    avg_rating: Decimal
    review_count: int
    positive_sentiment_ratio: Decimal
    innovation_keyword_count: int
    data_keyword_count: int
    individual_mentions: int = 0
    current_employee_ratio: Decimal = Decimal("0.5")


@dataclass
class CultureScore:
    """Final culture assessment."""
    overall_score: Decimal
    signals: CultureSignals
    confidence: Decimal
    rationale: str


class GlassdoorAPICollector:
    """
    Collect Glassdoor reviews using RapidAPI.
    
    Dynamically searches for companies and fetches reviews.
    """
    
    # Culture keywords
    INNOVATION_POSITIVE = [
        # CS3 Document keywords
        "innovative", "cutting-edge", "forward-thinking",
        "encourages new ideas", "experimental", "creative freedom",
        "startup mentality", "move fast", "disruptive",
        # Expanded
        "innovation", "pioneering", "breakthrough", "novel",
        "creative", "creativity", "leading-edge", "visionary",
        "collaboration", "collaborative", "first principle",
        "world-class", "best", "top tier", "excellence",
        "research", "r&d", "prototype", "pilot"
    ]
    
    INNOVATION_NEGATIVE = [
        # CS3 Document keywords
        "bureaucratic", "slow to change", "resistant",
        "outdated", "stuck in old ways", "red tape",
        "politics", "siloed", "hierarchical",
        # Expanded
        "conservative", "micromanagement", "inflexible",
        "legacy systems", "no innovation", "stagnant",
        "old-fashioned", "behind", "inefficient"
    ]
    
    DATA_DRIVEN_KEYWORDS = [
        # CS3 Document keywords
        "data-driven", "metrics", "evidence-based",
        "analytical", "kpis", "dashboards", "data culture",
        "measurement", "quantitative",
        # Expanded
        "data science", "analytics", "data analytics",
        "insights", "reporting", "business intelligence",
        "data literacy", "analysis", "statistics",
        "data team", "analyst", "data engineer",
        "bi", "reporting tools", "data quality"
    ]
    
    AI_AWARENESS_KEYWORDS = [
        # CS3 Document keywords
        "ai", "artificial intelligence", "machine learning",
        "automation", "data science", "ml", "algorithms",
        "predictive", "neural network",
        # Expanded
        "deep learning", "models", "llm", "training",
        "inference", "gpu", "cuda", "ai platform",
        "ml engineer", "ml platform", "ai tools",
        "generative ai", "nlp", "computer vision",
        "tensorflow", "pytorch", "ai strategy"
    ]
    
    CHANGE_POSITIVE = [
        # CS3 Document keywords
        "agile", "adaptive", "fast-paced", "embraces change",
        "continuous improvement", "growth mindset",
        # Expanded
        "flexible", "dynamic", "responsive", "evolving",
        "transforming", "transformation", "modern",
        "progressive", "open to change", "willing to adapt",
        "learning culture", "improvement", "advancing"
    ]
    
    CHANGE_NEGATIVE = [
        # CS3 Document keywords
        "rigid", "traditional", "slow", "risk-averse",
        "change resistant", "old school",
        # Expanded
        "bureaucracy", "resistant to change", "stuck",
        "inflexible", "unwilling", "hesitant",
        "status quo", "comfort zone", "afraid of change"
    ]
    
    def __init__(self, data_dir: str = "data/glassdoor"):
        """Initialize with RapidAPI key."""
        self.api_key = settings.RAPIDAPI_KEY
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.api_key:
            raise ValueError("RAPIDAPI_KEY not set in .env file!")
    
    async def search_company(self, company_name: str) -> Optional[str]:
        """
        Search for company and return Glassdoor company_id.
        
        Args:
            company_name: Full company name (e.g., "NVIDIA Corporation")
            
        Returns:
            Glassdoor company_id as string, or None if not found
        """
        
        print(f"\n{'='*70}")
        print(f"🔍 Searching Glassdoor for: {company_name}")
        print(f"{'='*70}")
        
        # Try multiple variations
        search_terms = [
            company_name,
            company_name.replace(" Inc.", "").replace(" Corporation", "").strip(),
            company_name.split()[0]
        ]
        
        search_terms = list(dict.fromkeys(search_terms))
        
        print(f"📝 Will try {len(search_terms)} variations:")
        for i, term in enumerate(search_terms, 1):
            print(f"   {i}. '{term}'")
        
        url = "https://real-time-glassdoor-data.p.rapidapi.com/company-search"
        
        headers = {
            "x-rapidapi-host": "real-time-glassdoor-data.p.rapidapi.com",
            "x-rapidapi-key": self.api_key
        }
        
        for attempt, term in enumerate(search_terms, 1):
            print(f"\n{'─'*70}")
            print(f"Attempt {attempt}/{len(search_terms)}: '{term}'")
            
            params = {"query": term}
            
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url, headers=headers, params=params)
                    
                    print(f"Status: {response.status_code}")
                    
                    if response.status_code != 200:
                        print(f"Error: {response.text[:200]}")
                        continue
                    
                    data = response.json()
                    companies = data.get('data', [])
                    
                    print(f"Found: {len(companies)} companies")
                    
                    if not companies:
                        continue
                    
                    company = companies[0]
                    company_id = str(company.get('company_id') or company.get('id'))
                    found_name = company.get('name', 'Unknown')
                    
                    print(f"✅ SUCCESS: {found_name} (ID: {company_id})")
                    
                    return company_id
                    
            except Exception as e:
                print(f"❌ Error: {e}")
                continue
        
        print(f"\n❌ No company found for: {company_name}")
        return None
    
    async def fetch_reviews_from_api(
        self, 
        ticker: str,
        max_reviews: int = 40
    ) -> List[CompanyReview]:
        """
        Fetch reviews from RapidAPI - DYNAMIC (no hardcoded IDs).
        
        Args:
            ticker: Company ticker (e.g., "NVDA")
            max_reviews: Number of reviews to fetch (will paginate if needed)
            
        Returns:
            List of CompanyReview objects
        """
        
        print(f"\n{'='*70}")
        print(f"📥 Fetching Reviews for {ticker}")
        print(f"{'='*70}")
        
        # Step 1: Get company name from database
        from app.services.snowflake import db
        
        company_query = f"SELECT name FROM companies WHERE ticker = '{ticker.upper()}'"
        result = db.execute_query(company_query)
        
        if not result:
            print(f"❌ Company {ticker} not found in database")
            return []
        
        company_name = result[0].get('NAME') or result[0].get('name')
        print(f"✅ Company Name: {company_name}")
        
        # Step 2: Search Glassdoor for company_id
        company_id = await self.search_company(company_name)
        
        if not company_id:
            print(f"❌ Could not find Glassdoor ID")
            return []
        
        print(f"✅ Glassdoor Company ID: {company_id}")
        
        # Step 3: Fetch reviews (with pagination)
        url = "https://real-time-glassdoor-data.p.rapidapi.com/company-reviews"
        
        headers = {
            "x-rapidapi-host": "real-time-glassdoor-data.p.rapidapi.com",
            "x-rapidapi-key": self.api_key
        }
        
        all_reviews = []
        reviews_per_page = 10
        pages_needed = 4
        
        print(f"\n📄 Fetching {pages_needed} pages (target: {max_reviews} reviews)")
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                for page in range(1, pages_needed + 1):
                    params = {
                        "company_id": company_id,
                        "page": str(page),
                        "sort": "POPULAR",
                        "language": "en",
                        "only_current_employees": "false",
                        "extended_rating_data": "false",
                        "domain": "www.glassdoor.com"
                    }
                    
                    print(f"\n  Page {page}/{pages_needed}:")
                    print(f"    Requesting...")
                    
                    response = await client.get(url, headers=headers, params=params)
                    
                    print(f"    Status: {response.status_code}")
                    
                    if response.status_code != 200:
                        print(f"    Error: {response.text[:200]}")
                        break
                    
                    data = response.json()
                    
                    print(f"    Response Keys: {list(data.keys())}")
                    print(f"    Data structure: {type(data.get('data'))}")
                    
                    # Extract reviews from nested structure
                    reviews_data = data.get('data', {}).get('reviews', [])
                    
                    print(f"    Reviews in response: {len(reviews_data)}")
                    
                    if reviews_data:
                        print(f"    First review keys: {list(reviews_data[0].keys())[:10]}")
                        print(f"    🔄 Starting to parse {len(reviews_data)} reviews...")
                    
                    if not reviews_data:
                        print(f"    No more reviews, stopping pagination")
                        break
                    
                    # Parse reviews
                    parsed_count = 0
                    for i, review_data in enumerate(reviews_data):
                        print(f"      📝 Parsing review {i+1}/{len(reviews_data)}...")
                        
                        if len(all_reviews) >= max_reviews:
                            print(f"      ⏸️ Reached max_reviews limit ({max_reviews})")
                            break
                        
                        review = self._parse_api_review(review_data, len(all_reviews), ticker)
                        
                        if review:
                            all_reviews.append(review)
                            parsed_count += 1
                            print(f"      ✅ Added review {i+1}: '{review.title[:30]}...' (rating: {review.rating})")
                        else:
                            print(f"      ❌ Review {i+1} returned None!")
                    
                    print(f"    Parsed: {parsed_count}/{len(reviews_data)} reviews")
                    print(f"    Total so far: {len(all_reviews)}")
                    
                    if len(all_reviews) >= max_reviews:
                        print(f"    ✅ Target reached!")
                        break
            
            print(f"\n✅ TOTAL COLLECTED: {len(all_reviews)} reviews for {ticker}")
            
            if all_reviews:
                print(f"📦 Caching reviews...")
                self._cache_reviews(ticker, all_reviews)
                print(f"✅ Cached to data/glassdoor/{ticker}.json")
            else:
                print(f"⚠️ No reviews to cache")
            
            return all_reviews
            
        except Exception as e:
            print(f"\n❌ Error during review fetch: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _parse_api_review(self, data: Dict, index: int, ticker: str) -> Optional[CompanyReview]:
        """Parse review from RapidAPI response."""
    
        try:
            print(f"      Parsing review {index}...")
        
        # Extract fields
            review_id = str(data.get('review_id', f'r{index}'))
            rating = float(data.get('rating', 3.0))
            title = data.get('summary', 'Review')
            pros = data.get('pros', '')
            cons = data.get('cons', '')
            advice = data.get('advice_to_management')
            is_current = data.get('is_current_employee', False)
            job_title = data.get('job_title', 'Employee')
        
        # Parse datetime
            date_str = data.get('review_datetime', '')
            try:
                review_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                review_date = datetime.now()
        
            review = CompanyReview(
            company_id=ticker,
            review_id=review_id,
            rating=rating,
            title=title,
            pros=pros,
            cons=cons,
            advice_to_management=advice,
            is_current_employee=is_current,
            job_title=job_title,
            review_date=review_date
            )
        
            print(f"      ✅ Parsed: {title[:30]}... (rating: {rating})")
            return review
        
        except Exception as e:
            print(f"      ❌ Parse error: {e}")
            import traceback
            traceback.print_exc()
            return None
            
        
    
    def _cache_reviews(self, ticker: str, reviews: List[CompanyReview]):
        """Save reviews to JSON cache."""
        cache_file = self.data_dir / f"{ticker}.json"
        
        data = {
            "ticker": ticker,
            "source": "RapidAPI Glassdoor",
            "cached_at": datetime.now().isoformat(),
            "review_count": len(reviews),
            "reviews": [
                {
                    "review_id": r.review_id,
                    "rating": r.rating,
                    "title": r.title,
                    "pros": r.pros,
                    "cons": r.cons,
                    "advice_to_management": r.advice_to_management,
                    "is_current_employee": r.is_current_employee,
                    "job_title": r.job_title,
                    "review_date": r.review_date.isoformat()
                }
                for r in reviews
            ]
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Cached {len(reviews)} reviews to {cache_file}")
    
    def load_from_cache(self, ticker: str) -> List[CompanyReview]:
        """Load from cache."""
        cache_file = self.data_dir / f"{ticker}.json"
        
        if not cache_file.exists():
            return []
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            reviews = []
            for r in data.get('reviews', []):
                reviews.append(CompanyReview(
                    company_id=ticker,
                    review_id=r['review_id'],
                    rating=r['rating'],
                    title=r['title'],
                    pros=r['pros'],
                    cons=r['cons'],
                    advice_to_management=r.get('advice_to_management'),
                    is_current_employee=r['is_current_employee'],
                    job_title=r['job_title'],
                    review_date=datetime.fromisoformat(r['review_date'])
                ))
            
            logger.info(f"Loaded {len(reviews)} reviews from cache for {ticker}")
            return reviews
            
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            return []
    
    async def fetch_reviews(
        self,
        ticker: str,
        use_cache: bool = True,
        max_reviews: int = 40
    ) -> List[CompanyReview]:
        """Fetch reviews (cache or API)."""
        
        if use_cache:
            reviews = self.load_from_cache(ticker)
            if reviews:
                logger.info(f"Using cached reviews for {ticker}")
                return reviews[:max_reviews]
        
        return await self.fetch_reviews_from_api(ticker, max_reviews)
    
    def analyze_culture_signals(self, reviews: List[CompanyReview]) -> CultureSignals:
        """
        Analyze reviews for culture indicators using CS3 algorithm.
    
        CS3 Algorithm:
        1. Combine pros and cons text for each review
        2. Count keyword matches for each category
        3. Weight by recency (last 2 years = full weight, older = 0.5)
        4. Weight current employees higher (1.2x multiplier)
        5. Calculate component scores using CS3 formulas
            6. Calculate overall weighted average
         """
    
        if not reviews:
            return CultureSignals(
                innovation_score=Decimal("50"),
                data_driven_score=Decimal("50"),
                change_readiness_score=Decimal("50"),
                ai_awareness_score=Decimal("50"),
                avg_rating=Decimal("3.0"),
                review_count=0,
                positive_sentiment_ratio=Decimal("0.5"),
                innovation_keyword_count=0,
                data_keyword_count=0,
                individual_mentions=0,
                current_employee_ratio=Decimal("0.5")
            )
    
    # Initialize counters
        innovation_positive = 0.0
        innovation_negative = 0.0
        data_driven_mentions = 0.0
        ai_awareness_mentions = 0.0
        change_positive = 0.0
        change_negative = 0.0
        total_weight = 0.0
    
    # Process each review
        for review in reviews:
        # Combine text (pros + cons + advice)
            text = f"{review.pros} {review.cons}".lower()
            if review.advice_to_management:
                text += f" {review.advice_to_management}".lower()
        
        # Calculate recency weight
            from datetime import timezone
            days_old = (datetime.now(timezone.utc) - review.review_date).days
            recency_weight = 1.0 if days_old < 730 else 0.5  # 730 days = 2 years
        
        # Calculate employee weight
            employee_weight = 1.2 if review.is_current_employee else 1.0
        
        # Combined weight
            weight = recency_weight * employee_weight
            total_weight += weight
        
        # Count keywords (weighted)
            for kw in self.INNOVATION_POSITIVE:
                if kw in text:
                    innovation_positive += weight
        
            for kw in self.INNOVATION_NEGATIVE:
                if kw in text:
                    innovation_negative += weight
        
            for kw in self.DATA_DRIVEN_KEYWORDS:
                if kw in text:
                    data_driven_mentions += weight
        
            for kw in self.AI_AWARENESS_KEYWORDS:
                if kw in text:
                    ai_awareness_mentions += weight
        
            for kw in self.CHANGE_POSITIVE:
                if kw in text:
                    change_positive += weight
        
            for kw in self.CHANGE_NEGATIVE:
                if kw in text:
                    change_negative += weight
    
        num_reviews = len(reviews)
    
    # CS3 Formulas:
    # innovation_score = (positive - negative) / total * 50 + 50
        innovation_score = ((innovation_positive - innovation_negative) / num_reviews) * 50 + 50
        innovation_score = max(0, min(100, innovation_score))
    
    # data_driven_score = data_mentions / total * 100
        data_driven_score = (data_driven_mentions / num_reviews) * 100
        data_driven_score = max(0, min(100, data_driven_score))
    
    # ai_awareness_score = ai_mentions / total * 100
        ai_awareness_score = (ai_awareness_mentions / num_reviews) * 100
        ai_awareness_score = max(0, min(100, ai_awareness_score))
    
    # change_readiness = (positive - negative) / total * 50 + 50
        change_readiness_score = ((change_positive - change_negative) / num_reviews) * 50 + 50
        change_readiness_score = max(0, min(100, change_readiness_score))
    
    # Calculate other metrics
        avg_rating = sum(r.rating for r in reviews) / len(reviews)
        positive_reviews = sum(1 for r in reviews if r.rating >= 4.0)
        positive_ratio = positive_reviews / len(reviews)
        current_employees = sum(1 for r in reviews if r.is_current_employee)
        current_ratio = current_employees / len(reviews)
    
    # Count individuals mentioned
        all_text = " ".join(review.full_text for review in reviews)
        individual_mentions = self._count_individual_mentions(all_text)
    
        return CultureSignals(
            innovation_score=Decimal(str(round(innovation_score, 2))),
            data_driven_score=Decimal(str(round(data_driven_score, 2))),
            change_readiness_score=Decimal(str(round(change_readiness_score, 2))),
            ai_awareness_score=Decimal(str(round(ai_awareness_score, 2))),
            avg_rating=Decimal(str(round(avg_rating, 2))),
            review_count=len(reviews),
            positive_sentiment_ratio=Decimal(str(round(positive_ratio, 2))),
            innovation_keyword_count=int(innovation_positive),
            data_keyword_count=int(data_driven_mentions),
            individual_mentions=individual_mentions,
            current_employee_ratio=Decimal(str(round(current_ratio, 2)))
        )
    
    async def calculate_culture_score(self, ticker: str, use_cache: bool = True) -> CultureScore:
        """Main method: fetch and analyze using CS3 formulas."""
    
        reviews = await self.fetch_reviews(ticker, use_cache=use_cache, max_reviews=40)
        signals = self.analyze_culture_signals(reviews)
    
    # CS3 Formula: 0.30*innovation + 0.25*data + 0.25*ai + 0.20*change
        overall_score = (
            signals.innovation_score * Decimal("0.30") +
            signals.data_driven_score * Decimal("0.25") +
            signals.ai_awareness_score * Decimal("0.25") +
            signals.change_readiness_score * Decimal("0.20")
        )
    
    # Already bounded in analysis, but double-check
        overall_score = max(Decimal("0"), min(Decimal("100"), overall_score))
    
        confidence = self._calculate_confidence(signals.review_count)
        rationale = self._generate_rationale(signals)
    
        return CultureScore(
            overall_score=overall_score,
            signals=signals,
            confidence=confidence,
            rationale=rationale
        )
    
    # Helper methods
    def _count_keywords(self, text: str, keywords: List[str]) -> int:
        count = 0
        for keyword in keywords:
            count += len(re.findall(r'\b' + re.escape(keyword) + r'\b', text, re.IGNORECASE))
        return count
    
    def _calculate_score(
        self, 
        keyword_count: int, 
        review_count: int, 
        avg_rating: float, 
        negative_count: int
    ) -> float:
        if review_count == 0:
            return 50.0
        
        # Keyword density
        density = keyword_count / review_count
        base_score = min(70, density * 30)
        
        # Rating boost
        rating_boost = max(0, (avg_rating - 3.0) * 10)
        
        # Negative penalty
        negative_density = negative_count / review_count
        negative_penalty = min(10, negative_density * 20)
        
        score = base_score + rating_boost - negative_penalty
        return max(0, min(100, score))
    
    def _count_individual_mentions(self, text: str) -> int:
        """Count mentions of specific individuals (for talent concentration)."""
        pattern = r'\b[A-Z][a-z]+(?:\'s|\s+is|\s+was|\s+has|\s+does)\b'
        return len(re.findall(pattern, text))
    
    def _calculate_confidence(self, review_count: int) -> Decimal:
        """Confidence based on sample size."""
        if review_count == 0:
            return Decimal("0.1")
        elif review_count < 10:
            return Decimal("0.5")
        elif review_count < 20:
            return Decimal("0.7")
        elif review_count < 40:
            return Decimal("0.85")
        else:
            return Decimal("0.95")
    
    def _generate_rationale(self, signals: CultureSignals) -> str:
        """Generate human-readable rationale."""
        parts = []
        
        if signals.avg_rating >= 4.0:
            parts.append("Strong employee sentiment")
        elif signals.avg_rating >= 3.5:
            parts.append("Positive employee sentiment")
        elif signals.avg_rating >= 3.0:
            parts.append("Mixed employee sentiment")
        else:
            parts.append("Negative employee sentiment")
        
        if signals.innovation_score >= 70:
            parts.append("high innovation culture")
        elif signals.innovation_score >= 50:
            parts.append("moderate innovation")
        else:
            parts.append("low innovation culture")
        
        if signals.data_driven_score >= 70:
            parts.append("strong data-driven culture")
        elif signals.data_driven_score >= 50:
            parts.append("emerging data culture")
        
        if signals.ai_awareness_score >= 70:
            parts.append("high AI awareness")
        
        parts.append(f"{signals.review_count} reviews")
        
        return "; ".join(parts)


# ============================================================================
# MAIN FUNCTION (Used by API endpoints)
# ============================================================================

async def collect_glassdoor_data(ticker: str, use_cache: bool = True) -> Dict:
    """
    Collect and analyze Glassdoor data for a company.
    
    Returns dict with scores AND reviews.
    """
    
    collector = GlassdoorAPICollector()
    
    # Get reviews first
    reviews = await collector.fetch_reviews(ticker, use_cache=use_cache, max_reviews=40)
    
    # Calculate scores from reviews
    culture_score = await collector.calculate_culture_score(ticker, use_cache=use_cache)
    
    # Convert reviews to dict format
    reviews_list = [
        {
            "review_id": r.review_id,
            "rating": r.rating,
            "title": r.title,
            "pros": r.pros,
            "cons": r.cons,
            "advice": r.advice_to_management,
            "is_current_employee": r.is_current_employee,
            "job_title": r.job_title,
            "date": r.review_date.isoformat()
        }
        for r in reviews
    ]
    
    return {
        "ticker": ticker,
        "culture_score": float(culture_score.overall_score),
        "avg_rating": float(culture_score.signals.avg_rating),
        "review_count": culture_score.signals.review_count,
        "confidence": float(culture_score.confidence),
        "innovation_score": float(culture_score.signals.innovation_score),
        "data_driven_score": float(culture_score.signals.data_driven_score),
        "change_readiness_score": float(culture_score.signals.change_readiness_score),
        "ai_awareness_score": float(culture_score.signals.ai_awareness_score),
        "individual_mentions": culture_score.signals.individual_mentions,
        "current_employee_ratio": float(culture_score.signals.current_employee_ratio),
        "rationale": culture_score.rationale,
        "reviews": reviews_list,  # ← ADD THIS!
        "positive_keywords_found": [],
        "negative_keywords_found": []
    }