# app/pipelines/patent_signals.py
"""
Patent Signal Collection Pipeline - Innovation Activity

Complete pipeline for:
1. Fetching companies from Snowflake
2. Collecting patents from USPTO API
3. Calculating scores
4. Inserting signals into Snowflake
5. Updating company summaries

Usage:
    from app.pipelines.patent_signals import PatentSignalPipeline
    
    pipeline = PatentSignalPipeline()
    await pipeline.run_for_all_companies()
"""

from typing import List, Dict, Optional
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import httpx
import structlog
import json
from app.config import settings
from app.services.snowflake import SnowflakeService

logger = structlog.get_logger()


_SUFFIXES = frozenset({"corporation", "inc", "llc", "ltd", "co", "company", "incorporated"})


def _search_term_for_assignee(name: str) -> str:
    """
    Derive patent assignee search term from company name.
    - Strip corporate suffixes (Corporation, Inc., LLC, etc.)
    - Use 1-2 words: Tesla, Dollar General, General Electric
    """
    s = (name or "").strip()
    if not s:
        return s
    parts = s.split(",")[0].strip().split()
    while parts and parts[-1].lower().rstrip(".") in _SUFFIXES:
        parts.pop()
    if not parts:
        return s
    if len(parts) == 1:
        return parts[0]
    return " ".join(parts[:2])


def _resolve_assignee_name(ticker: str, company_name: str) -> str:
    """
    Resolve company name for patent assignee search.
    When DB has name=ticker, fetch from yfinance. Then derive search term
    (1-2 words, no suffix) to match assignee variants.
    """
    name = (company_name or "").strip()
    t = (ticker or "").strip().upper()
    if name and name.upper() != t:
        resolved = name
    elif t:
        try:
            import yfinance as yf
            info = (yf.Ticker(t).info or {})
            resolved = info.get("shortName") or info.get("longName") or name or t
        except Exception:
            resolved = name or t
    else:
        resolved = name or "Unknown"
    # Patents are often assigned to operating subs (e.g. Google LLC), not holding company name.
    if t in ("GOOGL", "GOOG"):
        return "Google"
    search = _search_term_for_assignee(resolved)
    return search if len(search) >= 2 else (resolved or "Unknown")


class PatentSignalCollector:
    """Patent signal collector using CPC code filtering."""
    
    # API BASE URL
    API_BASE = "https://search.patentsview.org/api/v1/patent/"
    
    # AI/ML CPC Codes (G06N is primary AI code)
    AI_CPC_CODES = [
        "G06N",      # Computing arrangements based on specific computational models
        "G06F18",    # Pattern recognition
        "G06V10",    # Image or video recognition
        "G10L15"     # Speech recognition
    ]
    
    # AI Keywords
    AI_KEYWORDS = [
        "artificial intelligence", "machine learning", "deep learning",
        "neural network", "natural language processing", "nlp",
        "computer vision", "reinforcement learning", "supervised learning",
        "unsupervised learning", "predictive analytics", "data mining",
        "pattern recognition", "knowledge graph", "semantic search",
        "recommendation system", "anomaly detection", "sentiment analysis",
        "speech recognition", "image recognition", "object detection",
        "generative model", "transformer", "attention mechanism",
        "convolutional network", "recurrent network", "autoencoder",
        "gan", "generative adversarial"
    ]
    
    # Patent Categories
    AI_CATEGORIES = {
        "ml_core": [
            "machine learning", "deep learning", "neural network",
            "supervised learning", "unsupervised learning", "reinforcement learning"
        ],
        "nlp": [
            "natural language processing", "nlp", "sentiment analysis",
            "text mining", "semantic search", "language model"
        ],
        "computer_vision": [
            "computer vision", "image recognition", "object detection",
            "facial recognition", "image processing"
        ],
        "predictive": [
            "predictive analytics", "forecasting", "anomaly detection",
            "pattern recognition"
        ],
        "automation": [
            "robotics", "autonomous systems", "process automation",
            "intelligent agents"
        ]
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize collector."""
        self.logger = logger.bind(component="patent_signals")
        self.api_key = api_key or settings.uspto_api_key
        
        if not self.api_key:
            self.logger.warning("No USPTO API key configured")
    
    async def scrape_patents(
        self,
        company_name: str,
        years: int = 5,
        max_results: int = 1000  # Get up to 1000 patents total
    ) -> List[Dict]:
        """
        Scrape AI/ML patents with pagination to get ALL results.
        
        Uses cursor-based pagination to retrieve all available patents,
        not just the first 100.
        """
        
        if not self.api_key:
            raise ValueError("USPTO API key required")
        
        self.logger.info(
            "Scraping USPTO patents with pagination",
            company_name=company_name,
            years=years
        )
        
        start_date_str = "2021-01-01"
        all_patents = []
        page_num = 1
        last_patent_id = None
        
        while len(all_patents) < max_results:
            # Build query with pagination
            query = {
                "q": {
                    "_and": [
                        # _contains = partial match - works for any company (no hardcoding)
                        {"_contains": {"assignees.assignee_organization": company_name}},
                        {
                            "_begins": {
                                "cpc_current.cpc_group_id": "G06N"
                            }
                        },
                        {
                            "_gte": {
                                "patent_date": start_date_str
                            }
                        }
                    ]
                },
                "f": [
                    "patent_id",
                    "patent_title",
                    "patent_date",
                    "patent_abstract",
                    "assignees",
                    "cpc_current"
                ],
                "s": [{"patent_id": "asc"}],  # Sort for consistent pagination
                "o": {
                    "size": 100  # PatentSearch API uses 'size' (max 1000), not 'per_page'
                }
            }
            
            # Add cursor pagination if not first page
            if last_patent_id:
                query["o"]["after"] = last_patent_id
            
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    headers = {
                        "X-Api-Key": self.api_key,
                        "Content-Type": "application/json"
                    }
                    
                    response = await client.post(
                        self.API_BASE,
                        json=query,
                        headers=headers
                    )
                    
                    if response.status_code != 200:
                        self.logger.error(
                            "HTTP error",
                            status=response.status_code,
                            page=page_num
                        )
                        break
                    
                    data = response.json()
                    patent_data = data.get("patents", [])
                    total_hits = data.get("total_hits", 0)
                    
                    if not patent_data:
                        # No more results
                        break
                    
                    self.logger.info(
                        f"Page {page_num} retrieved",
                        patents_this_page=len(patent_data),
                        total_so_far=len(all_patents) + len(patent_data),
                        total_available=total_hits
                    )
                    
                    # Parse patents from this page
                    for patent in patent_data:
                        assignees_list = patent.get("assignees", [])
                        assignee_name = assignees_list[0].get("assignee_organization", company_name) if assignees_list and isinstance(assignees_list, list) else company_name
                        
                        cpc_list = patent.get("cpc_current", [])
                        cpc_codes = []
                        if isinstance(cpc_list, list):
                            for cpc in cpc_list:
                                if isinstance(cpc, dict):
                                    cpc_id = cpc.get("cpc_group_id")
                                    if cpc_id:
                                        cpc_codes.append(cpc_id)
                        if not cpc_codes:
                            cpc_codes = ["G06N"]
                        
                        all_patents.append({
                            "patent_number": patent.get("patent_id"),
                            "title": patent.get("patent_title", ""),
                            "abstract": patent.get("patent_abstract", ""),
                            "filing_date": self._parse_date(patent.get("patent_date")),
                            "assignee": assignee_name,
                            "cpc_codes": cpc_codes
                        })
                    
                    # Check if we got everything
                    if len(all_patents) >= total_hits:
                        self.logger.info(
                            "✅ All patents retrieved",
                            total=len(all_patents)
                        )
                        break
                    
                    # Set cursor for next page
                    last_patent_id = patent_data[-1].get("patent_id")
                    page_num += 1
                    
                    # Safety limit
                    if page_num > 10:  # Max 10 pages = 1000 patents
                        self.logger.warning(
                            "Reached pagination limit",
                            retrieved=len(all_patents)
                        )
                        break
                    
            except Exception as e:
                self.logger.error(
                    "Pagination failed",
                    page=page_num,
                    error=str(e)
                )
                break
        
        self.logger.info(
            "Patent scraping complete",
            company=company_name,
            total_patents=len(all_patents)
        )
        
        return all_patents

    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            return None
    
    def classify_patent(self, patent: Dict) -> Dict:
        """Classify and categorize an AI patent."""
        title = patent.get("title", "").lower()
        abstract = patent.get("abstract", "").lower()
        combined_text = f"{title} {abstract}"
        cpc_codes = patent.get("cpc_codes", [])
        
        is_ai = True  # Already CPC filtered
        
        found_keywords = [kw for kw in self.AI_KEYWORDS if kw in combined_text]
        
        categories = set()
        for category, keywords in self.AI_CATEGORIES.items():
            if any(kw in combined_text for kw in keywords):
                categories.add(category)
        
        # Categorize by CPC
        if cpc_codes:
            for cpc in cpc_codes:
                cpc_str = str(cpc).upper()
                if cpc_str.startswith("G06N"):
                    categories.add("ml_core")
                elif cpc_str.startswith("G06V"):
                    categories.add("computer_vision")
                elif cpc_str.startswith("G10L"):
                    categories.add("nlp")
                elif cpc_str.startswith("G06F18"):
                    categories.add("predictive")
        
        return {
            "is_ai": is_ai,
            "categories": list(categories) if categories else ["ml_core"],
            "keywords_found": found_keywords,
            "cpc_codes": cpc_codes,
            "confidence": 1.0
        }
    
    async def collect_signals(
        self,
        company_id: str,
        company_name: str,
        years: int = 10,
        patents: Optional[List[Dict]] = None
    ) -> Dict:
        """Collect and analyze patent signals."""
        
        self.logger.info(
            "Collecting patent signals",
            company_id=company_id,
            company_name=company_name
        )
        
        if patents is None:
            patents = await self.scrape_patents(company_name, years)
        
        if not patents:
            self.logger.warning("No patents found", company_id=company_id)
            return self._create_empty_signal(company_id, years)
        
        # Classify all patents
        ai_patents = []
        all_categories = set()
        one_year_ago = datetime.now() - timedelta(days=365)
        recent_ai_patents = []
        
        for patent in patents:
            classification = self.classify_patent(patent)
            patent_data = {**patent, "classification": classification}
            ai_patents.append(patent_data)
            all_categories.update(classification["categories"])
            
            filing_date = patent.get("filing_date")
            if filing_date and isinstance(filing_date, datetime):
                if filing_date >= one_year_ago:
                    recent_ai_patents.append(patent_data)
        
        # Calculate scores
        patent_count_score = min(len(ai_patents) * 5, 50)
        recency_score = min(len(recent_ai_patents) * 2, 20)
        category_score = min(len(all_categories) * 10, 30)
        normalized_score = patent_count_score + recency_score + category_score
        
        # Maturity
        if normalized_score >= 80:
            maturity = "AI Innovator"
        elif normalized_score >= 60:
            maturity = "AI Developer"
        elif normalized_score >= 30:
            maturity = "AI Experimenter"
        else:
            maturity = "Limited AI Innovation"
        
        # Metadata
        metadata = {
            "total_patents": len(patents),
            "ai_patents": len(ai_patents),
            "recent_ai_patents": len(recent_ai_patents),
            "categories": list(all_categories),
            "category_count": len(all_categories),
            "years_analyzed": years,
            "cpc_filtered": True,
            "cpc_codes_used": ["G06N"],  # Only using G06N for now
            "score_breakdown": {
                "patent_count": patent_count_score,
                "recency": recency_score,
                "diversity": category_score
            },
            "maturity_level": maturity,
            "sample_patents": [
                {
                    "number": p["patent_number"],
                    "title": p["title"][:100],
                    "categories": p["classification"]["categories"],
                    "cpc_codes": p.get("cpc_codes", [])[:3]
                }
                for p in ai_patents[:5]
            ]
        }
        
        self.logger.info(
            "Patent signal collected",
            company_id=company_id,
            score=normalized_score,
            ai_patents=len(ai_patents),
            recent=len(recent_ai_patents),
            categories=len(all_categories)
        )
        
        return {
            "id": str(uuid4()),
            "company_id": company_id,
            "category": "innovation_activity",
            "source": "uspto",
            "signal_date": datetime.now(),
            "raw_value": f"{len(ai_patents)} AI patents in {years} years",
            "normalized_score": normalized_score,
            "confidence": 0.95,
            "metadata": metadata
        }
    
    def _create_empty_signal(self, company_id: str, years: int) -> Dict:
        """Create empty signal for companies with no patents."""
        return {
            "id": str(uuid4()),
            "company_id": company_id,
            "category": "innovation_activity",
            "source": "uspto",
            "signal_date": datetime.now(),
            "raw_value": f"0 AI patents in {years} years",
            "normalized_score": 0.0,
            "confidence": 1.0,
            "metadata": {
                "total_patents": 0,
                "ai_patents": 0,
                "recent_ai_patents": 0,
                "categories": [],
                "category_count": 0,
                "years_analyzed": years,
                "cpc_filtered": True,
                "cpc_codes_used": ["G06N"],
                "score_breakdown": {
                    "patent_count": 0,
                    "recency": 0,
                    "diversity": 0
                },
                "maturity_level": "No AI Innovation",
                "sample_patents": []
            }
        }


class PatentSignalPipeline:
    """
    Complete pipeline for collecting patent signals and storing in Snowflake.
    
    This class orchestrates:
    1. Fetching companies from Snowflake
    2. Collecting patents via PatentSignalCollector
    3. Inserting signals into external_signals table
    4. Updating company_signal_summaries table
    """
    
    def __init__(self, years: int = 5):
        self.db = SnowflakeService()
        self.collector = PatentSignalCollector()
        self.years = years
        self.results = {
            "successful": [],
            "failed": [],
            "skipped": []
        }
        self.logger = logger.bind(component="patent_pipeline")
    
    def get_companies_from_snowflake(self) -> List[Dict]:
        """Fetch all companies from Snowflake."""
        self.logger.info("Fetching companies from Snowflake")
        
        query = """
            SELECT id, name, ticker, industry_id
            FROM companies
            WHERE is_deleted = FALSE
            ORDER BY ticker
        """
        
        companies = self.db.execute_query(query)
        self.logger.info("Companies fetched", count=len(companies))
        return companies
    
    def filter_companies_with_names(self, companies: List[Dict]) -> List[Dict]:
        """Filter companies that have a name (used for assignee _contains search)."""
        filtered = [c for c in companies if c.get('name', '').strip()]
        self.logger.info(
            "Filtered companies with names",
            total=len(companies),
            filtered=len(filtered)
        )
        return filtered
    
    async def collect_signal_for_company(self, company: Dict) -> Optional[Dict]:
        """Collect patent signal for a single company (resolves assignee name via yfinance when name=ticker)."""
        ticker = (company.get('ticker') or '').strip().upper()
        company_id = company['id']
        company_name = (company.get('name') or '').strip()
        assignee_name = _resolve_assignee_name(ticker, company_name)
        
        self.logger.info(
            "Processing company",
            ticker=ticker,
            assignee_name=assignee_name
        )
        
        try:
            signal = await self.collector.collect_signals(
                company_id=company_id,
                company_name=assignee_name,
                years=self.years
            )
            
            return signal
            
        except Exception as e:
            self.logger.error(
                "Failed to collect signal",
                ticker=ticker,
                error=str(e)
            )
            return None
    
    def insert_signal_to_snowflake(self, signal: Dict, ticker: str) -> bool:
        """Insert signal into external_signals table."""
        try:
            query = """
                INSERT INTO external_signals (
                    id, company_id, category, source, signal_date,
                    raw_value, normalized_score, confidence, metadata, created_at
                )
                SELECT 
                    %(id)s, %(company_id)s, %(category)s, %(source)s, %(signal_date)s,
                    %(raw_value)s, %(normalized_score)s, %(confidence)s, 
                    PARSE_JSON(%(metadata)s), %(created_at)s
            """
            
            params = {
                'id': signal['id'],
                'company_id': signal['company_id'],
                'category': signal['category'],
                'source': signal['source'],
                'signal_date': signal['signal_date'].date(),
                'raw_value': signal['raw_value'],
                'normalized_score': signal['normalized_score'],
                'confidence': signal['confidence'],
                'metadata': json.dumps(signal['metadata']),
                'created_at': datetime.now(timezone.utc)
            }
            
            self.db.execute_update(query, params)
            self.logger.info("Signal inserted", ticker=ticker)
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to insert signal",
                ticker=ticker,
                error=str(e)
            )
            return False
    
    def update_company_summary(self, company_id: str, ticker: str, innovation_score: float) -> bool:
        """Update or insert company_signal_summaries."""
        try:
            # Check if summary exists
            check_query = """
                SELECT company_id FROM company_signal_summaries
                WHERE company_id = %(company_id)s
            """
            existing = self.db.execute_query(check_query, {'company_id': company_id})
            
            if existing:
                # Update existing
                query = """
                    UPDATE company_signal_summaries
                    SET innovation_activity_score = %(score)s,
                        signal_count = signal_count + 1,
                        last_updated = %(last_updated)s,
                        composite_score = (
                            COALESCE(technology_hiring_score, 0) * 0.30 +
                            %(score)s * 0.25 +
                            COALESCE(digital_presence_score, 0) * 0.25 +
                            COALESCE(leadership_signals_score, 0) * 0.20
                        )
                    WHERE company_id = %(company_id)s
                """
            else:
                # Insert new
                query = """
                    INSERT INTO company_signal_summaries (
                        company_id, ticker, innovation_activity_score,
                        composite_score, signal_count, last_updated
                    )
                    VALUES (
                        %(company_id)s, %(ticker)s, %(score)s,
                        %(score)s * 0.25, 1, %(last_updated)s
                    )
                """
            
            params = {
                'company_id': company_id,
                'ticker': ticker,
                'score': innovation_score,
                'last_updated': datetime.now(timezone.utc)
            }
            
            self.db.execute_update(query, params)
            self.logger.info("Company summary updated", ticker=ticker)
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to update summary",
                ticker=ticker,
                error=str(e)
            )
            return False
    
    async def process_company(self, company: Dict) -> Dict:
        """Process a single company: collect, insert, update."""
        ticker = company['ticker'].upper()
        company_id = company['id']
        
        result = {
            'ticker': ticker,
            'company_name': company['name'],
            'success': False,
            'score': None,
            'error': None
        }
        
        # Collect signal
        signal = await self.collect_signal_for_company(company)
        
        if not signal:
            result['error'] = "Failed to collect signal"
            self.results['failed'].append(result)
            return result
        
        result['score'] = signal['normalized_score']
        
        # Insert into external_signals
        if not self.insert_signal_to_snowflake(signal, ticker):
            result['error'] = "Failed to insert signal"
            self.results['failed'].append(result)
            return result
        
        # Update company_signal_summaries
        if not self.update_company_summary(company_id, ticker, signal['normalized_score']):
            result['error'] = "Failed to update summary"
            self.results['failed'].append(result)
            return result
        
        result['success'] = True
        self.results['successful'].append(result)
        return result
    
    async def run_for_all_companies(self, ticker_filter: Optional[str] = None) -> Dict:
        """
        Run the complete pipeline for all companies.
        
        Args:
            ticker_filter: Optional ticker to process only one company
            
        Returns:
            Dictionary with results summary
        """
        self.logger.info(
            "Starting patent signal pipeline",
            years=self.years,
            ticker_filter=ticker_filter
        )
        
        # Fetch companies
        all_companies = self.get_companies_from_snowflake()
        target_companies = self.filter_companies_with_names(all_companies)
        
        # Apply ticker filter if provided
        if ticker_filter:
            target_companies = [
                c for c in target_companies 
                if c['ticker'].upper() == ticker_filter.upper()
            ]
            if not target_companies:
                self.logger.error("No company found", ticker=ticker_filter)
                return {"error": f"No company found with ticker: {ticker_filter}"}
        
        self.logger.info(
            "Processing companies",
            count=len(target_companies),
            tickers=[c['ticker'] for c in target_companies]
        )
        
        # Process each company
        for i, company in enumerate(target_companies, 1):
            self.logger.info(
                "Processing company",
                progress=f"{i}/{len(target_companies)}",
                ticker=company['ticker']
            )
            await self.process_company(company)
            
            # Small delay to be nice to the API
            if i < len(target_companies):
                import asyncio
                await asyncio.sleep(2)
        
        # Cleanup
        self.db.close()
        
        # Return summary
        return {
            "total": len(target_companies),
            "successful": len(self.results['successful']),
            "failed": len(self.results['failed']),
            "results": self.results
        }
    
    async def run_for_single_company(self, ticker: str) -> Dict:
        """
        Run pipeline for a single company by ticker.
        
        Args:
            ticker: Company ticker symbol (e.g., 'WMT')
            
        Returns:
            Dictionary with result
        """
        return await self.run_for_all_companies(ticker_filter=ticker)
    
    # ============================================================================
# ADAPTER FUNCTIONS - Convert to Friend's Interface
# ============================================================================

from dataclasses import dataclass
from app.models.signal import ExternalSignal, SignalCategory, SignalSource, CompanySignalSummary


@dataclass(frozen=True)
class PatentSignalInput:
    """Mock patent input - for orchestrator compatibility"""
    title: str
    description: str
    company: str
    url: Optional[str] = None
    filing_date: Optional[str] = None


def scrape_patent_signal_inputs_mock(company: str) -> List[PatentSignalInput]:
    """Mock function for orchestrator compatibility"""
    return []


def patent_inputs_to_signals(company_id: str, items: List[PatentSignalInput]) -> List[ExternalSignal]:
    """Convert patent inputs to ExternalSignal - for orchestrator compatibility"""
    return []


def aggregate_patent_signals(company_id: str, patent_signals: List[ExternalSignal]) -> CompanySignalSummary:
    """Aggregate patent signals - for orchestrator compatibility"""
    if not patent_signals:
        patents_score = 0
    else:
        from statistics import mean
        patents_score = int(round(mean(s.score for s in patent_signals)))
    
    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=0,
        tech_score=0,
        patents_score=patents_score,
        leadership_score=0,
        composite_score=0
    )


async def collect_patent_signals_real(
    company_id: str,
    company_name: str,
    years: int = 5,
    ticker: Optional[str] = None,
) -> List[ExternalSignal]:
    """
    REAL patent collection - Returns ExternalSignal objects.
    Uses _contains match on assignee name (resolves via yfinance when DB has name=ticker).
    """
    collector = PatentSignalCollector()
    assignee_name = _resolve_assignee_name(ticker or "", company_name)
    
    # Get patent data (assignee_name used for _contains search)
    patent_data = await collector.collect_signals(
        company_id=company_id,
        company_name=assignee_name,
        years=years
    )
    
    # Convert to friend's ExternalSignal model
    signal = ExternalSignal(
        id=patent_data['id'],
        company_id=company_id,
        category=SignalCategory.INNOVATION_ACTIVITY,
        source=SignalSource.external,
        signal_date=patent_data['signal_date'],
        score=int(patent_data['normalized_score']),
        title=patent_data['raw_value'],
        url=None,
        metadata_json=json.dumps(patent_data['metadata'])
    )
    
    return [signal]
