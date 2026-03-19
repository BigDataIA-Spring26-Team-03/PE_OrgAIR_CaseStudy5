from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum
from decimal import Decimal
import re
import logging

logger = logging.getLogger(__name__)


class ScoreLevel(Enum):
    """5-level scoring system for rubrics."""
    LEVEL_5 = (80, 100, "Excellent")
    LEVEL_4 = (60, 79, "Good")
    LEVEL_3 = (40, 59, "Adequate")
    LEVEL_2 = (20, 39, "Developing")
    LEVEL_1 = (0, 19, "Nascent")
    
    @property
    def min_score(self) -> int:
        return self.value[0]
    
    @property
    def max_score(self) -> int:
        return self.value[1]
    
    @property
    def label(self) -> str:
        return self.value[2]


@dataclass
class RubricCriteria:
    """Criteria for a single rubric level."""
    level: ScoreLevel
    keywords: List[str]
    min_keyword_matches: int
    quantitative_threshold: float
    description: str = ""


@dataclass
class RubricResult:
    """Result of rubric scoring."""
    dimension: str
    level: ScoreLevel
    score: Decimal
    matched_keywords: List[str]
    keyword_match_count: int
    confidence: Decimal
    rationale: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "dimension": self.dimension,
            "level": self.level.value[2],
            "score": float(self.score),
            "matched_keywords": self.matched_keywords,
            "keyword_match_count": self.keyword_match_count,
            "confidence": float(self.confidence),
            "rationale": self.rationale
        }



#RUBRIC DEFINITIONS FOR ALL 7 DIMENSIONS

DIMENSION_RUBRICS: Dict[str, Dict[ScoreLevel, RubricCriteria]] = {
    
    # DATA INFRASTRUCTURE
    "data_infrastructure": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # Tech keywords:
                "snowflake", "databricks", "data lake", "real-time", "streaming",
                "cloud-based", "data analytics", "advanced analytics",
                "integrated systems", "digital infrastructure", "analytics platform",
                "data management", "technology infrastructure", "digital transformation"
            ],
            min_keyword_matches=2, 
            quantitative_threshold=0.80,
            description="Modern cloud platform with analytics capabilities"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "cloud", "hybrid", "data quality", "analytics", "warehouse",
                "information systems", "it systems", "technology systems",
                "data processing", "infrastructure investments", "digital"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.60,
            description="Hybrid cloud environment with data capabilities"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "systems", "infrastructure", "technology", "data",
                "modernization", "upgrade", "digital", "information technology",
                "it infrastructure", "technology investments"
            ],
            min_keyword_matches=1,  
            quantitative_threshold=0.40,
            description="Basic systems with modernization underway"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "legacy systems", "aging infrastructure", "outdated",
                "manual processes", "system limitations", "legacy"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.20,
            description="Legacy systems with limitations"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "limited technology", "basic systems", "minimal infrastructure",
                "manual", "fragmented"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="Minimal infrastructure"
        ),
    },
    
    # AI GOVERNANCE
    "ai_governance": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # Explicit AI governance:
                "caio", "chief data officer", "cdo", "ai oversight",
                "risk management framework", "governance structure",
                "board oversight", "compliance framework", "risk committee",
                "model governance", "data governance", "regulatory compliance"
            ],
            min_keyword_matches=2,  # Lowered from 3
            quantitative_threshold=0.80,
            description="Strong governance with board oversight"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "risk management", "governance", "oversight", "compliance",
                "controls", "policies and procedures", "regulatory",
                "internal controls", "risk framework"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.60,
            description="Established risk management and governance"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "risk", "governance", "oversight", "compliance",
                "policies", "procedures", "controls", "management"
            ],
            min_keyword_matches=1, 
            quantitative_threshold=0.40,
            description="Basic governance policies in place"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "informal", "ad-hoc", "limited oversight",
                "minimal controls", "developing policies"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.20,
            description="Informal governance structure"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no governance", "no oversight", "unmanaged",
                "no policies", "no controls"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="No formal governance"
        ),
    },
    
    # TALENT
    "talent": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # Tech talent:
                "data scientists", "machine learning", "ai specialists",
                "ml engineers", "ai research",
                "technical expertise", "specialized skills", "talent acquisition",
                "highly skilled", "engineering talent", "technology professionals",
                "technical capabilities", "skilled workforce"
            ],
            min_keyword_matches=2,  
            quantitative_threshold=0.40,
            description="Strong technical talent and expertise"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "talent", "employees", "workforce", "professionals",
                "skilled", "expertise", "technical", "engineering",
                "recruitment", "hiring", "capabilities"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.25,
            description="Skilled workforce with technical capabilities"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "employees", "workforce", "personnel", "staff",
                "human capital", "labor", "talent", "team"
            ],
            min_keyword_matches=1, 
            quantitative_threshold=0.15,
            description="Basic workforce in place"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "limited", "turnover", "contractors", "vendors",
                "shortage", "challenges"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.05,
            description="Limited talent with challenges"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no dedicated", "dependent on", "vendor only",
                "minimal", "no expertise"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="No dedicated technical talent"
        ),
    },
    
    # TECHNOLOGY STACK
    "technology_stack": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # Explicit ML/AI:
                "machine learning", "artificial intelligence", "ai",
                "predictive analytics", "mlops",
                "proprietary technology", "technology platforms",
                "advanced capabilities", "automation", "algorithmic",
                "data science", "analytics tools", "digital solutions"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.80,
            description="Advanced technology with AI/ML capabilities"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "technology", "platforms", "systems", "tools",
                "software", "applications", "digital", "automation",
                "analytics", "capabilities", "solutions"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.60,
            description="Modern technology platforms and tools"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "technology", "systems", "software", "applications",
                "digital", "tools", "it", "capabilities"
            ],
            min_keyword_matches=1, 
            quantitative_threshold=0.40,
            description="Basic technology systems"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "manual", "spreadsheet", "basic", "limited",
                "no automation", "minimal tools"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.20,
            description="Manual processes with limited tools"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no technology", "manual only", "no tools",
                "no capabilities", "minimal"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="No technology capabilities"
        ),
    },
    
    # LEADERSHIP
    "leadership": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                # Explicit:
                "ceo", "board", "strategic priority", "transformation",
                "strategic initiatives", "digital initiatives",
                "technology investments", "innovation strategy",
                "executive management", "board of directors",
                "strategic focus", "digital strategy"
            ],
            min_keyword_matches=2, 
            quantitative_threshold=0.80,
            description="Strong executive leadership and strategy"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "strategy", "strategic", "initiatives", "leadership",
                "management", "executive", "board", "governance",
                "direction", "vision", "priorities"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.60,
            description="Strategic management with clear direction"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "management", "executive", "leadership", "strategy",
                "operations", "oversight", "direction"
            ],
            min_keyword_matches=1, 
            quantitative_threshold=0.40,
            description="Basic management structure"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "limited", "developing", "emerging", "new",
                "transitioning", "evolving"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.20,
            description="Limited strategic direction"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no strategy", "no leadership", "unclear",
                "no direction", "absent"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="No clear leadership"
        ),
    },
    
    # USE CASE PORTFOLIO
    "use_case_portfolio": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "revenue growth", "roi", "return on investment",
                "products and services", "solutions", "offerings",
                "revenue-generating", "market opportunity",
                "business model", "competitive advantage",
                "value proposition", "monetize"
            ],
            min_keyword_matches=2, 
            quantitative_threshold=0.80,
            description="Strong portfolio generating revenue"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "products", "services", "solutions", "offerings",
                "business", "operations", "customers", "market",
                "revenue", "growth", "value"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.60,
            description="Established products and services"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "business", "operations", "products", "services",
                "customers", "market", "offerings"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.40,
            description="Basic business operations"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "limited", "developing", "pilot", "proof of concept",
                "early stage", "testing"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.20,
            description="Early stage offerings"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "no products", "exploring", "research",
                "no revenue", "pre-launch"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="No established offerings"
        ),
    },
    
    # CULTURE
    "culture": {
        ScoreLevel.LEVEL_5: RubricCriteria(
            level=ScoreLevel.LEVEL_5,
            keywords=[
                "innovation", "innovative", "culture",
                "data-driven", "entrepreneurial",
                "values", "employee engagement", "diversity",
                "inclusion", "workplace", "talent retention"
            ],
            min_keyword_matches=2,  
            quantitative_threshold=0.80,
            description="Innovation culture with engagement"
        ),
        ScoreLevel.LEVEL_4: RubricCriteria(
            level=ScoreLevel.LEVEL_4,
            keywords=[
                "culture", "values", "employees", "workplace",
                "engagement", "diversity", "inclusion",
                "talent", "retention"
            ],
            min_keyword_matches=2,
            quantitative_threshold=0.60,
            description="Positive workplace culture"
        ),
        ScoreLevel.LEVEL_3: RubricCriteria(
            level=ScoreLevel.LEVEL_3,
            keywords=[
                "employees", "workforce", "workplace", "culture",
                "values", "team", "organization"
            ],
            min_keyword_matches=1,  
            quantitative_threshold=0.40,
            description="Basic organizational culture"
        ),
        ScoreLevel.LEVEL_2: RubricCriteria(
            level=ScoreLevel.LEVEL_2,
            keywords=[
                "challenges", "turnover", "resistance",
                "hierarchical", "traditional"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.20,
            description="Cultural challenges present"
        ),
        ScoreLevel.LEVEL_1: RubricCriteria(
            level=ScoreLevel.LEVEL_1,
            keywords=[
                "hostile", "siloed", "no culture",
                "dysfunctional", "fragmented"
            ],
            min_keyword_matches=1,
            quantitative_threshold=0.0,
            description="Poor organizational culture"
        ),
    },
}


# RUBRIC SCORER CLASS


class RubricScorer:
    """Score evidence against PE Org-AI-R rubrics with SEC-optimized keywords."""
    
    def __init__(self):
        """Initialize with rubric definitions."""
        self.rubrics = DIMENSION_RUBRICS
        logger.info(f"RubricScorer initialized with {len(self.rubrics)} dimension rubrics")
    
    def score_dimension(
        self,
        dimension: str,
        evidence_text: str,
        quantitative_metrics: Optional[Dict[str, float]] = None
    ) -> RubricResult:
        if quantitative_metrics is None:
            quantitative_metrics = {}
        
        # Normalize text
        text = evidence_text.lower()
        
        # Get rubric for dimension
        rubric = self.rubrics.get(dimension, {})
        if not rubric:
            logger.warning(f"No rubric found for dimension: {dimension}")
            return self._default_result(dimension)
        
        # Check each level from 5 to 1
        for level in [ScoreLevel.LEVEL_5, ScoreLevel.LEVEL_4, ScoreLevel.LEVEL_3, 
                      ScoreLevel.LEVEL_2, ScoreLevel.LEVEL_1]:
            
            criteria = rubric.get(level)
            if not criteria:
                continue
            
            # Count keyword matches
            matched_keywords = []
            for keyword in criteria.keywords:
                # Use word boundaries for better matching
                pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
                if re.search(pattern, text):
                    matched_keywords.append(keyword)
            
            match_count = len(matched_keywords)
            
            # Check if criteria met
            meets_keyword_threshold = match_count >= criteria.min_keyword_matches
            
            # Check quantitative threshold (if metric provided)
            meets_quantitative = True
            if quantitative_metrics:
                metric_value = list(quantitative_metrics.values())[0] if quantitative_metrics else 0
                meets_quantitative = metric_value >= criteria.quantitative_threshold
            
            if meets_keyword_threshold and meets_quantitative:
                # Interpolate score within level range
                min_score = level.min_score
                max_score = level.max_score
                range_size = max_score - min_score
                
                # Calculate match ratio (beyond minimum)
                extra_matches = match_count - criteria.min_keyword_matches
                max_possible_extra = len(criteria.keywords) - criteria.min_keyword_matches
                
                if max_possible_extra > 0:
                    match_ratio = extra_matches / max_possible_extra
                else:
                    match_ratio = 0.5  # Default to middle
                
                # Interpolate within range
                score = Decimal(str(min_score + (range_size * match_ratio)))
                
                # Boost if exceeds quantitative threshold significantly
                if quantitative_metrics:
                    metric_value = list(quantitative_metrics.values())[0]
                    if metric_value > criteria.quantitative_threshold * 1.5:
                        score += Decimal("5")
                
                # Clamp to level range
                score = max(Decimal(str(min_score)), min(Decimal(str(max_score)), score))
                
                # Calculate confidence based on evidence strength
                confidence = self._calculate_confidence(match_count, len(evidence_text), quantitative_metrics)
                
                rationale = (
                    f"Level {level.value[2]}: Matched {match_count}/{len(criteria.keywords)} keywords. "
                    f"Keywords: {', '.join(matched_keywords[:5])}{'...' if len(matched_keywords) > 5 else ''}. "
                )
                
                if quantitative_metrics:
                    rationale += f"Quantitative metrics: {quantitative_metrics}."
                
                logger.info(f"Scored {dimension} at {level.label}: {float(score):.2f}")
                
                return RubricResult(
                    dimension=dimension,
                    level=level,
                    score=score,
                    matched_keywords=matched_keywords,
                    keyword_match_count=match_count,
                    confidence=confidence,
                    rationale=rationale
                )
        
        # No criteria met - try fallback generic scoring
        generic_score = self._calculate_generic_score(text)
        
        if generic_score >= 40:
            # Use generic score with moderate confidence
            return RubricResult(
                dimension=dimension,
                level=ScoreLevel.LEVEL_3,
                score=Decimal(str(generic_score)),
                matched_keywords=["generic business terms"],
                keyword_match_count=0,
                confidence=Decimal("0.5"),
                rationale=f"Scored using generic term frequency: {generic_score:.1f}/100 (no specific rubric keywords matched)"
            )
        
        # Fallback to default
        logger.warning(f"No rubric criteria met for {dimension}, defaulting to Level 1")
        return self._default_result(dimension)
    
    def _calculate_generic_score(self, text: str) -> float:
        # Count occurrences of broad terms
        tech_terms = ["technology", "digital", "data", "analytics", "systems",
                     "platform", "software", "cloud", "automation", "innovation"]
        
        business_terms = ["strategy", "strategic", "growth", "revenue", "efficiency",
                         "operations", "customers", "market", "competitive", "value"]
        
        risk_terms = ["risk", "governance", "compliance", "oversight", "controls"]
        
        tech_count = sum(1 for term in tech_terms if term in text)
        business_count = sum(1 for term in business_terms if term in text)
        risk_count = sum(1 for term in risk_terms if term in text)
        
        total_count = tech_count + business_count + risk_count
        
        # Score based on mention frequency
        if total_count >= 20:
            return min(95, 70 + (total_count - 20) * 1.5)  # 70-95
        elif total_count >= 10:
            return 55 + (total_count - 10) * 1.5  # 55-70
        elif total_count >= 5:
            return 40 + (total_count - 5) * 3.0   # 40-55
        else:
            return 20 + total_count * 4.0         # 20-40
    
    def _calculate_confidence(
        self,
        match_count: int,
        text_length: int,
        quantitative_metrics: Dict[str, float]
    ) -> Decimal:
        """Calculate confidence score based on evidence quality."""
        # Base confidence from keyword matches
        if match_count >= 5:
            confidence = Decimal("0.90")
        elif match_count >= 3:
            confidence = Decimal("0.75")
        elif match_count >= 2:
            confidence = Decimal("0.65")
        else:
            confidence = Decimal("0.50")
        
        # Adjust for text length
        if text_length > 10000:
            confidence *= Decimal("1.1")
        elif text_length < 1000:
            confidence *= Decimal("0.9")
        
        # Boost if quantitative metrics available
        if quantitative_metrics:
            confidence *= Decimal("1.1")
        
        # Clamp to [0.3, 1.0]
        return max(Decimal("0.3"), min(Decimal("1.0"), confidence))
    
    def _default_result(self, dimension: str) -> RubricResult:
        """Return default result when no criteria met."""
        return RubricResult(
            dimension=dimension,
            level=ScoreLevel.LEVEL_1,
            score=Decimal("15"),
            matched_keywords=[],
            keyword_match_count=0,
            confidence=Decimal("0.3"),
            rationale="No rubric criteria met. Limited or no evidence found."
        )
    
    def score_all_dimensions(
        self,
        evidence_by_dimension: Dict[str, str],
        metrics_by_dimension: Optional[Dict[str, Dict[str, float]]] = None
    ) -> Dict[str, RubricResult]:
        """Score all 7 dimensions."""
        if metrics_by_dimension is None:
            metrics_by_dimension = {}
        
        results = {}
        
        for dimension in self.rubrics.keys():
            evidence = evidence_by_dimension.get(dimension, "")
            metrics = metrics_by_dimension.get(dimension, {})
            
            if not evidence:
                logger.warning(f"No evidence provided for {dimension}")
                results[dimension] = self._default_result(dimension)
            else:
                results[dimension] = self.score_dimension(dimension, evidence, metrics)
        
        logger.info(f"Scored all {len(results)} dimensions")
        return results



# HELPER FUNCTIONS


def concatenate_evidence_chunks(chunks: List[str], max_length: int = 50000) -> str:
    """Concatenate evidence chunks into single text."""
    result = []
    current_length = 0
    
    for chunk in chunks:
        if current_length + len(chunk) > max_length:
            break
        result.append(chunk)
        current_length += len(chunk)
    
    return " ".join(result)


def extract_quantitative_metrics(
    dimension: str,
    evidence_metadata: Dict
) -> Dict[str, float]:
    """Extract quantitative metrics from evidence metadata."""
    metrics = {}
    
    if dimension == "talent":
        if "ai_job_ratio" in evidence_metadata:
            metrics["ai_job_ratio"] = evidence_metadata["ai_job_ratio"]
        if "team_size" in evidence_metadata:
            metrics["team_size"] = evidence_metadata["team_size"]
    
    elif dimension == "use_case_portfolio":
        if "production_cases" in evidence_metadata:
            metrics["production_cases"] = evidence_metadata["production_cases"]
        if "roi_multiple" in evidence_metadata:
            metrics["roi_multiple"] = evidence_metadata["roi_multiple"]
    
    elif dimension == "culture":
        if "avg_rating" in evidence_metadata:
            metrics["culture_score"] = (evidence_metadata["avg_rating"] - 1) / 4
    
    return metrics