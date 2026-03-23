# streamlit_app/api_client.py
"""
Complete API Client for PE Org-AI-R Platform
Supports CS1 (Platform Foundation) + CS2 (Evidence Collection)
"""

import os
import requests
from typing import Optional, Dict, List

class APIClient:
    """Client for communicating with FastAPI backend"""
    
    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or os.getenv("API_BASE_URL", "http://localhost:8000")).rstrip("/")
    
    def _handle_response(self, response):
        """Handle API response and errors"""
        if response.status_code >= 400:
            try:
                error = response.json()
                raise Exception(f"API Error: {error.get('detail', response.text)}")
            except:
                raise Exception(f"API Error {response.status_code}: {response.text}")
        return response
    
    # ========================================
    # HEALTH
    # ========================================
    def get_health(self) -> Dict:
        """Get system health"""
        response = requests.get(f"{self.base_url}/health")
        data = response.json()
        if 'detail' in data and isinstance(data['detail'], dict):
            return data['detail']
        return data
    
    # ========================================
    # INDUSTRIES
    # ========================================
    def get_industries(self) -> List[Dict]:
        """Get available industries"""
        response = requests.get(f"{self.base_url}/api/v1/companies/available-industries")
        data = self._handle_response(response).json()
        return data.get('items', data) if isinstance(data, dict) else data
    
    # ========================================
    # COMPANIES (CS1)
    # ========================================
    def list_companies(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        """List companies"""
        response = requests.get(
            f"{self.base_url}/api/v1/companies",
            params={"limit": limit, "offset": offset}
        )
        return self._handle_response(response).json()
    
    def get_company(self, company_id: str) -> Dict:
        """Get single company"""
        response = requests.get(f"{self.base_url}/api/v1/companies/{company_id}")
        return self._handle_response(response).json()
    
    def create_company(self, data: Dict) -> Dict:
        """Create company"""
        response = requests.post(
            f"{self.base_url}/api/v1/companies",
            json=data
        )
        return self._handle_response(response).json()
    
    def update_company(self, company_id: str, data: Dict) -> Dict:
        """Update company"""
        response = requests.put(
            f"{self.base_url}/api/v1/companies/{company_id}",
            json=data
        )
        return self._handle_response(response).json()
    
    def delete_company(self, company_id: str) -> bool:
        """Delete company"""
        response = requests.delete(f"{self.base_url}/api/v1/companies/{company_id}")
        return response.status_code == 204
    
    # ========================================
    # ASSESSMENTS (CS1)
    # ========================================
    def list_assessments(
        self, 
        limit: int = 50, 
        offset: int = 0,
        company_id: Optional[str] = None
    ) -> List[Dict]:
        """List assessments"""
        params = {"limit": limit, "offset": offset}
        if company_id:
            params["company_id"] = company_id
        
        response = requests.get(
            f"{self.base_url}/api/v1/assessments",
            params=params
        )
        return self._handle_response(response).json()
    
    def get_assessment(self, assessment_id: str) -> Dict:
        """Get single assessment"""
        response = requests.get(f"{self.base_url}/api/v1/assessments/{assessment_id}")
        return self._handle_response(response).json()
    
    def create_assessment(self, data: Dict) -> Dict:
        """Create assessment"""
        response = requests.post(
            f"{self.base_url}/api/v1/assessments",
            json=data
        )
        return self._handle_response(response).json()
    
    def update_assessment_status(self, assessment_id: str, status: str) -> Dict:
        """Update assessment status"""
        response = requests.patch(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/status",
            json={"status": status}
        )
        return self._handle_response(response).json()
    
    # ========================================
    # DIMENSION SCORES (CS1)
    # ========================================
    def get_dimension_scores(self, assessment_id: str) -> List[Dict]:
        """Get dimension scores for assessment"""
        response = requests.get(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/scores"
        )
        return self._handle_response(response).json()
    
    def create_dimension_score(self, assessment_id: str, data: Dict) -> Dict:
        """Create dimension score"""
        response = requests.post(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/scores",
            json=data
        )
        return self._handle_response(response).json()
    
    def update_dimension_score(self, score_id: str, data: Dict) -> Dict:
        """Update dimension score"""
        response = requests.put(
            f"{self.base_url}/api/v1/scores/{score_id}",
            json=data
        )
        return self._handle_response(response).json()
    
    def delete_dimension_score(self, assessment_id: str, dimension: str) -> bool:
        """Delete dimension score"""
        response = requests.delete(
            f"{self.base_url}/api/v1/assessments/{assessment_id}/scores/{dimension}"
        )
        return response.status_code == 204
    
    # ========================================
    # EXTERNAL SIGNALS (CS2)
    # ========================================
    def collect_all_signals(self, ticker: str, years: int = 5, job_location: str = "United States") -> Dict:
        """Trigger collection of ALL 4 signal types"""
        response = requests.post(
            f"{self.base_url}/api/v1/signals/collect/{ticker}",
            params={"years": years, "job_location": job_location}
        )
        return self._handle_response(response).json()
    
    def collect_patents_only(self, ticker: str, years: int = 5) -> Dict:
        """Trigger patent collection only"""
        response = requests.post(
            f"{self.base_url}/api/v1/signals/collect/patents/{ticker}",
            params={"years": years}
        )
        return self._handle_response(response).json()
    
    def get_signals_by_ticker(self, ticker: str) -> Dict:
        """Get all signals for a company"""
        response = requests.get(
            f"{self.base_url}/api/v1/signals/company/{ticker}"
        )
        return self._handle_response(response).json()
    
    def get_signal_summary(self, ticker: str) -> Dict:
        """Get signal summary for a company"""
        response = requests.get(
            f"{self.base_url}/api/v1/signals/summary/{ticker}"
        )
        return self._handle_response(response).json()
    
    def get_all_signal_summaries(self) -> Dict:
        """Get signal summaries for all companies"""
        response = requests.get(
            f"{self.base_url}/api/v1/signals/summary"
        )
        return self._handle_response(response).json()
    
    # ========================================
    # DOCUMENTS (CS2)
    # ========================================
    def collect_documents(
        self,
        ticker: str,
        filing_types: List[str] = ["10-K", "10-Q", "8-K"],
        limit_per_type: int = 1,
        steps: List[str] = ["download", "parse", "clean", "chunk"]
    ) -> Dict:
        """Trigger SEC document collection"""
        response = requests.post(
            f"{self.base_url}/api/v1/documents/collect",
            json={
                "ticker": ticker,
                "filing_types": filing_types,
                "limit_per_type": limit_per_type,
                "steps": steps
            }
        )
        return self._handle_response(response).json()
    
    def list_documents(
        self,
        ticker: Optional[str] = None,
        filing_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict:
        """List SEC documents"""
        params = {"limit": limit, "offset": offset}
        if ticker:
            params["ticker"] = ticker
        if filing_type:
            params["filing_type"] = filing_type
        if status:
            params["status"] = status
        
        response = requests.get(
            f"{self.base_url}/api/v1/documents",
            params=params
        )
        return self._handle_response(response).json()
    
    def get_document(self, doc_id: str) -> Dict:
        """Get single document"""
        response = requests.get(
            f"{self.base_url}/api/v1/documents/{doc_id}"
        )
        return self._handle_response(response).json()
    
    def get_document_chunks(self, doc_id: str, limit: int = 100, offset: int = 0) -> Dict:
        """Get chunks for a document"""
        response = requests.get(
            f"{self.base_url}/api/v1/documents/{doc_id}/chunks",
            params={"limit": limit, "offset": offset}
        )
        return self._handle_response(response).json()
    
    def trigger_scoring_for_company(self, ticker: str, sector: str) -> dict:
        """
        Trigger Org-AI-R scoring for a single company.
    
        This calls the integration service directly (bypasses Airflow).
        """
    # Option 1: Direct integration service call (faster)
        response = self._request(
            "POST",
            f"/api/v1/scoring/score/{ticker}",
            params={"sector": sector}
        )
        return response

    def get_scoring_status(self, ticker: str) -> dict:
        """Check if scoring results exist for a company."""
        try:
            response = self._request("GET", f"/api/v1/scoring/results/{ticker}")
            return response
        except:
            return {"status": "not_found"}
        

    def search_evidence(self, query: str, company_id: str = None,
                    dimension: str = None, top_k: int = 10,
                    min_confidence: float = 0.0,
                    source_types: list = None) -> list:
        """Search evidence using hybrid search."""
        params = {"query": query, "top_k": top_k, "min_confidence": min_confidence}
        if company_id:
            params["company_id"] = company_id
        if dimension:
            params["dimension"] = dimension
        if source_types:
            params["source_types"] = source_types
        response = requests.get(f"{self.base_url}/api/v1/search", params=params)
        return self._handle_response(response).json()

    def get_justification(self, ticker: str, dimension: str) -> dict:
        """Get score justification for a company dimension."""
        response = requests.get(
            f"{self.base_url}/api/v1/justification/{ticker}/{dimension}",
            timeout=60
        )
        return self._handle_response(response).json()

    def get_assessment_history(self, ticker: str, days: int = 365) -> dict:
        """Get assessment history and trend for a company."""
        response = requests.get(
            f"{self.base_url}/api/v1/assessment-history/{ticker.upper()}",
            params={"days": days},
            timeout=30,
        )
        return self._handle_response(response).json()

    def get_ic_prep(self, ticker: str, focus_dimensions: list = None) -> dict:
        """Get full IC meeting preparation package."""
        payload = {}
        if focus_dimensions:
            payload["focus_dimensions"] = focus_dimensions
        response = requests.post(
            f"{self.base_url}/api/v1/justification/{ticker}/ic-prep",
            json=payload,
            timeout=300
        )
        return self._handle_response(response).json()

    # ========================================
    # ANALYST NOTES (CS4)
    # ========================================
    def seed_evidence(self, ticker: str) -> Dict:
        """Fetch evidence for a ticker from Snowflake and index into the search retriever."""
        response = requests.post(
            f"{self.base_url}/api/v1/search/seed/{ticker}",
            timeout=120
        )
        return self._handle_response(response).json()

    def submit_interview(self, ticker: str, payload: Dict) -> Dict:
        response = requests.post(
            f"{self.base_url}/api/v1/analyst-notes/{ticker}/interview",
            json=payload, timeout=30
        )
        return self._handle_response(response).json()

    def submit_dd_finding(self, ticker: str, payload: Dict) -> Dict:
        response = requests.post(
            f"{self.base_url}/api/v1/analyst-notes/{ticker}/dd-finding",
            json=payload, timeout=30
        )
        return self._handle_response(response).json()

    def submit_data_room(self, ticker: str, payload: Dict) -> Dict:
        response = requests.post(
            f"{self.base_url}/api/v1/analyst-notes/{ticker}/data-room",
            json=payload, timeout=30
        )
        return self._handle_response(response).json()

    def submit_management_meeting(self, ticker: str, payload: Dict) -> Dict:
        response = requests.post(
            f"{self.base_url}/api/v1/analyst-notes/{ticker}/management-meeting",
            json=payload, timeout=30
        )
        return self._handle_response(response).json()

    def submit_site_visit(self, ticker: str, payload: Dict) -> Dict:
        response = requests.post(
            f"{self.base_url}/api/v1/analyst-notes/{ticker}/site-visit",
            json=payload, timeout=30
        )
        return self._handle_response(response).json()