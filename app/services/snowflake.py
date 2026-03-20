# app/services/snowflake.py
import snowflake.connector
from typing import Optional, List, Dict, Any, Iterable
from uuid import UUID, uuid4
from datetime import datetime, timezone
import json

from app.config import settings
from app.models.signal import ExternalSignal, CompanySignalSummary


class SnowflakeService:
    """Service for Snowflake database operations"""

    def __init__(self):
        self.connection = None

    def _conn_params(self):
        """Connection parameters with keep-alive to avoid token expiry."""
        return {
            "account": settings.SNOWFLAKE_ACCOUNT,
            "user": settings.SNOWFLAKE_USER,
            "password": settings.SNOWFLAKE_PASSWORD,
            "database": settings.SNOWFLAKE_DATABASE,
            "schema": settings.SNOWFLAKE_SCHEMA,
            "warehouse": settings.SNOWFLAKE_WAREHOUSE,
            "client_session_keep_alive": True,  # Prevents 390114 token expiry
        }

    def connect(self):
        """Establish connection to Snowflake"""
        if not self.connection:
            self.connection = snowflake.connector.connect(**self._conn_params())
        return self.connection

    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None

    def _new_connection(self):
        """Fresh connection per request - avoids stale auth tokens (390114)."""
        conn = snowflake.connector.connect(**self._conn_params())
        return conn

    def _lowercase_keys(self, data: List[Dict]) -> List[Dict]:
        """Convert dictionary keys to lowercase for Pydantic compatibility"""
        return [{k.lower(): v for k, v in row.items()} for row in data]

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute SELECT - uses fresh connection to avoid 390114 token expiry."""
        conn = self._new_connection()
        try:
            cursor = conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(query, params or {})
            results = self._lowercase_keys(cursor.fetchall())
            cursor.close()
            return results
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def execute_update(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute INSERT/UPDATE/DELETE - uses fresh connection to avoid 390114."""
        conn = self._new_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or {})
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()
            try:
                conn.close()
            except Exception:
                pass

    async def check_health(self) -> str:
        """Check if Snowflake is accessible"""
        try:
            self.execute_query("SELECT 1")
            return "healthy"
        except Exception as e:
            print(f"Snowflake health check failed: {e}")
            return "unhealthy"

    # ========================================
    # CASE STUDY 2: EXTERNAL SIGNALS
    # ========================================

    def insert_external_signals(self, signals: Iterable[ExternalSignal]) -> int:
        """
        Insert ExternalSignal rows into CS2 external_signals schema.
        Maps:
          jobs   -> technology_hiring
          tech   -> digital_presence
          patents-> innovation_activity

        IMPORTANT FIX:
        Your Snowflake setup rejects/rewrites PARSE_JSON/TRY_PARSE_JSON when the JSON
        string is passed as a bound parameter, causing METADATA to be treated as VARCHAR.
        So we inline the JSON as a SQL string literal (escaped) and call PARSE_JSON('...'),
        which always yields a VARIANT.
        """
        rows = list(signals)
        if not rows:
            return 0

        # Look up ticker for each company_id
        company_ids = list({str(s.company_id) for s in rows})
        ticker_map = {}
        if company_ids:
            placeholders = ",".join(["%s"] * len(company_ids))
            q = f"SELECT id, ticker FROM companies WHERE id IN ({placeholders})"
            conn0 = self._new_connection()
            try:
                cur = conn0.cursor()
                cur.execute(q, company_ids)
                for r in cur.fetchall():
                    ticker_map[str(r[0])] = (r[1] or "").strip().upper()
            finally:
                try:
                    conn0.close()
                except Exception:
                    pass

        def map_category(cat: str) -> str:
            c = (cat or "").lower()
            if c == "jobs":
                return "technology_hiring"
            if c == "tech":
                return "digital_presence"
            if c == "patents":
                return "innovation_activity"
            if c == "technology_hiring":
                return "technology_hiring"
            if c == "digital_presence":
                return "digital_presence"
            if c == "innovation_activity":
                return "innovation_activity"
            if c == "leadership_signals":
                return "leadership_signals"
            # Schema allows only: technology_hiring, innovation_activity, digital_presence, leadership_signals.
            # Safe default: do NOT map unknown to leadership_signals (was causing job postings to be
            # mislabeled). Prefer digital_presence for unknown categories.
            return "digital_presence"

        def _ensure_json_string(val: Any) -> str:
            """
            Return a VALID JSON string.
            - If val is dict/list -> dumps
            - If val is str and valid JSON -> keep
            - Else -> wrap into {"raw": "..."}
            """
            if val is None:
                return "{}"
            if isinstance(val, (dict, list)):
                return json.dumps(val)
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    return "{}"
                try:
                    json.loads(s)
                    return s
                except Exception:
                    return json.dumps({"raw": s})
            return json.dumps({"raw": str(val)})

        def _sql_escape_string_literal(s: str) -> str:
            """
            Escape for single-quoted SQL literal: ' -> ''
            (Snowflake accepts doubled quotes inside single-quoted literals)
            """
            return s.replace("'", "''")

        conn = self._new_connection()
        cursor = conn.cursor()
        inserted = 0

        try:
            for s in rows:
                sid = s.id if (isinstance(s.id, str) and len(s.id) <= 36) else str(uuid4())

                category = s.category.value if hasattr(s.category, "value") else str(s.category)
                source = s.source.value if hasattr(s.source, "value") else str(s.source)

                meta_json = _ensure_json_string(getattr(s, "metadata_json", None))
                meta_sql_literal = _sql_escape_string_literal(meta_json)

                # Inline JSON literal so PARSE_JSON definitely returns VARIANT
                meta_dollar = meta_json.replace("$$", "$ $")
                ticker_val = ticker_map.get(str(s.company_id), "")

                query = f"""
                    INSERT INTO external_signals
                        (id, company_id, category, source, signal_date, raw_value, normalized_score, confidence, metadata, ticker)
                    SELECT
                        %(id)s,
                        %(company_id)s,
                        %(category)s,
                        %(source)s,
                        %(signal_date)s,
                        %(raw_value)s,
                        %(normalized_score)s,
                        %(confidence)s,
                        PARSE_JSON($${meta_dollar}$$),
                        NULLIF(%(ticker)s, '')
                """

                params = {
                    "id": sid,
                    "company_id": str(s.company_id),
                    "ticker": ticker_val,
                    "category": map_category(category),
                    "source": source,
                    "signal_date": s.signal_date.date() if hasattr(s.signal_date, "date") else s.signal_date,
                    "raw_value": s.title or "",
                    "normalized_score": float(s.score),
                    "confidence": 0.8,
                }

                cursor.execute(query, params)
                inserted += 1

            conn.commit()
            return inserted

        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            try:
                conn.close()
            except Exception:
                pass

    def upsert_company_signal_summary(self, summary: CompanySignalSummary, signal_count: int = 0) -> None:
        """
        Upsert into CS2 company_signal_summaries schema.
        Maps:
          jobs_score   -> technology_hiring_score
          tech_score   -> digital_presence_score
          patents_score-> innovation_activity_score
        Also requires ticker (pulled from companies table).
        """
        company = self.get_company(str(summary.company_id))
        ticker = (company or {}).get("ticker") or ""

        query = """
            MERGE INTO company_signal_summaries t
            USING (
                SELECT
                    %(company_id)s AS company_id,
                    %(ticker)s AS ticker,
                    %(technology_hiring_score)s AS technology_hiring_score,
                    %(innovation_activity_score)s AS innovation_activity_score,
                    %(digital_presence_score)s AS digital_presence_score,
                    %(leadership_signals_score)s AS leadership_signals_score,
                    %(composite_score)s AS composite_score,
                    %(signal_count)s AS signal_count
            ) s
            ON t.company_id = s.company_id
            WHEN MATCHED THEN UPDATE SET
                ticker = s.ticker,
                technology_hiring_score = s.technology_hiring_score,
                innovation_activity_score = s.innovation_activity_score,
                digital_presence_score = s.digital_presence_score,
                leadership_signals_score = s.leadership_signals_score,
                composite_score = s.composite_score,
                signal_count = s.signal_count,
                last_updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (company_id, ticker, technology_hiring_score, innovation_activity_score,
                 digital_presence_score, leadership_signals_score, composite_score, signal_count, last_updated)
            VALUES
                (s.company_id, s.ticker, s.technology_hiring_score, s.innovation_activity_score,
                 s.digital_presence_score, s.leadership_signals_score, s.composite_score, s.signal_count, CURRENT_TIMESTAMP())
        """

        params = {
            "company_id": str(summary.company_id),
            "ticker": ticker,
            "technology_hiring_score": float(summary.jobs_score),
            "innovation_activity_score": float(summary.patents_score),
            "digital_presence_score": float(summary.tech_score),
            "leadership_signals_score": float(getattr(summary, "leadership_score", 0)),
            "composite_score": float(summary.composite_score),
            "signal_count": int(signal_count),
        }

        self.execute_update(query, params)

    # ========================================
    # COMPANY CRUD
    # ========================================

    def create_company(self, company_data: Dict) -> str:
        company_id = str(uuid4())
        query = """
            INSERT INTO companies (id, name, ticker, industry_id, position_factor, created_at, updated_at)
            VALUES (%(id)s, %(name)s, %(ticker)s, %(industry_id)s, %(position_factor)s, %(created_at)s, %(updated_at)s)
        """
        now = datetime.now(timezone.utc)
        params = {
            "id": company_id,
            "name": company_data["name"],
            "ticker": company_data.get("ticker"),
            "industry_id": str(company_data["industry_id"]),
            "position_factor": company_data.get("position_factor", 0.0),
            "created_at": now,
            "updated_at": now,
        }
        self.execute_update(query, params)
        return company_id

    def get_company(self, company_id: str) -> Optional[Dict]:
        query = """
            SELECT * FROM companies
            WHERE id = %(id)s AND is_deleted = FALSE
        """
        results = self.execute_query(query, {"id": company_id})
        return results[0] if results else None

    # ✅ NEW: domain lookup for digital presence scraping (with both fixes)
    def get_primary_domain_by_company_id(self, company_id: str) -> Optional[str]:
        """
        Returns the primary website/domain for a company from company_domains.

        Fixes:
        1) Don't require is_primary=TRUE strictly (many rows may be NULL).
        2) Deterministic fallback ordering: prefer primary, else most recently updated/created.
        """
        query = """
            SELECT domain_url
            FROM company_domains
            WHERE company_id = %(company_id)s
              AND (is_primary = TRUE OR is_primary IS NULL)
            ORDER BY is_primary DESC, updated_at DESC, created_at DESC
            LIMIT 1
        """
        rows = self.execute_query(query, {"company_id": company_id})
        return rows[0]["domain_url"] if rows else None

    def get_domain_for_company(self, company_id: str, ticker: str) -> Optional[str]:
        """Get domain: company_domains → yfinance (dynamic, no hardcoded map)."""
        domain = self.get_primary_domain_by_company_id(company_id)
        if domain:
            return self._normalize_domain(domain)
        t = (ticker or "").strip().upper()
        if not t:
            return None
        try:
            import yfinance as yf
            website = (yf.Ticker(t).info or {}).get("website")
            if website:
                return self._normalize_domain(website)
        except Exception:
            pass
        return None

    def _normalize_domain(self, url_or_domain: str) -> str:
        s = (url_or_domain or "").strip()
        if not s:
            return ""
        for p in ("https://", "http://", "www."):
            if s.lower().startswith(p):
                s = s[len(p):]
        return s.split("/")[0].lower() or ""

    def list_companies(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        query = """
            SELECT * FROM companies
            WHERE is_deleted = FALSE
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        return self.execute_query(query, {"limit": limit, "offset": offset})

    def update_company(self, company_id: str, update_data: Dict) -> bool:
        set_clauses = []
        params = {"id": company_id, "updated_at": datetime.now(timezone.utc)}

        for key, value in update_data.items():
            if value is not None:
                set_clauses.append(f"{key} = %({key})s")
                params[key] = str(value) if isinstance(value, UUID) else value

        if not set_clauses:
            return False

        set_clauses.append("updated_at = %(updated_at)s")

        query = f"""
            UPDATE companies
            SET {', '.join(set_clauses)}
            WHERE id = %(id)s AND is_deleted = FALSE
        """
        rows = self.execute_update(query, params)
        return rows > 0

    def delete_company(self, company_id: str) -> bool:
        query = """
            UPDATE companies
            SET is_deleted = TRUE, updated_at = %(updated_at)s
            WHERE id = %(id)s
        """
        rows = self.execute_update(
            query,
            {"id": company_id, "updated_at": datetime.now(timezone.utc)},
        )
        return rows > 0

    # ========================================
    # INDUSTRIES
    # ========================================

    def get_industry(self, industry_id: str) -> Optional[Dict]:
        query = "SELECT * FROM industries WHERE id = %(id)s"
        results = self.execute_query(query, {"id": industry_id})
        return results[0] if results else None

    def list_industries(self) -> List[Dict]:
        query = "SELECT * FROM industries ORDER BY name"
        return self.execute_query(query)

    # ========================================
    # ASSESSMENTS CRUD
    # ========================================

    def create_assessment(self, assessment_data: Dict) -> str:
        assessment_id = str(uuid4())

        assessment_type = assessment_data["assessment_type"]
        if hasattr(assessment_type, "value"):
            assessment_type = assessment_type.value

        query = """
            INSERT INTO assessments (
                id, company_id, assessment_type, assessment_date,
                primary_assessor, secondary_assessor, status, created_at
            )
            VALUES (
                %(id)s, %(company_id)s, %(assessment_type)s, %(assessment_date)s,
                %(primary_assessor)s, %(secondary_assessor)s, %(status)s, %(created_at)s
            )
        """

        params = {
            "id": assessment_id,
            "company_id": str(assessment_data["company_id"]),
            "assessment_type": assessment_type,
            "assessment_date": assessment_data["assessment_date"],
            "primary_assessor": assessment_data.get("primary_assessor"),
            "secondary_assessor": assessment_data.get("secondary_assessor"),
            "status": "draft",
            "created_at": datetime.now(timezone.utc),
        }

        self.execute_update(query, params)
        return assessment_id

    def get_assessment(self, assessment_id: str) -> Optional[Dict]:
        query = "SELECT * FROM assessments WHERE id = %(id)s"
        results = self.execute_query(query, {"id": assessment_id})
        return results[0] if results else None

    def list_assessments(
        self,
        limit: int = 10,
        offset: int = 0,
        company_id: Optional[str] = None,
    ) -> List[Dict]:
        where_clause = "WHERE 1=1"
        params: Dict[str, Any] = {"limit": limit, "offset": offset}

        if company_id:
            where_clause += " AND company_id = %(company_id)s"
            params["company_id"] = company_id

        query = f"""
            SELECT * FROM assessments
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        return self.execute_query(query, params)

    def update_assessment_status(self, assessment_id: str, status: str) -> bool:
        query = """
            UPDATE assessments
            SET status = %(status)s
            WHERE id = %(id)s
        """
        rows = self.execute_update(query, {"id": assessment_id, "status": status})
        return rows > 0

    # ========================================
    # DIMENSION SCORES CRUD
    # ========================================

    def create_dimension_score(self, score_data: Dict) -> str:
        score_id = str(uuid4())

        dimension = score_data["dimension"]
        if hasattr(dimension, "value"):
            dimension = dimension.value

        query = """
            INSERT INTO dimension_scores (
                id, assessment_id, dimension, score, weight,
                confidence, evidence_count, created_at
            )
            VALUES (
                %(id)s, %(assessment_id)s, %(dimension)s, %(score)s, %(weight)s,
                %(confidence)s, %(evidence_count)s, %(created_at)s
            )
        """
        params = {
            "id": score_id,
            "assessment_id": str(score_data["assessment_id"]),
            "dimension": dimension,
            "score": score_data["score"],
            "weight": score_data.get("weight"),
            "confidence": score_data.get("confidence", 0.8),
            "evidence_count": score_data.get("evidence_count", 0),
            "created_at": datetime.now(timezone.utc),
        }
        self.execute_update(query, params)
        return score_id

    def get_dimension_scores(self, assessment_id: str) -> List[Dict]:
        query = """
            SELECT * FROM dimension_scores
            WHERE assessment_id = %(assessment_id)s
            ORDER BY dimension
        """
        return self.execute_query(query, {"assessment_id": assessment_id})

    def get_dimension_score(self, score_id: str) -> Optional[Dict]:
        query = "SELECT * FROM dimension_scores WHERE id = %(id)s"
        results = self.execute_query(query, {"id": score_id})
        return results[0] if results else None

    def update_dimension_score(self, score_id: str, update_data: Dict) -> bool:
        set_clauses = []
        params: Dict[str, Any] = {"id": score_id}

        for key, value in update_data.items():
            if value is not None:
                if hasattr(value, "value"):
                    value = value.value
                set_clauses.append(f"{key} = %({key})s")
                params[key] = value

        if not set_clauses:
            return False

        query = f"""
            UPDATE dimension_scores
            SET {', '.join(set_clauses)}
            WHERE id = %(id)s
        """
        rows = self.execute_update(query, params)
        return rows > 0

    def delete_dimension_score(self, score_id: str) -> bool:
        query = "DELETE FROM dimension_scores WHERE id = %(id)s"
        rows = self.execute_update(query, {"id": score_id})
        return rows > 0

    def delete_dimension_score_by_assessment_and_dimension(self, assessment_id: str, dimension: str) -> bool:
        """
        Delete by natural key (assessment_id, dimension), matching UNIQUE constraint.
        """
        query = """
            DELETE FROM dimension_scores
            WHERE assessment_id = %(assessment_id)s AND dimension = %(dimension)s
        """
        rows = self.execute_update(query, {"assessment_id": assessment_id, "dimension": dimension})
        return rows > 0

    
# ========================================
    # CASE STUDY 2: DOCUMENTS PIPELINE HELPERS
    # ========================================

    def list_documents_by_status(self, status: str, limit: int = 200) -> List[Dict]:
        query = """
            SELECT id, ticker, filing_type, s3_key, status
            FROM documents_sec
            WHERE status = %(status)s
            ORDER BY created_at ASC
            LIMIT %(limit)s
        """
        return self.execute_query(query, {"status": status, "limit": limit})

    def mark_document_cleaned(self, document_id: str) -> None:
        self.execute_update(
            """
            UPDATE documents_sec
            SET status = 'cleaned',
                processed_at = CURRENT_TIMESTAMP(),
                error_message = NULL
            WHERE id = %(id)s
            """,
            {"id": document_id},
        )

    def mark_document_error(self, document_id: str, message: str) -> None:
        # keep message size safe-ish
        msg = (message or "")[:2000]
        self.execute_update(
            """
            UPDATE documents_sec
            SET status = 'error',
                error_message = %(msg)s,
                processed_at = CURRENT_TIMESTAMP()
            WHERE id = %(id)s
            """,
            {"id": document_id, "msg": msg},
        )

# Global instance
db = SnowflakeService()