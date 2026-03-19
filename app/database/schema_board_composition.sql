-- Board Governance Signals & Board Members tables
-- Mirrors the culture_signals / glassdoor_reviews pattern from CS3

CREATE TABLE IF NOT EXISTS board_governance_signals (
    id VARCHAR(36) NOT NULL,
    company_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    governance_score DECIMAL(5,2) NOT NULL,
    has_tech_committee BOOLEAN DEFAULT FALSE,
    has_ai_expertise BOOLEAN DEFAULT FALSE,
    has_data_officer BOOLEAN DEFAULT FALSE,
    has_independent_majority BOOLEAN DEFAULT FALSE,
    has_risk_tech_oversight BOOLEAN DEFAULT FALSE,
    has_ai_strategy BOOLEAN DEFAULT FALSE,
    ai_experts TEXT,
    evidence TEXT,
    confidence DECIMAL(4,3) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS board_members (
    id VARCHAR(36) NOT NULL,
    company_id VARCHAR(36) NOT NULL,
    governance_signal_id VARCHAR(36) NOT NULL,
    name VARCHAR(300) NOT NULL,
    title VARCHAR(300),
    committees TEXT,
    bio TEXT,
    is_independent BOOLEAN DEFAULT FALSE,
    tenure_years DECIMAL(4,1) DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
