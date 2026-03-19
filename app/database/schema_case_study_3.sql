CREATE TABLE IF NOT EXISTS culture_signals (
    id VARCHAR(36) NOT NULL,
    company_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    innovation_score DECIMAL(5,2) NOT NULL,
    data_driven_score DECIMAL(5,2) NOT NULL,
    change_readiness_score DECIMAL(5,2) NOT NULL,
    ai_awareness_score DECIMAL(5,2) NOT NULL,
    overall_score DECIMAL(5,2) NOT NULL,
    review_count INT NOT NULL,
    avg_rating DECIMAL(3,2) NOT NULL,
    current_employee_ratio DECIMAL(4,3) NOT NULL,
    confidence DECIMAL(4,3) NOT NULL,
    positive_keywords_found TEXT,
    negative_keywords_found TEXT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS glassdoor_reviews (
    id VARCHAR(36) NOT NULL,
    company_id VARCHAR(36) NOT NULL,
    culture_signal_id VARCHAR(36) NOT NULL,
    review_id VARCHAR(100) NOT NULL,
    rating DECIMAL(2,1) NOT NULL,
    title VARCHAR(500),
    pros TEXT,
    cons TEXT,
    advice_to_management TEXT,
    is_current_employee BOOLEAN,
    job_title VARCHAR(200),
    review_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);