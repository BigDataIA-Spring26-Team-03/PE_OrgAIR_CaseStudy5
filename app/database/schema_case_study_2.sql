-- =========================================================
-- Case Study 3 - SEC Documents Schema
-- =========================================================

-- ----------------------------
-- Documents table
-- ----------------------------
CREATE TABLE IF NOT EXISTS documents_sec (
  id VARCHAR(36) PRIMARY KEY,
  company_id VARCHAR(36) NOT NULL REFERENCES companies(id),
  ticker VARCHAR(10) NOT NULL,
  filing_type VARCHAR(20) NOT NULL,
  filing_date DATE NOT NULL,
  source_url VARCHAR(500),
  local_path VARCHAR(500),
  s3_key VARCHAR(500),
  content_hash VARCHAR(64),
  word_count INT,
  chunk_count INT,
  status VARCHAR(20) DEFAULT 'pending',
  error_message VARCHAR(1000),
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  processed_at TIMESTAMP_NTZ,
  -- Add CHECK constraint separately
  CONSTRAINT check_status CHECK (status IN ('pending','downloaded','parsed','chunked','indexed','failed'))
);

-- ----------------------------
-- Document chunks table
-- ----------------------------
CREATE TABLE IF NOT EXISTS document_chunks_sec (
  id VARCHAR(36) PRIMARY KEY,
  document_id VARCHAR(36) NOT NULL,
  chunk_index INT NOT NULL,
  content TEXT NOT NULL,
  section VARCHAR(50),
  start_char INT,
  end_char INT,
  word_count INT,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  -- Fixed foreign key reference
  FOREIGN KEY (document_id) REFERENCES documents_sec(id),
  -- Unique constraint
  UNIQUE (document_id, chunk_index)
);

-- ----------------------------
-- Indexes for performance
-- ----------------------------
CREATE INDEX IF NOT EXISTS idx_documents_sec_company 
  ON documents_sec(company_id);

CREATE INDEX IF NOT EXISTS idx_documents_sec_status 
  ON documents_sec(status);

CREATE INDEX IF NOT EXISTS idx_documents_sec_ticker 
  ON documents_sec(ticker);

CREATE INDEX IF NOT EXISTS idx_documents_sec_filing_type 
  ON documents_sec(filing_type);

CREATE INDEX IF NOT EXISTS idx_chunks_sec_document 
  ON document_chunks_sec(document_id);


-- ----------------------------
-- External signals table
-- ----------------------------
CREATE TABLE IF NOT EXISTS external_signals (
  id VARCHAR(36) PRIMARY KEY,
  company_id VARCHAR(36) NOT NULL REFERENCES companies(id),
  category VARCHAR(30) NOT NULL
    CHECK (category IN ('technology_hiring','innovation_activity','digital_presence','leadership_signals')),
  source VARCHAR(30) NOT NULL,
  signal_date DATE NOT NULL,
  raw_value VARCHAR(500),
  normalized_score DECIMAL(5,2)
    CHECK (normalized_score BETWEEN 0 AND 100),
  confidence DECIMAL(4,3)
    CHECK (confidence BETWEEN 0 AND 1),
  metadata VARIANT,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------
-- Company signal summaries table
-- ----------------------------
CREATE TABLE IF NOT EXISTS company_signal_summaries (
  company_id VARCHAR(36) PRIMARY KEY REFERENCES companies(id),
  ticker VARCHAR(10) NOT NULL,
  technology_hiring_score DECIMAL(5,2),
  innovation_activity_score DECIMAL(5,2),
  digital_presence_score DECIMAL(5,2),
  leadership_signals_score DECIMAL(5,2),
  composite_score DECIMAL(5,2),
  signal_count INT,
  last_updated TIMESTAMP_NTZ
);


-- These clustering keys improve pruning on common filters.
ALTER TABLE documents_sec CLUSTER BY (company_id, status, filing_type, filing_date);
ALTER TABLE document_chunks_sec CLUSTER BY (document_id, chunk_index);
ALTER TABLE external_signals CLUSTER BY (company_id, category, signal_date);
ALTER TABLE company_signal_summaries CLUSTER BY (company_id);
