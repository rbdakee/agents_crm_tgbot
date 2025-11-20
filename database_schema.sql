-- Force UTF8 client encoding for Windows psql
SET client_encoding = 'UTF8';

-- Schema for properties table (CRM real estate)
-- Merged data from two Google Sheets

-- Recreate table (warning: will drop data!)
DROP TABLE IF EXISTS properties CASCADE;

CREATE TABLE properties (
    -- Base fields (from SHEET_DEALS, read-only)
    crm_id VARCHAR(50) PRIMARY KEY,
    date_signed DATE,
    contract_number VARCHAR(100),
    mop VARCHAR(100),
    rop VARCHAR(100),
    dd VARCHAR(100),
    client_name TEXT,
    address TEXT,
    complex VARCHAR(200),
    contract_price BIGINT,
    expires DATE,
    
    -- Extra fields (from SHEET_PROGRESS, editable)
    category VARCHAR(100),
    area DOUBLE PRECISION,
    rooms_count INTEGER,
    krisha_price BIGINT,
    vitrina_price BIGINT,
    score DOUBLE PRECISION,
    collage BOOLEAN DEFAULT FALSE,
    prof_collage BOOLEAN DEFAULT FALSE,
    krisha TEXT,
    instagram TEXT,
    tiktok TEXT,
    mailing TEXT,
    stream TEXT,
    shows INTEGER DEFAULT 0,
    analytics BOOLEAN DEFAULT FALSE,
    price_update TEXT,
    provide_analytics BOOLEAN DEFAULT FALSE,
    push_for_price BOOLEAN DEFAULT FALSE,
    status VARCHAR(100) DEFAULT 'Размещено',
    
    -- Sync metadata
    last_modified_by VARCHAR(10) DEFAULT 'SHEET', -- 'BOT' or 'SHEET'
    last_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_properties_agent ON properties(mop, rop, dd);
CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status);
CREATE INDEX IF NOT EXISTS idx_properties_expires ON properties(expires);
CREATE INDEX IF NOT EXISTS idx_properties_modified ON properties(last_modified_at);

-- Comments (ASCII only to avoid Windows codepage issues)
COMMENT ON TABLE properties IS 'Merged real estate table from two Google Sheets';
COMMENT ON COLUMN properties.crm_id IS 'Unique CRM identifier (PRIMARY KEY)';
COMMENT ON COLUMN properties.last_modified_by IS 'Last change source: BOT or SHEET';
COMMENT ON COLUMN properties.last_modified_at IS 'Last change timestamp';
COMMENT ON COLUMN properties.created_at IS 'Creation timestamp';
