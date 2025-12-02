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

-- Parsed supply objects from rbd.kz
CREATE TABLE IF NOT EXISTS parsed_properties (
    vitrina_id BIGSERIAL PRIMARY KEY,
    rbd_id BIGINT UNIQUE NOT NULL,
    krisha_id VARCHAR(64),
    krisha_date TIMESTAMPTZ,
    object_type VARCHAR(255),
    address TEXT,
    complex VARCHAR(255),
    builder VARCHAR(255),
    flat_type VARCHAR(255),
    property_class VARCHAR(255),
    condition VARCHAR(255),
    sell_price DOUBLE PRECISION,
    sell_price_per_m2 DOUBLE PRECISION,
    address_type VARCHAR(255),
    house_num VARCHAR(255),
    floor_num INTEGER,
    floor_count INTEGER,
    room_count INTEGER,
    phones VARCHAR(255),
    description TEXT,
    ceiling_height DOUBLE PRECISION,
    area DOUBLE PRECISION,
    year_built INTEGER,
    wall_type VARCHAR(255),
    stats_agent_given VARCHAR(255),
    stats_time_given TIMESTAMPTZ,
    stats_object_status VARCHAR(255),
    stats_recall_time TIMESTAMPTZ,
    stats_description TEXT,
    stats_object_category VARCHAR(10),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_parsed_properties_created ON parsed_properties(created_at);

-- Миграция: добавление новой колонки stats_object_category (если таблица уже существует)
ALTER TABLE parsed_properties ADD COLUMN IF NOT EXISTS stats_object_category VARCHAR(10);

-- Оптимизированные индексы для parsed_properties
CREATE INDEX IF NOT EXISTS idx_parsed_properties_krisha_id 
ON parsed_properties(krisha_id) 
WHERE krisha_id IS NOT NULL AND krisha_id != '';

CREATE INDEX IF NOT EXISTS idx_parsed_properties_agent_given 
ON parsed_properties(stats_agent_given) 
WHERE stats_agent_given IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_parsed_properties_status 
ON parsed_properties(stats_object_status) 
WHERE stats_object_status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_parsed_properties_recall_time 
ON parsed_properties(stats_recall_time) 
WHERE stats_recall_time IS NOT NULL;

-- Составной индекс для уведомлений о перезвоне
CREATE INDEX IF NOT EXISTS idx_parsed_properties_recall_notification 
ON parsed_properties(stats_object_status, stats_recall_time, stats_agent_given) 
WHERE stats_object_status = 'Перезвонить' AND stats_recall_time IS NOT NULL AND stats_agent_given IS NOT NULL;

-- Составной индекс для get_latest_parsed_properties
CREATE INDEX IF NOT EXISTS idx_parsed_properties_latest 
ON parsed_properties(krisha_id, stats_agent_given, krisha_date DESC) 
WHERE krisha_id IS NOT NULL AND krisha_id != '';

-- Индекс для сортировки по stats_time_given
CREATE INDEX IF NOT EXISTS idx_parsed_properties_time_given 
ON parsed_properties(stats_time_given DESC NULLS LAST);

-- Составной индекс для get_my_new_parsed_properties
CREATE INDEX IF NOT EXISTS idx_parsed_properties_my_objects 
ON parsed_properties(stats_agent_given, stats_time_given DESC NULLS LAST, vitrina_id DESC) 
WHERE stats_agent_given IS NOT NULL;

-- Индекс для архивации
CREATE INDEX IF NOT EXISTS idx_parsed_properties_archive 
ON parsed_properties(stats_object_status, krisha_id) 
WHERE krisha_id IS NOT NULL AND krisha_id != '' 
  AND (stats_object_status IS NULL OR stats_object_status != 'Архив');

-- Дополнительные индексы для properties
CREATE INDEX IF NOT EXISTS idx_properties_category 
ON properties(category) 
WHERE category IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_properties_mop_lower 
ON properties(LOWER(mop)) 
WHERE mop IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_properties_rop_lower 
ON properties(LOWER(rop)) 
WHERE rop IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_properties_dd_lower 
ON properties(LOWER(dd)) 
WHERE dd IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_properties_client_name 
ON properties(LOWER(client_name)) 
WHERE client_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_properties_agent_category 
ON properties(mop, rop, dd, category) 
WHERE category IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_properties_modified_at 
ON properties(last_modified_at DESC);

-- Индекс для фильтрации по property_class в parsed_properties
CREATE INDEX IF NOT EXISTS idx_parsed_properties_property_class 
ON parsed_properties(property_class) 
WHERE property_class IS NOT NULL;

-- Таблица агентов витрины: ФИО, телефон, chat_ids (массив TEXT), роль и настройки фильтров
CREATE TABLE IF NOT EXISTS vitrina_agents (
    agent_phone VARCHAR(255) PRIMARY KEY,
    full_name TEXT,
    chat_ids TEXT[],  -- Массив chat_id: {'123456', '789012', ...} (поддержка отрицательных и начинающихся с нуля)
    role VARCHAR(50),
    property_classes TEXT[],
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для поиска по chat_id в массиве
CREATE INDEX IF NOT EXISTS idx_vitrina_agents_chat_ids 
ON vitrina_agents USING GIN (chat_ids);