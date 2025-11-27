-- Оптимизация индексов для улучшения производительности БД
-- Этот файл можно выполнить на существующей БД без потери данных

-- ============================================
-- Индексы для таблицы parsed_properties
-- ============================================

-- Индекс для поиска по krisha_id (используется в get_latest_parsed_properties)
CREATE INDEX IF NOT EXISTS idx_parsed_properties_krisha_id 
ON parsed_properties(krisha_id) 
WHERE krisha_id IS NOT NULL AND krisha_id != '';

-- Индекс для поиска по stats_agent_given (часто используется в WHERE)
CREATE INDEX IF NOT EXISTS idx_parsed_properties_agent_given 
ON parsed_properties(stats_agent_given) 
WHERE stats_agent_given IS NOT NULL;

-- Индекс для поиска по stats_object_status (часто используется в WHERE)
CREATE INDEX IF NOT EXISTS idx_parsed_properties_status 
ON parsed_properties(stats_object_status) 
WHERE stats_object_status IS NOT NULL;

-- Индекс для поиска по stats_recall_time (используется в уведомлениях)
CREATE INDEX IF NOT EXISTS idx_parsed_properties_recall_time 
ON parsed_properties(stats_recall_time) 
WHERE stats_recall_time IS NOT NULL;

-- Составной индекс для запроса уведомлений о перезвоне
-- Оптимизирует: WHERE stats_object_status = 'Перезвонить' AND stats_recall_time <= NOW() AND stats_agent_given IS NOT NULL
CREATE INDEX IF NOT EXISTS idx_parsed_properties_recall_notification 
ON parsed_properties(stats_object_status, stats_recall_time, stats_agent_given) 
WHERE stats_object_status = 'Перезвонить' AND stats_recall_time IS NOT NULL AND stats_agent_given IS NOT NULL;

-- Составной индекс для get_latest_parsed_properties
-- Оптимизирует: WHERE krisha_id IS NOT NULL AND stats_agent_given IS NULL ORDER BY krisha_date DESC
CREATE INDEX IF NOT EXISTS idx_parsed_properties_latest 
ON parsed_properties(krisha_id, stats_agent_given, krisha_date DESC) 
WHERE krisha_id IS NOT NULL AND krisha_id != '';

-- Индекс для сортировки по stats_time_given (используется в ORDER BY)
CREATE INDEX IF NOT EXISTS idx_parsed_properties_time_given 
ON parsed_properties(stats_time_given DESC NULLS LAST);

-- Составной индекс для get_my_new_parsed_properties
-- Оптимизирует: WHERE stats_agent_given = :phone ORDER BY stats_time_given DESC
CREATE INDEX IF NOT EXISTS idx_parsed_properties_my_objects 
ON parsed_properties(stats_agent_given, stats_time_given DESC NULLS LAST, vitrina_id DESC) 
WHERE stats_agent_given IS NOT NULL;

-- Индекс для архивации (поиск активных объектов с krisha_id)
CREATE INDEX IF NOT EXISTS idx_parsed_properties_archive 
ON parsed_properties(stats_object_status, krisha_id) 
WHERE krisha_id IS NOT NULL AND krisha_id != '' 
  AND (stats_object_status IS NULL OR stats_object_status != 'Архив');

-- ============================================
-- Индексы для таблицы properties
-- ============================================

-- Индекс для поиска по category (используется в WHERE и GROUP BY)
CREATE INDEX IF NOT EXISTS idx_properties_category 
ON properties(category) 
WHERE category IS NOT NULL;

-- Индексы для поиска по агентам с использованием LOWER() для case-insensitive поиска
-- Для mop
CREATE INDEX IF NOT EXISTS idx_properties_mop_lower 
ON properties(LOWER(mop)) 
WHERE mop IS NOT NULL;

-- Для rop
CREATE INDEX IF NOT EXISTS idx_properties_rop_lower 
ON properties(LOWER(rop)) 
WHERE rop IS NOT NULL;

-- Для dd
CREATE INDEX IF NOT EXISTS idx_properties_dd_lower 
ON properties(LOWER(dd)) 
WHERE dd IS NOT NULL;

-- Индекс для поиска по client_name (используется в поиске с LIKE)
CREATE INDEX IF NOT EXISTS idx_properties_client_name 
ON properties(LOWER(client_name)) 
WHERE client_name IS NOT NULL;

-- Составной индекс для поиска по агенту и категории
CREATE INDEX IF NOT EXISTS idx_properties_agent_category 
ON properties(mop, rop, dd, category) 
WHERE category IS NOT NULL;

-- Индекс для сортировки по last_modified_at (часто используется в ORDER BY)
CREATE INDEX IF NOT EXISTS idx_properties_modified_at 
ON properties(last_modified_at DESC);

-- ============================================
-- Статистика для оптимизатора запросов
-- ============================================

-- Обновляем статистику для всех таблиц
ANALYZE properties;
ANALYZE parsed_properties;

-- ============================================
-- Комментарии к индексам
-- ============================================

COMMENT ON INDEX idx_parsed_properties_krisha_id IS 'Индекс для поиска объектов по krisha_id (get_latest_parsed_properties)';
COMMENT ON INDEX idx_parsed_properties_agent_given IS 'Индекс для поиска объектов по агенту (stats_agent_given)';
COMMENT ON INDEX idx_parsed_properties_status IS 'Индекс для фильтрации по статусу объекта';
COMMENT ON INDEX idx_parsed_properties_recall_time IS 'Индекс для поиска объектов с временем перезвона';
COMMENT ON INDEX idx_parsed_properties_recall_notification IS 'Составной индекс для уведомлений о перезвоне';
COMMENT ON INDEX idx_parsed_properties_latest IS 'Составной индекс для получения последних объектов';
COMMENT ON INDEX idx_parsed_properties_my_objects IS 'Составной индекс для получения объектов агента';
COMMENT ON INDEX idx_properties_category IS 'Индекс для фильтрации и группировки по категории';
COMMENT ON INDEX idx_properties_mop_lower IS 'Индекс для case-insensitive поиска по МОП';
COMMENT ON INDEX idx_properties_rop_lower IS 'Индекс для case-insensitive поиска по РОП';
COMMENT ON INDEX idx_properties_dd_lower IS 'Индекс для case-insensitive поиска по ДД';

