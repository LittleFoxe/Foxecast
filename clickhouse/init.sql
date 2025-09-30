-- Используем переменные окружения для паролей
SET allow_settings_after_format_in_insert=1;

-- Создаём базу данных для будущих записей
CREATE DATABASE IF NOT EXISTS forecast_main;
USE forecast_main;
-- Создаём таблицу для хранения прогностических данных
CREATE TABLE IF NOT EXISTS forecast_data (
    -- Уникальный ID для данного сообщения
    id UUID,

    -- Дата формирования прогноза
    forecast_date DateTime,
    -- Количество часов, на которые был сформирован прогноз
    forecast_hour UInt16,
    -- Название прогностической модели
    data_source String,
    
    --Название параметра
    parameter String,
    -- единица измерения параметра
    parameter_unit String,
    -- Тип поверхности (изобарический уровень, высота в метрах, поверхность Земли)
    surface_type String,
    -- Значение высоты для данного типа
    surface_value Float32,
    
    -- Краевые координаты сетки
    min_lon Decimal(9,6),
    max_lon Decimal(9,6), 
    min_lat Decimal(9,6),
    max_lat Decimal(9,6),
    -- Шаги сетки
    lon_step Decimal(9,6),
    lat_step Decimal(9,6),
    -- Количество точек по широте и долготе в отдельности
    grid_size_lat UInt16,
    grid_size_lon UInt16,
    
    -- Значения в узлах сетки (двумерный массив сетки, развёрнутый в одномерный массив) 
    values Array(Float32),
    
    -- Служебные поля
    created_at DateTime DEFAULT now(),
    file_name String,
    
    -- Индексы
    INDEX idx_geo_coverage (min_lon, max_lon, min_lat, max_lat) TYPE minmax GRANULARITY 3,
    INDEX idx_parameter_surface (parameter, surface_type, surface_value) TYPE bloom_filter GRANULARITY 2,
    INDEX idx_forecast_time (forecast_date, forecast_hour) TYPE minmax GRANULARITY 2,
    INDEX idx_data_source data_source TYPE bloom_filter GRANULARITY 1,
    INDEX idx_created_at created_at TYPE minmax GRANULARITY 2,
    INDEX idx_id id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_file_name file_name TYPE bloom_filter GRANULARITY 1
    
) ENGINE = MergeTree()
PARTITION BY parameter
ORDER BY (parameter, surface_type, surface_value, forecast_date, forecast_hour, data_source, file_name)
SETTINGS index_granularity = 8192;