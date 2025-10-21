-- Create database for future records
CREATE DATABASE IF NOT EXISTS forecast_main;
-- Create database for testing
CREATE DATABASE IF NOT EXISTS forecast_test;

-- Init the main DB
USE forecast_main;
-- Create table for storing forecast data
CREATE TABLE IF NOT EXISTS forecast_data (
    -- Unique ID for this message
    id UUID,

    -- Date when forecast was generated
    forecast_date DateTime,
    -- Number of hours for which the forecast was generated
    forecast_hour UInt16,
    -- Name of the forecast model
    data_source String,
    
    -- Parameter name
    parameter String,
    -- Parameter unit of measurement
    parameter_unit String,
    -- Surface type (isobaric level, height in meters, mean sea level, ...)
    surface_type String,
    -- Height value for this type
    surface_value Float32,
    
    -- Grid boundary coordinates
    min_lon Decimal(9,6),
    max_lon Decimal(9,6), 
    min_lat Decimal(9,6),
    max_lat Decimal(9,6),
    -- Grid steps
    lon_step Decimal(9,6),
    lat_step Decimal(9,6),
    -- Number of points along latitude and longitude
    grid_size_lat UInt16,
    grid_size_lon UInt16,
    
    -- Values at grid nodes (2D grid array is turned into 1D array)
    values Array(Float32),
    
    -- Auxiliary fields
    created_at DateTime DEFAULT now(),
    file_name String,
    
    -- Indexes
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

-- Init the testing DB
USE forecast_test;
-- Create table for storing forecast data
CREATE TABLE IF NOT EXISTS forecast_data (
    -- Unique ID for this message
    id UUID,

    -- Date when forecast was generated
    forecast_date DateTime,
    -- Number of hours for which the forecast was generated
    forecast_hour UInt16,
    -- Name of the forecast model
    data_source String,
    
    -- Parameter name
    parameter String,
    -- Parameter unit of measurement
    parameter_unit String,
    -- Surface type (isobaric level, height in meters, mean sea level, ...)
    surface_type String,
    -- Height value for this type
    surface_value Float32,
    
    -- Grid boundary coordinates
    min_lon Decimal(9,6),
    max_lon Decimal(9,6), 
    min_lat Decimal(9,6),
    max_lat Decimal(9,6),
    -- Grid steps
    lon_step Decimal(9,6),
    lat_step Decimal(9,6),
    -- Number of points along latitude and longitude
    grid_size_lat UInt16,
    grid_size_lon UInt16,
    
    -- Values at grid nodes (2D grid array is turned into 1D array)
    values Array(Float32),
    
    -- Auxiliary fields
    created_at DateTime DEFAULT now(),
    file_name String,
    
    -- Indexes
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