-- ===============================================
-- Data Warehouse Initialization Script (PostgreSQL)
-- ===============================================

-- Подключаться нужно к postgres или другой системной БД, 
-- потому что текущую базу удалить нельзя.
-- В psql:  \c postgres

-- Удаляем БД, если она существует
DROP DATABASE IF EXISTS "DataWarehouse";

-- Создаем новую базу
CREATE DATABASE "DataWarehouse";

-- Подключаемся к новой базе
\c DataWarehouse;

-- Создаем слои Medallion Architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Можно добавить тестовую таблицу в bronze
CREATE TABLE bronze.sample_data (
    id SERIAL PRIMARY KEY,
    raw_value TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================================
-- End of Script
-- ===============================================
