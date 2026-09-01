-- ============================================================
-- GAMES DATA WAREHOUSE V2 - CREACIÓN DE TABLAS ORC
-- Hive 4.1.0
-- Autor: ikermuinos
-- Descripción: Script de creación de base de datos y tablas
--              dimensionales y de hechos en formato ORC.
--              Incluye genres y categories en dim_game.
-- ============================================================


-- ============================================================
-- 1. BASE DE DATOS
-- ============================================================

DROP DATABASE IF EXISTS games_dw_v2 CASCADE;
CREATE DATABASE games_dw_v2
    COMMENT 'Data Warehouse de juegos v2 - ORC'
    LOCATION '/user/hive/warehouse/games_dw_v2.db';

USE games_dw_v2;


-- ============================================================
-- 2. DIM_DATE
-- ============================================================

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id          INT,
    release_date     STRING,
    release_year     INT,
    release_month    INT,
    release_quarter  INT
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


-- ============================================================
-- 3. DIM_GAME
-- ============================================================

DROP TABLE IF EXISTS dim_game;
CREATE TABLE dim_game (
    app_id                INT,
    name                  STRING,
    required_age          INT,
    is_free               BOOLEAN,
    dlc_count             INT,
    game_engine           STRING,
    translated_languages  STRING,
    dubbed_languages      STRING,
    genres                STRING,
    categories            STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


-- ============================================================
-- 4. DIM_DEVELOPER
-- ============================================================

DROP TABLE IF EXISTS dim_developer;
CREATE TABLE dim_developer (
    developer_id   INT,
    name           STRING,
    country        STRING,
    founding_year  INT,
    size           STRING,
    is_closed      BOOLEAN,
    continent      STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


-- ============================================================
-- 5. DIM_PUBLISHER
-- ============================================================

DROP TABLE IF EXISTS dim_publisher;
CREATE TABLE dim_publisher (
    publisher_id   INT,
    name           STRING,
    country        STRING,
    founding_year  INT,
    size           STRING,
    is_closed      BOOLEAN,
    continent      STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


-- ============================================================
-- 6. DIM_PLATFORM
-- ============================================================

DROP TABLE IF EXISTS dim_platform;
CREATE TABLE dim_platform (
    platform_id       INT,
    platform_name     STRING,
    windows           BOOLEAN,
    mac               BOOLEAN,
    linux             BOOLEAN,
    launch_year       INT,
    market_share_pct  FLOAT
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


-- ============================================================
-- 7. FACT_GAME_RELEASE
-- ============================================================

DROP TABLE IF EXISTS fact_game_release;
CREATE TABLE fact_game_release (
    app_id                INT,
    date_id               INT,
    developer_id          INT,
    publisher_id          INT,
    platform_id           INT,
    price                 FLOAT,
    price_range           STRING,
    positive              INT,
    negative              INT,
    estimated_owners_min  INT,
    estimated_owners_max  INT,
    peak_ccu              INT,
    pct_pos_total         FLOAT,
    metacritic_score      INT,
    achievements          INT
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


-- ============================================================
-- VERIFICACIÓN
-- ============================================================

SHOW TABLES;