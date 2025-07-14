CREATE OR REPLACE PROCEDURE `ticket_master.sp_refresh_prod_event_details`()
BEGIN
  DECLARE table_exists BOOL;

  -- Check if the table exists
  SET table_exists = (
    SELECT COUNT(*)
    FROM `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE table_name = 'prod_event_details'
      AND table_schema = 'ticket_master'
  ) > 0;

    -- If exists, clear the table for a fresh insert
  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.prod_event_details`';
  END IF;

  EXECUTE IMMEDIATE '''
      CREATE TABLE `ticket_master.prod_event_details` (
        -- Event fields
        event_id STRING,
        event_name STRING,
        event_type STRING,
        event_locale STRING,
        event_ticketing_id STRING,
        event_info STRING,
        event_description STRING,
        event_start_datetime TIMESTAMP,
        event_end_local_date DATE,
        event_end_datetime TIMESTAMP,
        event_timezone STRING,
        event_status_code STRING,
        event_span_multiple_days BOOL,
        event_legal_age_enforced BOOL,
        event_safeTix_enabled BOOL,
        event_sales_start_datetime TIMESTAMP,
        event_sales_end_datetime TIMESTAMP,
        event_start_local_date DATE,
        event_promoter STRING,
        event_place_city_name STRING,
        event_place_country_name STRING,
        event_place_state_name STRING,
        event_place_location_longitude FLOAT64,
        event_place_location_latitude FLOAT64,
        event_year INT64,
        event_season STRING,
        event_part_of_day STRING,
        event_place_country_region STRING,

        -- Classification
        event_classification_family BOOL,
        event_classification_genre STRING,
        event_classification_primary BOOL,

        -- Prices
        event_price_min FLOAT64,
        event_price_max FLOAT64,
        event_price_type STRING,
        event_price_avg_midpoint_price FLOAT64,

        -- Presales
        presale_total_presales INT64,
        presale_first_presale_start TIMESTAMP,
        presale_last_presale_end TIMESTAMP,
        presale_total_presale_window_days INT64,

        -- Venues
        venue_id STRING,
        venue_name STRING,
        venue_postal_code STRING,
        venue_city STRING,
        venue_state STRING,
        venue_state_code STRING,
        venue_country STRING,
        venue_country_code STRING,
        venue_latitude FLOAT64,
        venue_longitude FLOAT64,
        venue_locale STRING,
        venue_timezone STRING,
        venue_type STRING
      )
    ''';

  -- Insert fresh data with your logic
  INSERT INTO `ticket_master.prod_event_details`
    WITH presales_summary AS(
      SELECT
        event_id,
        COUNT(*) AS presale_total_presales,
        MIN(event_presale_startdatetime) AS presale_first_presale_start,
        MAX(event_presale_enddatetime) AS presale_last_presale_end,
        DATE_DIFF(MAX(DATE(event_presale_enddatetime)), MIN(DATE(event_presale_startdatetime)), DAY) AS presale_total_presale_window_days
      FROM
        `ticket_master.stg_event_presales`
      GROUP BY
        event_id
    )
    SELECT 
      --Events details
      e.*,
      EXTRACT(YEAR FROM event_start_datetime) AS event_year,
      CASE
        WHEN EXTRACT(MONTH FROM event_start_datetime) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM event_start_datetime) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM event_start_datetime) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM event_start_datetime) IN (9, 10, 11) THEN 'Fall'
      END AS event_season,
      CASE
        WHEN EXTRACT(HOUR FROM DATETIME(event_start_datetime, 'America/Chicago')) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM DATETIME(event_start_datetime, 'America/Chicago')) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM DATETIME(event_start_datetime, 'America/Chicago')) BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'  -- Covers 21–23 and 0–4
      END AS event_part_of_day,
      CASE 
        WHEN event_place_state_name = 'Queensland' THEN 'Oceania - Australia'
        WHEN event_place_state_name = 'South Australia' THEN 'Oceania - Australia'
        WHEN event_place_state_name = 'Victoria' THEN 'Oceania - Australia'
        WHEN event_place_state_name = 'New South Wales' THEN 'Oceania - Australia'
        WHEN event_place_state_name = 'Salzburg' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'British Columbia' THEN 'Canada - West'
        WHEN event_place_state_name = 'Ontario' THEN 'Canada - East'
        WHEN event_place_state_name = 'Quebec' THEN 'Canada - East'
        WHEN event_place_state_name = 'Alberta' THEN 'Canada - West'
        WHEN event_place_state_name = 'Nova Scotia' THEN 'Canada - Atlantic'
        WHEN event_place_state_name = 'New Brunswick' THEN 'Canada - Atlantic'
        WHEN event_place_state_name = 'Manitoba' THEN 'Canada - Central'
        WHEN event_place_state_name = 'Uusimaa' THEN 'Europe - Northern Europe'
        WHEN event_place_state_name = "Provence-Alpes-Côte d'Azur" THEN 'Europe - Western Europe'
        WHEN event_place_state_name = 'Île-de-France' THEN 'Europe - Western Europe'
        WHEN event_place_state_name = 'Pays de la Loire' THEN 'Europe - Western Europe'
        WHEN event_place_state_name = 'Baden-Württemberg' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Rheinland-Pfalz' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Hamburg' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Schleswig-Holstein' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Saarland' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Bremen' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Mecklenburg-Vorpommern' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Berlin' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Brandenburg' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'Leinster' THEN 'Europe - Western Europe'
        WHEN event_place_state_name = 'Munster' THEN 'Europe - Western Europe'
        WHEN event_place_state_name = 'Utrecht' THEN 'Europe - Western Europe'
        WHEN event_place_state_name = 'Vest-Agder' THEN 'Europe - Northern Europe'
        WHEN event_place_state_name = 'Nordland' THEN 'Europe - Northern Europe'
        WHEN event_place_state_name = 'Østfold' THEN 'Europe - Northern Europe'
        WHEN event_place_state_name = 'Oslo' THEN 'Europe - Northern Europe'
        WHEN event_place_state_name = 'Aust-Agder' THEN 'Europe - Northern Europe'
        WHEN event_place_state_name = 'Extremadura' THEN 'Europe - Southern Europe'
        WHEN event_place_state_name = 'Basel-Landschaft' THEN 'Europe - Central Europe'
        WHEN event_place_state_name = 'England' THEN 'Europe - British Isles'
        WHEN event_place_state_name = 'Scotland' THEN 'Europe - British Isles'
        WHEN event_place_state_name = 'Wales' THEN 'Europe - British Isles'
        WHEN event_place_state_name = 'Northern Ireland' THEN 'Europe - British Isles'
        WHEN event_place_state_name = 'California' THEN 'USA - West'
        WHEN event_place_state_name = 'Oregon' THEN 'USA - West'
        WHEN event_place_state_name = 'Washington' THEN 'USA - West'
        WHEN event_place_state_name = 'Hawaii' THEN 'USA - West'
        WHEN event_place_state_name = 'Nevada' THEN 'USA - West'
        WHEN event_place_state_name = 'Idaho' THEN 'USA - West'
        WHEN event_place_state_name = 'Colorado' THEN 'USA - West'
        WHEN event_place_state_name = 'Utah' THEN 'USA - West'
        WHEN event_place_state_name = 'Minnesota' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Iowa' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Kansas' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Missouri' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Nebraska' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Illinois' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Indiana' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Ohio' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Michigan' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Wisconsin' THEN 'USA - Midwest'
        WHEN event_place_state_name = 'Arizona' THEN 'USA - Southwest'
        WHEN event_place_state_name = 'New Mexico' THEN 'USA - Southwest'
        WHEN event_place_state_name = 'Texas' THEN 'USA - Southwest'
        WHEN event_place_state_name = 'Oklahoma' THEN 'USA - Southwest'
        WHEN event_place_state_name = 'Florida' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'Georgia' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'South Carolina' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'North Carolina' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'Virginia' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'West Virginia' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'Kentucky' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'Tennessee' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'Mississippi' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'Alabama' THEN 'USA - Southeast'
        WHEN event_place_state_name = 'New York' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'New Jersey' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'Pennsylvania' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'Massachusetts' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'Rhode Island' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'Connecticut' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'Delaware' THEN 'USA - Northeast'
        WHEN event_place_state_name = 'Maryland' THEN 'USA - Northeast'
      END AS event_place_country_region, 

      --Classifications
      c.* EXCEPT(event_id),

      --Prices
      p.* EXCEPT(event_id),

      SAFE_DIVIDE(p.event_price_min  + p.event_price_max, 2) AS event_price_avg_midpoint_price,

      --Presales
      ps.* EXCEPT(event_id),
      
      --Venues
      v.* EXCEPT(event_id)
    FROM 
      `ticket_master.stg_event` e
      LEFT JOIN `ticket_master.stg_event_prices` p ON e.event_id = p.event_id
      LEFT JOIN `ticket_master.stg_event_classifications` c ON e.event_id = c.event_id 
      LEFT JOIN presales_summary ps ON ps.event_id = e.event_id
      LEFT JOIN `ticket_master.stg_event_venues` v ON v.event_id = e.event_id;

END;