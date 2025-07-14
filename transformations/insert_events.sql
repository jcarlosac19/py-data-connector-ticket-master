CREATE OR REPLACE PROCEDURE `ticket_master.insert_event`()
BEGIN
  -- Drop the table if it exists
  DECLARE table_exists BOOL;

  SET table_exists = (
    SELECT 
      COUNT(*)
    FROM 
      `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE 
      table_name = 'stg_event'
      AND table_schema = 'ticket_master'
  ) > 0;

  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.stg_event`';
  END IF;

  -- Create the table fresh
  CREATE TABLE `ticket_master.stg_event` (
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
    event_place_region STRING
  );

  -- Insert the distinct data
  INSERT INTO `ticket_master.stg_event` (
    event_id,
    event_name,
    event_type,
    event_locale,
    event_ticketing_id,
    event_info,
    event_description,
    event_start_datetime,
    event_end_local_date,
    event_end_datetime,
    event_timezone,
    event_status_code,
    event_span_multiple_days,
    event_legal_age_enforced,
    event_safeTix_enabled,
    event_sales_start_datetime,
    event_sales_end_datetime,
    event_start_local_date,
    event_promoter,
    event_place_city_name,
    event_place_country_name,
    event_place_state_name,
    event_place_location_longitude,
    event_place_location_latitude,
    event_place_region
  )
  SELECT DISTINCT
    id                                                               AS event_id,
    name                                                             AS event_name, 
    type                                                             AS event_type,
    locale                                                           AS event_locale,
    ticketing_id                                                     AS event_ticketing_id,
    info                                                             AS event_info,
    description                                                      AS event_description,
    COALESCE(
      SAFE_CAST(dates_start_dateTime AS TIMESTAMP), 
      SAFE_CAST(
        CONCAT(dates_start_localDate, ' 00:00:00') AS TIMESTAMP
      )
    )                                                                AS event_start_datetime,
    SAFE_CAST(dates_end_localDate AS DATE)                           AS event_end_local_date,
    SAFE_CAST(dates_end_dateTime AS TIMESTAMP)                       AS event_end_datetime,
    dates_timezone                                                   AS event_timezone,
    dates_status_code                                                AS event_status_code,
    SAFE_CAST(dates_spanMultipleDays AS BOOLEAN)                     AS event_span_multiple_days,
    SAFE_CAST(ageRestrictions_legalAgeEnforced AS BOOLEAN)           AS event_legal_age_enforced,
    ticketing_safeTix_enabled                                        AS event_safeTix_enabled,
    SAFE_CAST(sales_public_startDateTime AS TIMESTAMP)               AS event_sales_start_datetime,
    SAFE_CAST(sales_public_endDateTime AS TIMESTAMP)                 AS event_sales_end_datetime,
    SAFE_CAST(dates_start_localDate AS DATE)                         AS event_start_local_date,
    promoter_name                                                    AS event_promoter, 
    place_city_name                                                  AS event_place_city_name,
    place_country_name                                               AS event_place_country_name,
    place_state_name                                                 AS event_place_state_name,
    place_location_longitude                                         AS event_place_location_longitude,
    place_location_latitude                                          AS event_place_location_latitude,
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
      END AS event_place_region
  FROM 
    `ticket_master.raw_events`;
END;