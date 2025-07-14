CREATE OR REPLACE PROCEDURE `ticket_master.insert_event_venues`()
BEGIN
  -- Drop the table if it exists
  DECLARE table_exists BOOL;

  SET table_exists = (
    SELECT 
      COUNT(*)
    FROM 
      `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE 
      table_name = 'stg_event_venues'
      AND table_schema = 'ticket_master'
  ) > 0;

  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.stg_event_venues`';
  END IF;

  -- Create the table fresh
  CREATE TABLE `ticket_master.stg_event_venues` (
    event_id STRING,
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
  );

  -- Insert the distinct data
  INSERT INTO `ticket_master.stg_event_venues` (
    event_id,
    venue_id,
    venue_name,
    venue_postal_code,
    venue_city,
    venue_state,
    venue_state_code,
    venue_country,
    venue_country_code,
    venue_latitude,
    venue_longitude,
    venue_locale,
    venue_timezone,
    venue_type
  )
  SELECT DISTINCT
    id                                  AS event_id,
    venue.element.id                    AS venue_id,
    venue.element.name                  AS venue_name,
    venue.element.postalCode            AS venue_postal_code,
    venue.element.city.name             AS venue_city,
    venue.element.state.name            AS venue_state,
    venue.element.state.statecode       AS venue_state_code,
    venue.element.country.name          AS venue_country,
    venue.element.country.countrycode   AS venue_country_code,
    SAFE_CAST(venue.element.location.latitude AS FLOAT64)   AS venue_latitude,
    SAFE_CAST(venue.element.location.longitude AS FLOAT64)  AS venue_longitude,
    venue.element.locale                AS venue_locale,
    venue.element.timezone              AS venue_timezone,
    venue.element.type                  AS venue_type
  FROM 
    `ticket_master.raw_events`,
    UNNEST(_embedded_venues.list) AS venue
  ORDER BY 
    venue.element.id;
END;