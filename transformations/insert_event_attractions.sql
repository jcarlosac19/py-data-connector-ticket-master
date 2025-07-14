CREATE OR REPLACE PROCEDURE `ticket_master.insert_event_attractions`()
BEGIN
  -- Drop the table if it exists
  DECLARE table_exists BOOL;

  SET table_exists = (
    SELECT 
      COUNT(*)
    FROM 
      `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE 
      table_name = 'stg_event_attractions'
      AND table_schema = 'ticket_master'
  ) > 0;

  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.stg_event_attractions`';
  END IF;

  -- Create the table fresh
  CREATE TABLE `ticket_master.stg_event_attractions` (
    event_id STRING,
    attraction_id STRING,
    attraction_locale STRING,
    attraction_test STRING, 
    attraction_type STRING,
    attraction_url STRING,
    attraction_segment STRING,
    attraction_genre STRING,
    attraction_sub_genre STRING,
    attraction_type_name STRING,
    attraction_subtype STRING
  
  );

  -- Insert the distinct data
  INSERT INTO `ticket_master.stg_event_attractions` (
    event_id,
    attraction_id,
    attraction_locale,
    attraction_test, 
    attraction_type,
    attraction_url,
    attraction_segment,
    attraction_genre,
    attraction_sub_genre,
    attraction_type_name,
    attraction_subtype
  )
  ----- Events price ranges 
  WITH attraction_classifications AS(
    SELECT DISTINCT
      attraction.element.id AS attraction_id,
      classification.element.segment.name AS segment,
      classification.element.genre.name AS genre,
      classification.element.subGenre.name AS sub_genre,
      classification.element.type.name AS type,
      classification.element.subtype.name AS subtype
    FROM
      ticket_master.raw_events,
      UNNEST(_embedded_attractions.list) AS attraction
      LEFT JOIN UNNEST(attraction.element.classifications.list) AS classification ON TRUE 
    WHERE 
      classification.element.primary = True
  )
  SELECT DISTINCT
    events.id                   AS event_id,
    attraction.element.id       AS attraction_id,
    attraction.element.locale   AS attraction_locale,
    attraction.element.name     AS attraction_test, 
    attraction.element.type     AS attraction_type,
    attraction.element.url      AS attraction_url,
    ac.segment                  AS attraction_segment,
    ac.genre                    AS attraction_genre,
    ac.sub_genre                AS attraction_sub_genre,
    ac.type                     AS attraction_type_name,
    ac.subtype                  AS attraction_subtype
  FROM 
      ticket_master.raw_events AS events,
      UNNEST(_embedded_attractions.list) AS attraction
      LEFT JOIN attraction_classifications ac ON attraction.element.id = ac.attraction_id;
END;