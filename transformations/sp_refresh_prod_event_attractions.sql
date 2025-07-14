CREATE OR REPLACE PROCEDURE `ticket_master.sp_refresh_prod_event_attractions`()
BEGIN
  DECLARE table_exists BOOL;

  -- 1. Check if the table exists
  SET table_exists = (
    SELECT COUNT(*)
    FROM `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE table_name = 'prod_event_attractions'
      AND table_schema = 'ticket_master'
  ) > 0;

  -- 2. Drop the table if it exists
  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.prod_event_attractions`';
  END IF;

  -- 3. Create the table with a defined schema
  EXECUTE IMMEDIATE '''
    CREATE TABLE `ticket_master.prod_event_attractions` (
      event_id STRING,
      attraction_id STRING,
      attraction_artist STRING,
      attraction_segments STRING,
      attraction_sub_genre STRING,
      attraction_genre STRING,
      attraction_subtype STRING,
      attraction_type STRING,
      attraction_type_name STRING
    )
  ''';

  -- 4. Insert the grouped data
  INSERT INTO `ticket_master.prod_event_attractions`
  SELECT DISTINCT
    event_id,
    attraction_id, 
    MAX(attraction_test) AS attraction_artist,
    STRING_AGG(attraction_segment, ",") AS attraction_segments,
    MAX(attraction_sub_genre) AS attraction_sub_genre,
    MAX(attraction_genre) AS attraction_genre,
    MAX(attraction_subtype) AS attraction_subtype,
    MAX(attraction_type) AS attraction_type,
    MAX(attraction_type_name) AS attraction_type_name
  FROM
    `ticket_master.stg_event_attractions`
  GROUP BY
    event_id,
    attraction_id;
END;