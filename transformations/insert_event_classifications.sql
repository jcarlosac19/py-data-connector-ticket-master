CREATE OR REPLACE PROCEDURE `ticket_master.insert_event_classifications`()
BEGIN
  -- Drop the table if it exists
  DECLARE table_exists BOOL;

  SET table_exists = (
    SELECT 
      COUNT(*)
    FROM 
      `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE 
      table_name = 'stg_event_classifications'
      AND table_schema = 'ticket_master'
  ) > 0;

  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.stg_event_classifications`';
  END IF;

  -- Create the table fresh
  CREATE TABLE `ticket_master.stg_event_classifications` (
    event_id STRING,
    event_classification_family BOOL,
    event_classification_genre STRING,
    event_classification_primary BOOL
  );

  -- Insert the distinct data
  INSERT INTO `ticket_master.stg_event_classifications` (
    event_id,
    event_classification_family,
    event_classification_genre,
    event_classification_primary
  )
  SELECT DISTINCT
    id AS event_id,
    event_classification.element.family AS event_classification_family,
    event_classification.element.genre.name AS event_classification_genre,
    event_classification.element.primary AS event_classification_primary
  FROM 
    `ticket_master.raw_events`,
    UNNEST(classifications.list) AS event_classification
  WHERE 
    event_classification.element.primary = true;
END;