CREATE OR REPLACE PROCEDURE `ticket_master.insert_event_presales`()
BEGIN
  -- Drop the table if it exists
  DECLARE table_exists BOOL;

  SET table_exists = (
    SELECT 
      COUNT(*)
    FROM 
      `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE 
      table_name = 'stg_event_presales'
      AND table_schema = 'ticket_master'
  ) > 0;

  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.stg_event_presales`';
  END IF;

  -- Create the table fresh
  CREATE TABLE `ticket_master.stg_event_presales` (
    event_id STRING,
    event_presale_startdatetime TIMESTAMP,
    event_presale_enddatetime TIMESTAMP,
    event_presale_name STRING
  );

  -- Insert the distinct data
  INSERT INTO `ticket_master.stg_event_presales` (
    event_id,
    event_presale_startdatetime,
    event_presale_enddatetime,
    event_presale_name
  )
  SELECT DISTINCT
    id AS event_id,
    SAFE_CAST(event_presale.element.startdatetime AS TIMESTAMP) AS event_presale_startdatetime,
    SAFE_CAST(event_presale.element.enddatetime AS TIMESTAMP) AS event_presale_enddatetime,
    event_presale.element.name AS event_presale_name
  FROM 
    `ticket_master.raw_events`,
    UNNEST(sales_presales.list) AS event_presale;
END;