CREATE OR REPLACE PROCEDURE `ticket_master.insert_event_prices`()
BEGIN
  -- Drop the table if it exists
  DECLARE table_exists BOOL;

  SET table_exists = (
    SELECT 
      COUNT(*)
    FROM 
      `ticket_master`.INFORMATION_SCHEMA.TABLES
    WHERE 
      table_name = 'stg_event_prices'
      AND table_schema = 'ticket_master'
  ) > 0;

  IF table_exists THEN
    EXECUTE IMMEDIATE 'DROP TABLE `ticket_master.stg_event_prices`';
  END IF;

  -- Create the table fresh
  CREATE TABLE `ticket_master.stg_event_prices` (
    event_id STRING,
    event_price_min FLOAT64,
    event_price_max FLOAT64,
    event_price_type STRING
  
  );

  -- Insert the distinct data
  INSERT INTO `ticket_master.stg_event_prices` (
    event_id,
    event_price_min,
    event_price_max,
    event_price_type
  )
  ----- Events price ranges 
  SELECT DISTINCT
      id AS event_id, 
      CASE WHEN price_range.element.currency = "CAD" THEN price_range.element.min * 0.73 ELSE price_range.element.min END AS event_price_min,
      CASE WHEN price_range.element.currency = "CAD" THEN price_range.element.max * 0.73 ELSE price_range.element.max END AS event_price_max,
      price_range.element.type AS event_price_type
  FROM 
    ticket_master.raw_events,
    UNNEST(priceRanges.list) AS price_range
  WHERE 
    price_range.element.max + price_range.element.min > 0;
END;