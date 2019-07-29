drop_staging_trips = "DROP TABLE IF EXISTS staging_trips"
drop_table_trips = "DROP TABLE IF EXISTS trips"
drop_table_vendors = "DROP TABLE IF EXISTS vendors"


create_staging_trips_table = """
    CREATE TABLE IF NOT EXISTS staging_trips (
    vendor_id          CHAR(3) REFERENCES vendors (vendor_id),
    pickup_datetime    TEXT,
    dropoff_datetime    TEXT,
    passenger_count    INT,
    trip_distance      NUMERIC,
    pickup_longitude   DECIMAL(9,6),
    pickup_latitude    DECIMAL(9,6),
    rate_code          NUMERIC,
    store_and_fwd_flag NUMERIC,
    dropoff_longitude  DECIMAL(9,6),
    dropoff_latitude   DECIMAL(9,6),
    payment_type       VARCHAR(20),
    fare_amount        NUMERIC,
    surcharge          NUMERIC,
    tip_amount         NUMERIC,
    tolls_amount       NUMERIC,
    total_amount       NUMERIC,
    PRIMARY KEY (pickup_datetime, dropoff_datetime)
    )
    """



create_table_vendors = ("""
CREATE TABLE IF NOT EXISTS vendors (
    vendor_id  VARCHAR(3) PRIMARY KEY,
    nome       VARCHAR(60),
    endereco   VARCHAR(60),
    cidade     VARCHAR(40),
    estado     CHAR(2),
    cep        CHAR(5),
    pais    CHAR(3),
    contato    VARCHAR(50),
    current    VARCHAR(3)
    )
"""
)

create_table_trips = (
    """
    CREATE TABLE IF NOT EXISTS trips (
    pickup_datetime    TIMESTAMP,
    dropoff_datetime   TIMESTAMP,
    pickup_latitude    DECIMAL(9,6),
    pickup_longitude   DECIMAL(9,6),
    dropoff_latitude   DECIMAL(9,6),
    dropoff_longitude  DECIMAL(9,6),
    fare_amount        NUMERIC,
    passenger_count    INT,
    payment_type       VARCHAR(20),
    rate_code          NUMERIC,
    store_and_fwd_flag NUMERIC,
    surcharge          NUMERIC,
    tip_amount         NUMERIC,
    tolls_amount       NUMERIC,
    total_amount       NUMERIC,
    trip_distance      NUMERIC,
    vendor_id          CHAR(3) REFERENCES vendors (vendor_id),
    PRIMARY KEY (pickup_datetime, dropoff_datetime)
    )
    """
)



copy_trips = (
    """
    COPY staging_trips 
    FROM {}
    IAM_ROLE {}
    JSON 'auto'
    """
)


insert_vendors = (
    """
    INSERT INTO vendors (vendor_id, nome, endereco, cidade, estado, cep, pais, contato, current)
    VALUES (%s, %s, %s, %s, %s, %s, %s,  %s, %s)
    """
)

etl_trips = ("""
INSERT INTO trips 
(
    pickup_datetime, dropoff_datetime, pickup_latitude, pickup_longitude, dropoff_latitude, 
    dropoff_longitude, fare_amount, passenger_count, payment_type, rate_code, store_and_fwd_flag, surcharge,
    tip_amount, tolls_amount, total_amount, trip_distance, vendor_id
)
SELECT 
    (SPLIT_PART(LEFT(pickup_datetime, 19), 'T', 1) || ' ' || SPLIT_PART(LEFT(pickup_datetime, 19), 'T', 2))::timestamp as pickup_datetime,
    (SPLIT_PART(LEFT(dropoff_datetime, 19), 'T', 1) || ' ' || SPLIT_PART(LEFT(dropoff_datetime, 19), 'T', 2))::timestamp as dropoff_datetime,
    pickup_latitude,
    pickup_longitude,
    dropoff_latitude,
    dropoff_longitude,
    fare_amount,
    passenger_count,   
    CASE 
        WHEN payment_type in ('Cas','CAS','Cash','CASH','CSH') THEN 'Cash'
        WHEN payment_type in ('Cre', 'CRE','Credit','CREDIT', 'CRD') THEN 'Credit'
        WHEN payment_type in ('DIS','Dis','Dispute') THEN 'Dispute'
        WHEN payment_type in ('No', 'No Charge', 'NOC') THEN 'No Charge'
        ELSE 'Unknown' END as payment_type,
    rate_code,
    store_and_fwd_flag,
    surcharge,
    tip_amount,
    tolls_amount,
    total_amount,
    trip_distance,
    vendor_id
FROM staging_trips
""")




drop_table_queries = [drop_table_trips, drop_staging_trips, drop_table_vendors]
create_table_queries = [create_table_vendors, create_staging_trips_table, create_table_trips]
etl_queries = {"copy_trips": copy_trips, "insert_vendors": insert_vendors, 'etl_trips': etl_trips}

