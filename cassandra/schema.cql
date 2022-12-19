CREATE KEYSPACE IF NOT EXISTS apache_air
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        };

-------------- batch_views ---
DROP TABLE IF EXISTS apache_air.batch_views;

CREATE TABLE apache_air.batch_views (
    timestamp timestamp,
    particle text,
    interval tinyint,
    mae float,
    PRIMARY KEY((timestamp), particle, interval)
);
