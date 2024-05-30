{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': ['geography_key'], 'unique': True},
          {'columns': ['country','city','latitude','longitude'], 'unique': False}
        ]
    )
}}

--  dim_geography is a hierarchical dimension table:
--      country -> city -> latitude/longitude (with latitude/longitude being optional)
--  Typical data pattern:
--      cityA, countryB, NULL, NULL
--      cityA, countryB, <lat1>, <long1>
--      cityA, countryB, <lat2>, <long2>
--      ...
--  Ideally, source data would need country_agent, for now we assume that country
--  is the same for city_agent.
WITH data_with_lat_long AS
(
    SELECT DISTINCT
        country,
        city,
        latitude                                                                                                            AS latitude,
        longitude                                                                                                           AS longitude
    FROM
        {{ref('combined_sightings')}} cs
    WHERE
        latitude IS NOT NULL AND
        longitude IS NOT NULL
),
data_without_lat_long AS
(
    SELECT DISTINCT
        country,
        city
    FROM
        {{ref('combined_sightings')}} cs
),
data_from_reported_city AS
(
    SELECT DISTINCT
        country,
        city_agent                                                                                                          AS city
    FROM
        {{ref('combined_sightings')}} cs
    WHERE
        NOT EXISTS
        (
            SELECT
                NULL
            FROM
                data_without_lat_long dfs
            WHERE
                cs.country=dfs.country AND
                cs.city_agent=dfs.city
        )
),
combined_data AS
(
    SELECT
        country,
        city,
        latitude,
        longitude
    FROM
        data_with_lat_long
    UNION
    SELECT
        country,
        city,
        NULL::DOUBLE PRECISION                                                                                              AS latitude,
        NULL::DOUBLE PRECISION                                                                                              AS longitude
    FROM
        data_without_lat_long
    UNION
    SELECT
        country,
        city,
        NULL::DOUBLE PRECISION                                                                                              AS latitude,
        NULL::DOUBLE PRECISION                                                                                              AS longitude
    FROM
        data_from_reported_city
)
SELECT
    (ROW_NUMBER() OVER())::BIGINT                                                                                           AS geography_key,
    country,
    city,
    latitude,
    longitude
FROM
    combined_data
