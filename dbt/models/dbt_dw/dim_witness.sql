{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': ['witness_key'], 'unique': True},
          {'columns': ['witness'], 'unique': False}
        ]
    )
}}

--  Too few data points to make this a full-fledged dimension table.
WITH all_data AS
(
    SELECT DISTINCT
        witness,
        country,
        city
    FROM
        {{ref('combined_sightings')}} cs
)
SELECT
    (ROW_NUMBER() OVER())::BIGINT                                                                                           AS witness_key,
    witness,
    country,
    city
FROM
    all_data
        
