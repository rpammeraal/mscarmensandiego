{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': ['behavior_key'], 'unique': True},
          {'columns': ['has_jacket','has_hat','has_weapon','behavior'], 'unique': False}
        ]
    )
}}

WITH all_data AS
(
    SELECT DISTINCT
       has_jacket,
       has_hat,
       has_weapon,
       behavior
    FROM
        {{ref('combined_sightings')}}
)
SELECT
    (ROW_NUMBER() OVER())::BIGINT                                                                                           AS behavior_key,
   has_jacket,
   has_hat,
   has_weapon,
   behavior
FROM
    all_data
