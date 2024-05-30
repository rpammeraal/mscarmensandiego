{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': [
                        'witnessed_date_key',
                        'reported_date_key',
                        'witnessed_geography_key',
                        'reported_geography_key',
                        'agent_key',
                        'behavior_key'], 'unique': True},
          {'columns': ['witnessed_date_key'], 'unique': False},
          {'columns': ['reported_date_key'], 'unique': False},
          {'columns': ['witnessed_geography_key'], 'unique': False},
          {'columns': ['reported_geography_key'], 'unique': False},
          {'columns': ['agent_key'], 'unique': False},
          {'columns': ['behavior_key'], 'unique': False},
          {'columns': ['witness_key'], 'unique': False},
        ]
    )
}}


SELECT
    dd_w.date_key                                                                                                           AS witnessed_date_key,
    dd_a.date_key                                                                                                           AS reported_date_key,
    dg_w.geography_key                                                                                                      AS witnessed_geography_key,
    dg_r.geography_key                                                                                                      AS reported_geography_key,
    da.agent_key,
    db.behavior_key,
    dw.witness_key
FROM
    {{ref('combined_sightings')}} cs
        JOIN {{ref('dim_witness')}} dw USING(witness,city,country)
        JOIN {{ref('dim_date')}} dd_w ON 
            cs.date_witness=dd_w.date_native
        JOIN {{ref('dim_date')}} dd_a ON 
            cs.date_agent=dd_a.date_native
        JOIN {{ref('dim_geography')}} dg_w ON
            cs.country=dg_w.country AND
            cs.city=dg_w.city AND
            cs.latitude=dg_w.latitude AND
            cs.longitude=dg_w.longitude
        JOIN {{ref('dim_agent')}} da USING(agent)
        JOIN {{ref('dim_geography')}} dg_r ON
            cs.country=dg_r.country AND
            cs.city_agent=dg_r.city AND
            dg_r.latitude IS NULL AND
            dg_r.longitude IS NULL
        JOIN {{ref('dim_behavior')}} db USING(has_jacket,has_weapon,has_hat,behavior)


            
