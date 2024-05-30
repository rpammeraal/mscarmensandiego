--  Query to determine that our fact table yields the same data as our base table
SELECT
    STRING_AGG(source,' - ')            AS discrepancy,
    date_witness,
    witness,
    agent,
    date_agent,
    city_agent,
    country,
    city,
    latitude,
    longitude,
    has_weapon,
    has_hat,
    has_jacket,
    behavior,
    num_occurences,
    COUNT(*)
FROM
    (
        SELECT
            'dbt_base.combined_sightings'    AS source,
            date_witness,
            witness,
            agent,
            date_agent,
            city_agent,
            country,
            city,
            latitude,
            longitude,
            has_weapon,
            has_hat,
            has_jacket,
            behavior,
            COUNT(*)                            AS num_occurences
        FROM
            dbt_base.combined_sightings
        GROUP BY
            2,3,4,5,6,7,8,9,10,11,12,13,14
        UNION
        SELECT
            'dbt_dw.fact_sighting'              AS source,
            dd_w.date_native                    AS date_witness,
            dw.witness,
            d_a.agent,
            dd_r.date_native                    AS date_agent,
            dg_r.city                           AS city_agent,
            dg_w.country,
            dg_w.city,
            dg_w.latitude,
            dg_w.longitude,
            d_b.has_weapon,
            d_b.has_hat,
            d_b.has_jacket,
            d_b.behavior,
            COUNT(*)                            AS num_occurences
        FROM
            dbt_dw.fact_sighting fs
                JOIN dbt_dw.dim_witness dw USING(witness_key)
                JOIN dbt_dw.dim_geography dg_w ON
                    fs.witnessed_geography_key=dg_w.geography_key
                JOIN dbt_dw.dim_agent d_a USING(agent_key)
                JOIN dbt_dw.dim_behavior d_b USING(behavior_key)
                JOIN dbt_dw.dim_date dd_w ON
                    fs.witnessed_date_key=dd_w.date_key
                JOIN dbt_dw.dim_geography dg_r ON
                    fs.reported_geography_key=dg_r.geography_key
                JOIN dbt_dw.dim_date dd_r ON
                    fs.reported_date_key=dd_r.date_key
        GROUP BY
            2,3,4,5,6,7,8,9,10,11,12,13,14
    ) a
GROUP BY
    2,3,4,5,6,7,8,9,10,11,12,13,14,15
HAVING 
    COUNT(*)!=2
