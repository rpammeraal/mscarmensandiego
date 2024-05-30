WITH behavior_by_month AS
(
    SELECT DISTINCT
        dd_w.month_name,
        dd_w.month_of_year,
        db.behavior,
        COUNT(*) OVER(PARTITION BY dd_w.month_name, db.behavior)    AS cnt,
        COUNT(*) OVER(PARTITION BY dd_w.month_name)                 AS total_count_by_month
    FROM
        {{ref('fact_sighting')}} fs
            JOIN {{ref('dim_date')}} dd_w ON
                fs.witnessed_date_key=dd_w.date_key
            JOIN {{ref('dim_behavior')}} db USING(behavior_key)
    ORDER BY 
        1,3 desc
)
SELECT
    bbm.month_name                                                  AS month,
    bbm.month_of_year,
    bbm.behavior || ' [%]'                                          AS behavior,
    ROUND((bbm.cnt * 100.0 / bbm.total_count_by_month),2)           AS probability
FROM
    behavior_by_month bbm
        JOIN {{ref('three_most_occuring_behaviors')}} USING(behavior)
ORDER BY
    bbm.month_of_year
