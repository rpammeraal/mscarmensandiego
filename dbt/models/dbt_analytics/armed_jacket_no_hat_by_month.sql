WITH calc_stats AS
(
    SELECT
        dd_s.month_name,
        dd_s.month_of_year,
        SUM(CASE WHEN db.has_weapon=TRUE AND db.has_jacket=TRUE AND db.has_hat=FALSE THEN 1 ELSE 0 END)
                                                                AS armed_jacket_no_hat_count,
        COUNT(*)                                                AS overall_count
    FROM
        {{ref('fact_sighting')}} fs
            JOIN {{ref('dim_date')}} dd_s ON
                fs.witnessed_date_key=dd_s.date_key
            JOIN {{ref('dim_behavior')}} db USING(behavior_key)
    GROUP BY
        dd_s.month_name,
        dd_s.month_of_year
    ORDER BY
        dd_s.month_of_year
)
SELECT
    month_name                                                  AS month,
    ROUND(armed_jacket_no_hat_count * 100.0/overall_count,2)    AS probability_armed_jacket_no_hat_pct
FROM
    calc_stats

