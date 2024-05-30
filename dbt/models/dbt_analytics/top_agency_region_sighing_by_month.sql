WITH num_occurences_by_month_agency AS
(
    SELECT
        dd_r.month_of_year,
        dd_r.month_name,
        dg_r.city,
        COUNT(*)                            AS num_occurences
    FROM
        {{ref('fact_sighting')}} fs
            JOIN {{ref('dim_geography')}} dg_r ON
                fs.reported_geography_key=dg_r.geography_key
            JOIN {{ref('dim_date')}} dd_r ON
                fs.reported_date_key=dd_r.date_key
    GROUP BY
        dd_r.month_of_year,
        dd_r.month_name,
        dg_r.city
),
apply_ranking AS
(
    SELECT
        month_of_year,
        month_name,
        city,
        num_occurences,
        ROW_NUMBER() OVER(PARTITION BY month_name ORDER BY num_occurences DESC)             AS ranking
    FROM
        num_occurences_by_month_agency
)
SELECT
    month_name,
    city
FROM
    apply_ranking ar
WHERE
    ranking=1
ORDER BY
    month_of_year

