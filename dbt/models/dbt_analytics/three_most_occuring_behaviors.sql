WITH top_three AS
(
    SELECT
        d_b.behavior,
        COUNT(*)                                                    AS num_occurences
    FROM
        {{ref('fact_sighting')}} fs
            JOIN {{ref('dim_behavior')}} d_b USING(behavior_key)
    GROUP BY
        d_b.behavior
    ORDER BY
        2 DESC
    LIMIT 3

)
SELECT
    behavior,
    ROW_NUMBER() OVER(ORDER BY num_occurences DESC)                 AS ranking
FROM
    top_three
