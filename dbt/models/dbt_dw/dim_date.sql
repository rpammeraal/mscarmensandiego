{{ config(
        materialized='table', 
        indexes=
        [
          {'columns': ['date_key'], 'unique': True}
        ]
    )
}}

--  Generate dim_date (simplified version).
WITH seed AS
(
    SELECT 
        a.s                                                                                                                 AS tmp_id
    FROM
        generate_series(0,50000,1) AS a(s)
    ORDER BY
        1
),
date_native AS
(
    SELECT
        tmp_id,
        DATE '1/1/1900' + INTERVAL '1 day ' * tmp_id                                                                        AS date_native
    FROM
        seed
),
intermediate AS
(
    SELECT
        (EXTRACT(YEAR FROM date_native)*10000+EXTRACT(MONTH FROM date_native)*100+EXTRACT(DAY FROM date_native))::BIGINT    AS date_key,
        date_native,
        tmp_id+TO_CHAR('1/1/1900'::DATE, 'J')::INT                                                                          AS julian_date,
        TO_CHAR(date_native,'Day') 					                                                                        AS day_name,
        TO_NUMBER(TO_CHAR(date_native,'W'),'9')					                                                            AS week_of_month,
        TO_NUMBER(TO_CHAR(date_native,'WW'),'99')					                                                        AS week_of_year,
        TO_CHAR(date_native,'Month') 					                                                                    AS month_name,
        TO_NUMBER(TO_CHAR(date_native,'MM'),'99')					                                                        AS month_of_year,
        CAST(TO_CHAR(date_native,'YYYYMM') || '01' AS DATE)					                                                AS month_start_date,
        TO_NUMBER(TO_CHAR(date_native,'YYYY'),'9999')                                                                       AS year_number,
        TO_CHAR(date_native,'YYYY')                                                                                         AS year_name,
        CAST(EXTRACT(year FROM date_native) AS VARCHAR) 
            || '-' || RIGHT('0' || CAST(EXTRACT(month FROM date_native) AS VARCHAR),2)                                      AS year_month
    FROM
        date_native
)
SELECT
    date_key,
    date_native,
    julian_date,
    day_name,
    SUBSTR(day_name,1,3)					                                                                                AS day_name_short,
    week_of_month,
    week_of_year,
    month_name,
    SUBSTR(month_name,1,3)					                                                                                AS month_name_short,
    month_of_year,
    month_start_date,
    CAST(month_start_date + INTERVAL '1 month' - INTERVAL '1 day' AS DATE)					                                AS month_end_date,
    year_number,
    year_name,
    year_month
FROM
    intermediate
