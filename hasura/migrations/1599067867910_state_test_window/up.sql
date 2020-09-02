DROP MATERIALIZED VIEW IF EXISTS public.state_testing_results_change;

-- The state didn't always publish data for total_tested, marking null if zero
CREATE MATERIALIZED VIEW public.state_testing_results_change
AS
    SELECT
    date,
    CASE
        WHEN total_tested = 0
        THEN NULL
        ELSE total_tested
        END
    AS total_tested,
    CASE
        WHEN total_tested = 0
        THEN NULL
        ELSE total_tested - lag(total_tested) OVER (ORDER BY date)
        END
    AS total_tested_change,
    CASE
        WHEN lag(total_tested) OVER (ORDER BY date) = 0
        THEN 0::numeric
        ELSE round(
            (
                (total_tested - lag(total_tested) OVER (ORDER BY date))::numeric / lag(total_tested) OVER (ORDER BY date)
            ) * 100,
            2
        )
        END
    AS total_tested_change_pct,
    confirmed_cases,
    confirmed_cases - lag(confirmed_cases) OVER (ORDER BY date) AS confirmed_cases_change,
    CASE
        WHEN lag(confirmed_cases) OVER (ORDER BY date) = 0
        THEN 0::numeric
        ELSE round(
            (
                (confirmed_cases - lag(confirmed_cases) OVER (ORDER BY date))::numeric / lag(confirmed_cases) OVER (ORDER BY date)
            ) * 100,
            2
        )
        END
    AS confirmed_cases_change_pct,
    confirmed_cases - deaths AS confirmed_cases_minus_deaths,
    deaths,
    deaths - lag(deaths) OVER (ORDER BY date) AS deaths_change,
    CASE
        WHEN lag(deaths) OVER (ORDER BY date) = 0
        THEN 0::numeric
        ELSE round(
            (
                (deaths - lag(deaths) OVER (ORDER BY date))::numeric / lag(deaths) OVER (ORDER BY date)
            ) * 100,
            2
        )
        END
    AS deaths_change_pct
    FROM public.state_testing_results
    GROUP BY date, total_tested, confirmed_cases, deaths
    ORDER BY date DESC;
