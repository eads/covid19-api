DROP MATERIALIZED VIEW IF EXISTS public.zipcode_testing_results_change;

-- Include demographics in materialized view to improve performance, flatten
CREATE MATERIALIZED VIEW public.zipcode_testing_results_change
AS
    SELECT
    public.zipcode_testing_results.date,
    public.zipcode_testing_results.zipcode,
    census_geography_id,
    total_tested,
    total_tested - lag(total_tested) OVER (PARTITION BY public.zipcode_testing_results.zipcode ORDER BY public.zipcode_testing_results.date) AS total_tested_change,
    round(
        (
            (total_tested - lag(total_tested) OVER (
                PARTITION BY public.zipcode_testing_results.zipcode ORDER BY public.zipcode_testing_results.date
            ))::numeric / lag(total_tested) OVER (
                PARTITION BY public.zipcode_testing_results.zipcode ORDER BY public.zipcode_testing_results.date
            )
        ) * 100,
        2
    ) AS total_tested_change_pct,
    confirmed_cases,
    confirmed_cases - lag(confirmed_cases) OVER (
        PARTITION BY public.zipcode_testing_results.zipcode ORDER BY public.zipcode_testing_results.date
    ) AS confirmed_cases_change,
    round(
        (
            (confirmed_cases - lag(confirmed_cases) OVER (
                PARTITION BY public.zipcode_testing_results.zipcode ORDER BY public.zipcode_testing_results.date
            ))::numeric / lag(confirmed_cases) OVER (
                PARTITION BY public.zipcode_testing_results.zipcode ORDER BY public.zipcode_testing_results.date
            )
        ) * 100,
        2
    ) AS confirmed_cases_change_pct,
    CASE
        WHEN public.census_geographies.id IS NULL
        THEN NULL
        ELSE round(
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric,
            2
        )
        END
    AS pct_population_moe,
    CASE
        WHEN public.census_geographies.id IS NULL
        THEN FALSE
        ELSE (
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric
        ) < 0.1
        END
    AS below_moe_threshold,
    CASE
        WHEN public.census_geographies.id IS NULL THEN NULL
        WHEN (
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric
        ) < 0.1 THEN round(
                confirmed_cases / (public.census_geographies.population->>'E')::numeric,
                2
            )
        ELSE NULL
        END
    AS cases_per_capita,
    CASE
        WHEN public.census_geographies.id IS NULL THEN NULL
        WHEN (
            (public.census_geographies.population->>'M')::numeric /
            (public.census_geographies.population->>'E')::numeric
        ) < 0.1 THEN round(
            (confirmed_cases / (public.census_geographies.population->>'E')::numeric) * 1000,
            2
        )
        ELSE NULL
        END
    AS cases_per_1000,
    confirmed_cases_unknown,
    total_tested_unknown,
    confirmed_cases_less_than_20,
    total_tested_less_than_20,
    confirmed_cases_20_to_29,
    total_tested_20_to_29,
    confirmed_cases_30_to_39,
    total_tested_30_to_39,
    confirmed_cases_40_to_49,
    total_tested_40_to_49,
    confirmed_cases_50_to_59,
    total_tested_50_to_59,
    confirmed_cases_60_to_69,
    total_tested_60_to_69,
    confirmed_cases_70_to_79,
    total_tested_70_to_79,
    confirmed_cases_80_or_more,
    total_tested_80_or_more,
    confirmed_cases_male,
    total_tested_male,
    confirmed_cases_female,
    total_tested_female,
    confirmed_cases_unknownleftblank,
    total_tested_unknownleftblank,
    confirmed_cases_aian,
    confirmed_cases_asian,
    confirmed_cases_black,
    confirmed_cases_hispanic,
    confirmed_cases_leftblank,
    confirmed_cases_nhpi,
    confirmed_cases_other,
    confirmed_cases_white,
    total_tested_aian,
    total_tested_asian,
    total_tested_black,
    total_tested_hispanic,
    total_tested_leftblank,
    total_tested_nhpi,
    total_tested_other,
    total_tested_white
    FROM public.zipcode_testing_results
        LEFT OUTER JOIN public.census_geographies ON public.zipcode_testing_results.census_geography_id = public.census_geographies.id
        LEFT OUTER JOIN public.zipcode_date_age_counts ON (
            public.zipcode_testing_results.date = public.zipcode_date_age_counts.date
            AND public.zipcode_testing_results.zipcode = public.zipcode_date_age_counts.zipcode
        )
        LEFT OUTER JOIN public.zipcode_date_gender_counts ON (
            public.zipcode_testing_results.date = public.zipcode_date_gender_counts.date
            AND public.zipcode_testing_results.zipcode = public.zipcode_date_gender_counts.zipcode
        )
        LEFT OUTER JOIN public.zipcode_date_race_counts ON (
            public.zipcode_testing_results.date = public.zipcode_date_race_counts.date
            AND public.zipcode_testing_results.zipcode = public.zipcode_date_race_counts.zipcode
        )
    GROUP BY
        public.zipcode_testing_results.date,
        public.zipcode_testing_results.zipcode,
        census_geography_id,
        total_tested,
        confirmed_cases,
        public.census_geographies.id,
        public.zipcode_date_age_counts.id,
        public.zipcode_date_gender_counts.id,
        public.zipcode_date_race_counts.id
    ORDER BY date DESC;
