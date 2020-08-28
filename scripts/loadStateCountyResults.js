const fs = require("fs");
const fetch = require("node-fetch");
const { query } = require("../lib/api");
const { backupToS3 } = require("../lib/backup");
const { STATE, COUNTY, PLACE } = require("../lib/constants");

const ENDPOINT = "https://www.dph.illinois.gov/sitefiles/COVIDTestResults.json";

const stateCountyResultsQuery = `
  mutation(
    $raceCounts: state_race_counts_insert_input!,
    $ageCounts: state_age_counts_insert_input!,
    $ageRaceCounts: state_age_race_counts_insert_input!,
    $genderCounts: state_gender_counts_insert_input!,
    $stateResults: [state_testing_results_insert_input!]!,
    $stateRecovery: [state_recovery_data_insert_input!]!,
    $probableCaseCounts: state_probable_case_counts_insert_input!
    $countyResults: [county_testing_results_insert_input!]!,
  ) {
    insert_state_race_counts(
      objects: [$raceCounts],
      on_conflict: {
        constraint: state_race_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_age_counts(
      objects: [$ageCounts],
      on_conflict: {
        constraint: state_age_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_age_race_counts(
      objects: [$ageRaceCounts],
      on_conflict: {
        constraint: state_age_race_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_gender_counts(
      objects: [$genderCounts],
      on_conflict: {
        constraint: state_gender_counts_date_key,
        update_columns: []
      }
    ) {
      affected_rows
    }
    insert_state_testing_results(
      objects: $stateResults,
      on_conflict: {
        constraint: state_testing_results_date_key,
        update_columns: [total_tested, confirmed_cases, deaths]
      }
    ) {
      affected_rows
    }
    insert_state_recovery_data(
      objects: $stateRecovery,
      on_conflict: {
        constraint: state_recovery_data_report_date_key,
        update_columns: [sample_surveyed, recovered_cases, recovered_and_deceased_cases, recovery_rate]
      }
    ) {
      affected_rows
    }
    insert_state_probable_case_counts(
      objects: [$probableCaseCounts],
      on_conflict: {
        constraint: state_probable_case_counts_date_key,
        update_columns: [probable_cases, probable_deaths]
      }
    ) {
      affected_rows
    }
    insert_county_testing_results(
      objects: $countyResults,
      on_conflict: {
        constraint: county_testing_results_date_county_key,
        update_columns: [confirmed_cases, deaths, total_tested, census_geography_id]
      }
    ) {
      affected_rows
    }
  }
`;

const censusIdQuery = `
  query {
    census_geographies {
      id
      name
      geography
    }
  }
`;

const transformData = ({
  LastUpdateDate: { year, month, day },
  characteristics_by_county: { values: countyValues },
  state_testing_results: { values: stateResultValues },
  demographics,
  state_recovery_data: { values: stateRecoveryValues },
  probable_case_counts: probableCaseValues,
}) => ({
  date: `${year}-${month}-${day}`,
  countyValues,
  stateResultValues,
  demographics,
  stateRecoveryValues,
  probableCaseValues,
});

// Converts date strings in m/d/yyyy format to yyyy-mm-dd
const transformDate = (dateStr) => {
  const [month, day, year] = dateStr.split("/");
  return [year, month, day].join("-");
};

const demographicsRowSlug = ({ age_group, race, gender }) =>
  [
    age_group
      ? age_group
          .replace("<", "less_than_")
          .replace("+", "_or_more")
          .replace("-", "_to_")
      : null,
    race,
    gender,
  ]
    .filter((s) => !!s)
    .map((s) => s.toLowerCase().replace(/[^a-z0-9\_]/g, ``))
    .join("_");

const flattenDemographics = (
  acc,
  { age_group, race, gender, count, tested, deaths }
) => {
  const slug = demographicsRowSlug({ age_group, race, gender });
  return {
    ...acc,
    [`count_${slug}`]: count,
    [`tested_${slug}`]: tested,
    [`deaths_${slug}`]: deaths,
  };
};

const transformDemographics = ({ age, race, gender }) => [
  ...age.map(({ age_group, count, tested, deaths }) => ({
    age_group,
    race: null,
    gender: null,
    count,
    tested,
    deaths,
  })),
  ...age
    .map(({ age_group, demographics: { race: ageRaceDemographics } }) =>
      ageRaceDemographics.map(({ description, count, tested, deaths }) => ({
        age_group,
        race: description,
        gender: null,
        count,
        tested,
        deaths,
      }))
    )
    .flat(),
  ...race.map(({ description, count, tested, deaths }) => ({
    age_group: null,
    race: description,
    gender: null,
    count,
    tested,
    deaths,
  })),
  ...gender.map(({ description, count, tested, deaths }) => ({
    age_group: null,
    race: null,
    gender: description,
    count,
    tested,
    deaths,
  })),
];

const transformStateResults = ({
  testDate,
  total_tested,
  confirmed_cases,
  deaths,
}) => ({
  date: transformDate(testDate),
  total_tested,
  confirmed_cases,
  deaths,
});

const transformStateRecovery = ({
  report_date,
  sample_surveyed,
  recovered_cases,
  recovered_and_deceased_cases,
  recovery_rate,
}) => ({
  report_date: transformDate(report_date),
  sample_surveyed,
  recovered_cases,
  recovered_and_deceased_cases,
  recovery_rate,
});

const transformProbableCaseCounts = ({
  LastUpdatedDate: date,
  probable_cases,
  probable_deaths,
}) => ({
  date: transformDate(date),
  probable_cases,
  probable_deaths,
});

const transformCounty = ({
  County: county,
  confirmed_cases,
  deaths,
  total_tested,
}) => ({ county, confirmed_cases, deaths, total_tested });

const processCountyCensusGeographyId = ({ county }, censusIdMap) => {
  if (county === "Illinois") {
    return censusIdMap[`${STATE}${county}`];
  } else if (county === "Chicago") {
    return censusIdMap[`${PLACE}${county}`];
  } else {
    return censusIdMap[`${COUNTY}${county}`];
  }
};

async function loadStateCountyResults() {
  const {
    data: { census_geographies: censusGeographies },
  } = await query({ query: censusIdQuery });

  const censusIdMap = censusGeographies.reduce(
    (acc, { name, geography, id }) => ({
      ...acc,
      [`${geography}${name}`]: id,
    }),
    {}
  );

  const data = await fetch(ENDPOINT).then((res) => res.json());

  backupToS3("COVIDTestResults.json", JSON.stringify(data));

  const {
    date,
    demographics: { age, race, gender },
    countyValues,
    stateResultValues,
    stateRecoveryValues,
    probableCaseValues,
  } = transformData(data);

  const demographicCounts = transformDemographics({ age, race, gender });
  const demographicCountBase = { date };
  const raceCounts = demographicCounts
    .filter(({ age_group, race, gender }) => !!race && !(age_group || gender))
    .reduce(flattenDemographics, demographicCountBase);
  const ageCounts = demographicCounts
    .filter(({ age_group, race, gender }) => !!age_group && !(race || gender))
    .reduce(flattenDemographics, demographicCountBase);
  const ageRaceCounts = demographicCounts
    .filter(({ age_group, race, gender }) => age_group && race && !gender)
    .reduce(flattenDemographics, demographicCountBase);
  const genderCounts = demographicCounts
    .filter(({ age_group, race, gender }) => gender && !(age_group || race))
    .reduce(flattenDemographics, demographicCountBase);

  const stateResults = stateResultValues.map(transformStateResults);
  const stateRecovery = stateRecoveryValues.map(transformStateRecovery);
  const probableCaseCounts = transformProbableCaseCounts(probableCaseValues);

  const countyResults = countyValues.map(transformCounty).map((county) => ({
    ...county,
    census_geography_id: processCountyCensusGeographyId(county, censusIdMap),
    date,
  }));

  await query({
    query: stateCountyResultsQuery,
    variables: {
      raceCounts,
      ageCounts,
      ageRaceCounts,
      genderCounts,
      stateResults,
      stateRecovery,
      probableCaseCounts,
      countyResults,
    },
  })
    .then(console.log)
    .catch(console.error);
}

loadStateCountyResults();
