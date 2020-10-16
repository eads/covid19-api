const fetch = require("node-fetch");
const slugify = require("slugify");
const { query } = require("../lib/api");
const { backupToS3 } = require("../lib/backup");
const { unzip } = require("lodash");
const asyncPool = require("tiny-async-pool");

const sourceURL =
  "https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetZip";

const demographicsURL =
  "https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetZipDemographics?zipCode=";

zipQuery = `
    mutation($zipcodes: [zipcodes_insert_input!]!,
             $counts: [zipcode_testing_results_insert_input!]!,
             $raceCounts: [zipcode_date_race_counts_insert_input!]!,
             $genderCounts: [zipcode_date_gender_counts_insert_input!]!,
             $ageCounts: [zipcode_date_age_counts_insert_input!]!) {
        insert_zipcodes(
            objects: $zipcodes,  
            on_conflict: {
                constraint: zipcodes_pkey,
                update_columns: [zipcode]
            }
        ) {
            affected_rows
        }
        insert_zipcode_testing_results(
            objects: $counts,  
            on_conflict: {
                constraint: zipcode_testing_results_date_zipcode_key,
                update_columns: [confirmed_cases, total_tested, census_geography_id]
            }
        ) {
            affected_rows
        }
        insert_zipcode_date_race_counts(
            objects: $raceCounts,  
            on_conflict: {
                constraint: zipcode_date_race_counts_zipcode_date_key,
                update_columns: []
            }
        ) {
            affected_rows
        }
        insert_zipcode_date_gender_counts(
            objects: $genderCounts,  
            on_conflict: {
                constraint: zipcode_date_gender_counts_zipcode_date_key,
                update_columns: []
            }
        ) {
            affected_rows
        }
        insert_zipcode_date_age_counts(
            objects: $ageCounts,  
            on_conflict: {
                constraint: zipcode_date_age_counts_zipcode_date_key,
                update_columns: []
            }
        ) {
            affected_rows
        }
    }
`;

const censusIdQuery = `
  query {
    census_geographies(where: {geography: {_eq: "zcta"}}) {
      id
      geoid
    }
  }
`;

function flattenRaceDemographics(accumulator, demographic) {
  const slug = slugify(demographic.description, {
    replacement: "",
    strict: true,
    lower: true,
  });
  accumulator[`confirmed_cases_${slug}`] = demographic.count;
  accumulator[`total_tested_${slug}`] = demographic.tested;
  return accumulator;
}

function flattenGenderDemographics(accumulator, demographic) {
  const slug = slugify(demographic.description, {
    replacement: "",
    strict: true,
    lower: true,
  });
  accumulator[`confirmed_cases_${slug}`] = demographic.count;
  accumulator[`total_tested_${slug}`] = demographic.tested;
  return accumulator;
}

function flattenAgeDemographics(accumulator, demographic) {
  const slug = demographic.age_group
    .replace("<", "less_than_")
    .replace("+", "_or_more")
    .replace("-", "_to_")
    .trim()
    .toLowerCase();

  accumulator[`confirmed_cases_${slug}`] = demographic.count;
  accumulator[`total_tested_${slug}`] = demographic.tested;
  return accumulator;
}

async function loadDay(zipData) {
  const {
    data: { census_geographies: censusGeographies },
  } = await query({ query: censusIdQuery });

  const censusIdMap = censusGeographies.reduce(
    (acc, { geoid, id }) => ({
      ...acc,
      [geoid]: id,
    }),
    {}
  );

  const updatedDate = `${zipData.lastUpdatedDate.year}-${zipData.lastUpdatedDate.month}-${zipData.lastUpdatedDate.day}`;

  // Generate objects for all queries in one loop
  const objects = zipData.zip_values.map((d) => {
    const census_geography_id = censusIdMap[d.zip] || null;

    const zipTestData = [
      {
        // ZIP codes + date-ZIP junction
        zipcode: d.zip,
        daily_counts: {
          data: [
            {
              date: updatedDate,
            },
          ],
          on_conflict: {
            constraint: "zipcode_date_pkey",
            update_columns: [],
          },
        },
      },
      {
        // ZIP code date counts
        zipcode: d.zip,
        date: updatedDate,
        confirmed_cases: d.confirmed_cases,
        total_tested: d.total_tested,
        census_geography_id,
      },
    ];

    let zipDemData = [null, null, null];

    if (d.demographics && "age" in d.demographics) {
      // Flatten race
      const raceRow = d.demographics.race.reduce(flattenRaceDemographics, {});

      // Flatten gender
      const genderRow = d.demographics.gender.reduce(
        flattenGenderDemographics,
        {}
      );

      // Flatten age
      const ageRow = d.demographics.age.reduce(flattenAgeDemographics, {});

      zipDemData = [
        {
          // ZIP code date counts for race groups
          zipcode: d.zip,
          date: updatedDate,
          ...raceRow,
        },
        {
          // ZIP code date counts for gender groups
          zipcode: d.zip,
          date: updatedDate,
          ...genderRow,
        },
        {
          // ZIP code date counts for age groups
          zipcode: d.zip,
          date: updatedDate,
          ...ageRow,
        },
      ];
    }

    return [...zipTestData, ...zipDemData];
  });

  // Collect array columns as variables for query input
  const [zipcodes, counts, raceVals, genderVals, ageVals] = unzip(objects);

  // Remove empty values for demographics
  const [raceCounts, genderCounts, ageCounts] = [
    raceVals,
    genderVals,
    ageVals,
  ].map((vals) => vals.filter((v) => v !== null));

  return query({
    query: zipQuery,
    variables: { zipcodes, counts, raceCounts, genderCounts, ageCounts },
  })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));
}

async function loadZIPCodesAlt() {
  const { lastUpdatedDate, zip_values } = await fetch(sourceURL).then((res) =>
    res.json()
  );
  const zipCodes = zip_values.map(({ zip }) => zip);
  // Combine all ZIP code demographic data into a mapping of ZIP codes to demographics
  const demographicsMap = (
    await asyncPool(2, zipCodes, (zip) =>
      fetch(`${demographicsURL}${zip}`)
        .then((res) => res.json())
        .then((data) => ({ ...data, zip }))
        .catch((e) => {
          console.error(e);
          return { zip };
        })
    )
  ).reduce((acc, { zip, ...curr }) => ({ ...acc, [zip]: curr }), {});

  const zipData = zip_values.map(({ zip, ...d }) => ({
    ...d,
    zip,
    demographics: demographicsMap[zip],
  }));

  backupToS3(
    "GetZip.json",
    JSON.stringify({
      lastUpdatedDate,
      zip_values: zipData,
    })
  );

  await loadDay({
    lastUpdatedDate,
    zip_values: zipData,
  });
}

loadZIPCodesAlt();
