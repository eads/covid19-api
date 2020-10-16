const fetch = require('node-fetch');
const { query } = require('../lib/api');
const { backupToS3 } = require("../lib/backup");
const { unzip } = require('lodash');

const sourceURL = 'https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetZip'

zipQuery = `
    mutation($zipcodes: [zipcodes_insert_input!]!,
             $counts: [zipcode_testing_results_insert_input!]!) {
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
    const objects = zipData.zip_values.map(d => {
        const census_geography_id = censusIdMap[d.zip] || null

        return [
            {   // ZIP codes + date-ZIP junction
                zipcode: d.zip,
                daily_counts: {
                    data: [{
                        date: updatedDate,
                    }],
                    on_conflict: {
                        constraint: 'zipcode_date_pkey',
                        update_columns: []
                    }
                },
            },
            {   // ZIP code date counts
                zipcode: d.zip,
                date: updatedDate,
                confirmed_cases: d.confirmed_cases,
                total_tested: d.total_tested,
                census_geography_id,
            },
        ]
    });

    // Collect array columns as variables for query input
    const [ zipcodes, counts, ] = unzip(objects);

    return query({
        query: zipQuery,
        variables: { zipcodes, counts, },
    })
    .then((response) => console.log(response))
    .catch((error) => console.error(error));
}

async function loadZIPCodesAlt() {
    const data = await fetch(sourceURL).then(res => res.json())

    backupToS3("GetZip.json", JSON.stringify(data));

    loadDay(data)
}

loadZIPCodesAlt()
