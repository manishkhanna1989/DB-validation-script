const { getPostgresData, getSnowflakeData } = require('./Migration_Class');
const moment = require('moment');
const pgTableName = 'dash_reporting_all_time';  // Replace with your actual PostgreSQL table name
const sfTableName = 'DASH_REPORTING_ALL_TIME_V'; // Replace with your actual Snowflake table name

const sfQuery = (`
SELECT * FROM DB_GU_DWH_CLONE_QA.CORE_TABLES.${sfTableName} order by "id" desc limit 10000; 
`);
// SELECT * FROM DB_GU_SOURCE.PUBLIC.${pgTableName} ORDER BY eid desc LIMIT 100;
const pgQuery = (`
select * from dashboard.gu_dw.${pgTableName} order by "id" desc limit 10000;
`);

function parseJSONSafe(jsonString) {
    try {
        return JSON.parse(jsonString);
    } catch (error) {
        // Return the original string if parsing fails
        return jsonString;
    }
}
function roundToPrecision(value, precision) {
    const multiplier = Math.pow(10, precision);
    return Math.round(value * multiplier) / multiplier;
}

// Helper function to compare sets
function compareSets1(set1, set2) {
    if (set1.size !== set2.size) {
        return false;
    }

    for (const item of set1) {
        if (!set2.has(item)) {
            return false;
        }
    }

    return true;
}
function calculateTimeDifference(pgValue, sfValue) {
    const pgDate = new Date(pgValue);
    const sfDate = new Date(sfValue);

    // Calculate the time difference in milliseconds
    const timeDifferenceMs = sfDate - pgDate;

    // Convert the time difference to a readable format
    const timeDifference = new Date(timeDifferenceMs);

    // Extract hours, minutes, and seconds
    const hours = timeDifference.getUTCHours();
    const minutes = timeDifference.getUTCMinutes();
    const seconds = timeDifference.getUTCSeconds();

    return {
        hours,
        minutes,
        seconds,
        totalMilliseconds: timeDifferenceMs,
    };
}

function compareDataRows(pgRow, sfRow) {
    const differences = [];
    const expectedTimeDifferenceMs = [19800000,0]; // Adjust this value as needed 5hrs:30min 
        
    Object.keys(pgRow).forEach(column => {        
        const pgValue = pgRow[column];
        const sfValue = sfRow[column];

        // Check if the column needs date and time normalization
        const normalizeDateTimeColumn = 
        ['created_at', 'updated_date', 'updated_time', 'date_of_acquisition', 'custom_fields_mc','insertion_time','updated_at', 'tags','campaign_start_date','campaign_end_date','charge_date','custom_fields_tr',
          'persona_created_at','primarykeyvaluepair','form_json','error','start_date','request_json','persona_date','lead_data','lead_creation_date','row_creation_date','timestamp','timestamp_to_listener','timestamp_to_sqs',
        'report_until','report_since','created_on','scheduler_started','scheduler_completed','end_date','last_login_at','missing_data_info','updated_cons_info_json','start_time','end_time','started_at','ended_at',
        'installed_on','filter_conditions','selected_columns','ad_stop_date','ad_live_date','lead_forum_ids','event_start_date','event_end_date','report_start_date','report_end_date','task_start_at','task_ended_at','file_names',
        'launch_date','access_token_date','expires_at','campaign_json_log','payload','last_updated','created_date','campaign_json','to_date','from_date','ad_json','interaction_date','unique_person_fields','subscribed_date','last_lead_creation_date','date_of_lead_acquisition','persona','repeat_previous_date','unsubscribed_date'];
          if (normalizeDateTimeColumn.includes(column)) {
            try {
                // Normalize Json columns 
                const specialCases = ['tags', 'custom_fields_mc', 'custom_fields_tr', 'primarykeyvaluepair','form_json','error','request_json','lead_data','missing_data_info','updated_cons_info_json',
                                      'filter_conditions','selected_columns','lead_forum_ids','file_names','campaign_json_log','payload','campaign_json','ad_json','unique_person_fields','persona'];
                const isSpecialCase = specialCases.includes(column);
        
                const normalizedPgValue = isSpecialCase ? parseJSONSafe(pgValue) : pgValue;
                const normalizedSfValue = isSpecialCase ? parseJSONSafe(sfValue) : sfValue;
        
                // Handle special cases
                if (isSpecialCase) {
                    // Compare the values of the individual fields within the JSON structure
                    if (normalizedPgValue && normalizedSfValue) {
                        const pgFields = Object.keys(normalizedPgValue);
                        const sfFields = Object.keys(normalizedSfValue);
        
                        // Check if the sets of fields are equal, irrespective of order
                        const setsEqual = compareSets1(new Set(pgFields), new Set(sfFields));
        
                        if (!setsEqual) {
                            differences.push({
                                column,
                                fieldDifferences: pgFields,
                            });
                        }
                    }
                  }
            else {
                    // Check for time difference for other datetime columns
                    const timeDifference = calculateTimeDifference(pgValue, sfValue);

                    if (expectedTimeDifferenceMs.includes(timeDifference.totalMilliseconds)) {
                        // console.log(`Time Difference matches as expected for column ${column}`);
                        // Skip adding differences for this column
                        return;
                    }else{
                            console.log("timeDifference",timeDifference.totalMilliseconds);
                    }
                    
                    // Compare the normalized values for non-JSON datetime columns
                    if (normalizedPgValue !== normalizedSfValue) {
                        differences.push({
                            column,
                            pgValue: normalizedPgValue,
                            sfValue: normalizedSfValue,
                        });
                    }
                }
            } catch (error) {
                console.error(`Error parsing JSON for column ${column}:`, error);
            }
        } else {

            const columnsToRound = ['total_donated','donation_total','percent_of_goal','quality_score','emotional_quotient','total_raised',
             'page_rank','degree_centrality','degree_betweeness','repeat_previous_raised','percent_of_goal'];

             const columnsToTrim = ['fundraiser_title','full_name','last_name','gift_aid_donation_country'];

            // Check if the current column needs rounding
            if (columnsToRound.includes(column)) {
            
                const precision = 6;

                // Round the decimal values before comparison
                const roundedPgValue = roundToPrecision(pgValue, precision);
                const roundedSfValue = roundToPrecision(sfValue, precision);

                if (roundedPgValue !== roundedSfValue) {
                    differences.push({
                        column,
                        normalizedPgValue: roundedPgValue,
                        normalizedSfValue: roundedSfValue,
                    });
                }    
            } else if(columnsToTrim.includes(column)){
                //Remove extra spaces at the beginning and end of text
                const trimmedPgValue1 = pgValue === null ? '' : pgValue.toString().trim();
                const trimmedSfValue1 = sfValue === null ? '' : sfValue.toString().trim();

                if (trimmedPgValue1 != trimmedSfValue1) {
                    differences.push({
                        column,
                        normalizedPgValue: trimmedPgValue1,
                        normalizedSfValue: trimmedSfValue1,
                    });
                }   
                    
            } 
            
            else {
            // Normalize null and empty string 
            const normalizedPgValue = pgValue === null ? '' : pgValue;//.toString().trim();
            const normalizedSfValue = sfValue === null ? '' : sfValue;

            // if (normalizedPgValue !== normalizedSfValue)  //to handle different data type is there 
            if (normalizedPgValue != normalizedSfValue) {
                differences.push({
                    column,
                    normalizedPgValue,
                    normalizedSfValue,
                });
            }
        }
}
});

    return differences;
}


async function compareTableData() {
    try {
        const pgData = await getPostgresData(pgQuery);
        const sfData = await getSnowflakeData(sfQuery);

        const pgRows = pgData.map(row => {
            const normalizedRow = {};
            Object.keys(row).forEach(column => {
                normalizedRow[column.toLowerCase()] = row[column];
            });
            return normalizedRow;
        });
        
        const sfRows = sfData.map(row => {
            const normalizedRow = {};
            Object.keys(row).forEach(column => {
                normalizedRow[column.toLowerCase()] = row[column];
            });
            return normalizedRow;
        });
        
        const differences = [];

        for (let i = 0; i < pgData.length; i++) {
            const pgRow = pgRows[i];
            const sfRow = sfRows[i];

            const rowDifferences = compareDataRows(pgRow, sfRow);

            if (rowDifferences.length > 0) {
                differences.push({
                    index: i,
                    differences: rowDifferences,
                });
            }
        }

        if (differences.length === 0) {
            console.log(`Data for table ${pgTableName} matches between PostgreSQL and Snowflake.`);
        } else {
            console.log(`Data for table ${pgTableName} does not match.`);
            console.log('Differences:', JSON.stringify(differences, null, 2));
        }

    } catch (error) {
        console.error(error);
    }
}

compareTableData();

