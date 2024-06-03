const { getPostgresData, getSnowflakeData, closePostgresConnection, closeSnowflakeConnection } = require('./Migration_Class');
const moment = require('moment');
const fsPromises = require('fs').promises;
const pgQueries = require('./public.json');
const fs = require('fs');

// Create a write stream to a log file
const logFilePath = './log.txt';
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

// Custom logging function
console.log = function (...args) {
    return new Promise(async (resolve) => {
        const formattedMessage = args.map(arg => {
            if (typeof arg === 'string') {
                return arg;
            } else {
                return JSON.stringify(arg, null, 2);
            }
        }).join('\n');

        await logStream.write(`${formattedMessage}\n`);
        process.stdout.write(`${formattedMessage}\n`); // Also print to the console

        resolve();
    });
};
//To handel the round off 
function roundToPrecision(value, precision) {
    const multiplier = Math.pow(10, precision);
    return Math.round(value * multiplier) / multiplier;
}
//To handel the Json String
function parseJSONSafe(jsonString) {
    try {
        return JSON.parse(jsonString);
    } catch (error) {
        // Return the original string if parsing fails
        return jsonString;
    }
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
//To check the time difference for timezone
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
    const expectedTimeDifferenceMs = [19800000, 0]; // Adjust this value as needed 5hrs:30min 

    Object.keys(pgRow).forEach(column => {
        const pgValue = pgRow[column];
        const sfValue = sfRow[column];

        // Check if the column needs date and time normalization
        const normalizeDateTimeColumn =
            ['created_at', 'updated_date', 'updated_time', 'date_of_acquisition', 'custom_fields_mc', 'insertion_time', 'updated_at', 'tags', 'campaign_start_date', 'campaign_end_date', 'charge_date', 'custom_fields_tr',
                'persona_created_at', 'primarykeyvaluepair', 'form_json', 'error', 'start_date', 'request_json', 'persona_date', 'lead_data', 'lead_creation_date', 'row_creation_date', 'timestamp', 'timestamp_to_listener', 'timestamp_to_sqs',
                'report_until', 'report_since', 'created_on', 'scheduler_started', 'scheduler_completed', 'end_date', 'last_login_at', 'missing_data_info', 'updated_cons_info_json', 'start_time', 'end_time', 'started_at', 'ended_at',
                'installed_on', 'filter_conditions', 'selected_columns', 'ad_stop_date', 'ad_live_date', 'lead_forum_ids', 'event_start_date', 'event_end_date', 'report_start_date', 'report_end_date', 'task_start_at', 'task_ended_at', 'file_names',
                'launch_date', 'access_token_date', 'expires_at', 'campaign_json_log', 'payload', 'last_updated', 'created_date', 'campaign_json', 'to_date', 'from_date', 'ad_json', 'interaction_date', 'unique_person_fields', 'subscribed_date', 'last_lead_creation_date'];
        if (normalizeDateTimeColumn.includes(column)) {
            try {
                // Normalize Json columns 
                const specialCases = ['tags', 'custom_fields_mc', 'custom_fields_tr', 'primarykeyvaluepair', 'form_json', 'error', 'request_json', 'lead_data', 'missing_data_info', 'updated_cons_info_json',
                    'filter_conditions', 'selected_columns', 'lead_forum_ids', 'campaign_json_log', 'payload', 'campaign_json', 'ad_json', 'unique_person_fields', 'file_names'];
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
                        // Skip adding differences for this column
                        return;
                    } else {
                        console.log("timeDifference not matches : ", timeDifference.totalMilliseconds);
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

            const columnsToRound = ['total_donated', 'donation_total', 'percent_of_goal', 'quality_score', 'emotional_quotient', 'total_raised', 'page_rank', 'degree_centrality', 'degree_betweeness'];

            // Check if the current column needs rounding
            if (columnsToRound.includes(column)) {
                const precision = 8;
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
            } else {
                // Normalize null and empty string
                const normalizedPgValue = pgValue === null ? '' : pgValue;
                const normalizedSfValue = sfValue === null ? '' : sfValue;

                // if (normalizedPgValue !== normalizedSfValue)  //to handle different data type 
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

async function readQueriesFromFile(filePath) {
    try {
        const fileContent = await fsPromises.readFile(filePath, 'utf-8');
        return JSON.parse(fileContent);
    } catch (error) {
        console.error(`Error reading queries from file: ${error.message}`);
        throw error;
    }
}

async function compareDataForTable(tableName, pgQuery, sfQuery) {
    console.log(`Comparing data for table ${tableName}...`);
    try {

        await closePostgresConnection();
        await closeSnowflakeConnection();

        const pgData = await getPostgresData(pgQuery);
        const sfData = await getSnowflakeData(sfQuery);

        await compareTableData(tableName, pgData, sfData);

    } catch (error) {
        console.error(`Error comparing data for table ${tableName}: ${error.message}`);
    }
}

async function compareTablesFromQueries(queries) {
    for (const { tableName, pgQuery, sfQuery } of queries) {
        await compareDataForTable(tableName, pgQuery, sfQuery);
    }
}

async function compareTableData(tableName, pgData, sfData) {
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
    // const pgRows = pgData.map(row => normalizeDataRow(row));
    // const sfRows = sfData.map(row => normalizeDataRow(row));

    const differences = [];

    // Assuming both datasets have the same length
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
        console.log(`Data for table ${tableName} matches between PostgreSQL and Snowflake.`);

    } else {
        console.log(`Data for table ${tableName} does not match.`);
        console.log('Differences:', JSON.stringify(differences, null, 2));
    }

}
// ./public.json
async function main() {
    try {
        const filePath = 'Queries/public.json'; // Specify the path to your JSON file
        const queries = await readQueriesFromFile(filePath);
        await compareTablesFromQueries(queries);
    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        logStream.end();
    }
}

main();


