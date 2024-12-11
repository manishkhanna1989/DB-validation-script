const { getPostgresData, getSqlData, closePostgresConnection, closeSqlConnection } = require('../Migration_Class');
const moment = require('moment');
const fsPromises = require('fs').promises;
const fs = require('fs');
const { fail } = require('assert');
// const database = 'FundAccounting';
const database = process.env.sqlDatabase
const logFilePath = `Log/${database}_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
let resultStatus = '';
let timestamp, startDate, endDate;
const filePath = 'Queries/FA_batch.json';
const csvPath = `Result_csv/${database}_Data_Validation_batch.csv`;

const normalizeDateTimeColumn =
[ 'rundate','murexendtime','modifiedat', 'createdat','updatedat','approvedat','validfrom','validto','date','adjustmentdate','fxdate','closedate','expirydate','cobdate'];
const columnsToRound = ['financeytd','fxrate','pnlmtd','dtdreturn','ytdreturn','mtdreturn','pnlytd','pnldtd','adjustmentdtd','adjustmentmtd','adjustmentytd'];


console.log = function (...args) {
    return new Promise(async (resolve) => {
        timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
        const formattedMessage = args.map(arg => {
            if (typeof arg === 'string') {
                return arg;
            } else {
                return JSON.stringify(arg, null, 2);
            }
        }).join('\n');

        logStream.write(`${timestamp} - ${formattedMessage}\n`);
        process.stdout.write(`${timestamp} - ${formattedMessage}\n`); // Also print to the console

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
function calculateTimeDifference(pgValue, SqlValue) {
    const pgDate = new Date(pgValue);
    const sqlDate = new Date(SqlValue);

    // Calculate the time difference in milliseconds
    const timeDifferenceMs = sqlDate - pgDate;

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

function compareDataRows(pgRow, sqlRow) {
    const differences = [];
    const expectedTimeDifferenceMs = [19800000, 0, 19799999]; // Adjust this value as needed 5hrs:30min 

    Object.keys(pgRow).forEach(column => {
        const pgValue = pgRow[column];
        const SqlValue = sqlRow[column];

        // Check if the column needs date and time normalization
        // const normalizeDateTimeColumn =
        //     [ 'createdat','updatedat','approvedat','validfrom','validto','date','adjustmentdate','fxdate','closedate','expirydate','cobdate'];
        
        if (normalizeDateTimeColumn.includes(column)) {
            try {
                // Normalize Json columns 
                const specialCases = [ 'ad_json'];
                const isSpecialCase = specialCases.includes(column);

                const normalizedPgValue = isSpecialCase ? parseJSONSafe(pgValue) : pgValue;
                const normalizedSqlValue = isSpecialCase ? parseJSONSafe(SqlValue) : SqlValue;

                // Handle special cases
                if (isSpecialCase) {
                    // Compare the values of the individual fields within the JSON structure
                    if (normalizedPgValue && normalizedSqlValue) {
                        const pgFields = Object.keys(normalizedPgValue);
                        const sfFields = Object.keys(normalizedSqlValue);

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
                    const timeDifference = calculateTimeDifference(pgValue, SqlValue);

                    if (expectedTimeDifferenceMs.includes(timeDifference.totalMilliseconds)) {
                        // Skip adding differences for this column
                        return;
                    } else {
                        console.log("timeDifference not matches : ", timeDifference.totalMilliseconds);
                    }
                    // Compare the normalized values for non-JSON datetime columns
                    if (normalizedPgValue !== normalizedSqlValue) {
                        differences.push({
                            column,
                            pgValue: normalizedPgValue,
                            SqlValue: normalizedSqlValue,
                        });
                    }
                }
            } catch (error) {
                console.error(`Error parsing JSON for column ${column}:`, error);
            }
        } else {

            // const columnsToRound = ['fxrate','pnlmtd','dtdreturn','ytdreturn','mtdreturn','pnlytd','pnldtd','adjustmentdtd','adjustmentmtd','adjustmentytd'];

            // Check if the current column needs rounding
            // if (columnsToRound.includes(column)) {
            //     const precision = 6;
            //     // Round the decimal values before comparison
            //     const roundedPgValue = roundToPrecision(pgValue, precision);
            //     const roundedSqlValue = roundToPrecision(SqlValue, precision);

            //     if (roundedPgValue !== roundedSqlValue) {
            //         differences.push({
            //             column,
            //             normalizedPgValue: roundedPgValue,
            //             normalizedSqlValue: roundedSqlValue,
            //         });
            //     }

            if (columnsToRound.includes(column)) {
                let precision = 7; // Start with a high precision
                let minPrecision = 5; // Set a minimum precision limit
            
                let roundedPgValue = roundToPrecision(pgValue, precision);
                let roundedSqlValue = roundToPrecision(SqlValue, precision);
            
                // Decrease precision until the rounded values are equal or the minimum precision is reached
                while (roundedPgValue !== roundedSqlValue && precision > minPrecision) {
                    precision--;
                    roundedPgValue = roundToPrecision(pgValue, precision);
                    roundedSqlValue = roundToPrecision(SqlValue, precision);
                }
            
                // If the rounded values are still not equal, record the difference
                if (roundedPgValue !== roundedSqlValue) {
                    differences.push({
                        column,
                        normalizedPgValue: roundedPgValue,
                        normalizedSqlValue: roundedSqlValue,
                    });
                }
            
            } else {
                // Normalize null and empty string
                const normalizedPgValue = pgValue === null ? '' : pgValue;
                const normalizedSqlValue = SqlValue === null ? '' : SqlValue;

                // if (normalizedPgValue !== normalizedSqlValue)  //to handle different data type 
                if (normalizedPgValue != normalizedSqlValue) {
                    differences.push({
                        column,
                        normalizedPgValue,
                        normalizedSqlValue,
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

async function compareDataForTable(tableName, database, pgQueryTemplate, sfQueryTemplate, year ) {
    try {
        await closePostgresConnection();
        await closeSqlConnection();

        console.log(`Comparing data for table ${tableName}...`);
        
        for (let month = 0; month < 12; month++) {
            startDate = moment(`${year}-01-01`).add(month, 'months').startOf('month').format('YYYY-MM-DD');
            endDate = moment(startDate).endOf('month').format('YYYY-MM-DD');

            const pgQuery = pgQueryTemplate.replace('${startDate}', startDate).replace('${endDate}', endDate);
            const sfQuery = sfQueryTemplate.replace('${startDate}', startDate).replace('${endDate}', endDate);
            // console.log('pgQuery',pgQuery);
            const pgData = await getPostgresData(database, pgQuery);
            const sfData = await getSqlData(database, sfQuery);

            await compareTableData(tableName, pgData, sfData);

            console.log(`Processed batch from ${startDate} to ${endDate}`);
        }

    } catch (error) {
        console.error(`Error comparing data for table ${tableName}: ${error.message}`);
    }
}

async function compareTablesFromQueries(queries) {
    for (const { tableName, pgQuery, sfQuery } of queries) {
        const year = tableName.split('_')[1];   //.split('-')[0];
        await compareDataForTable(tableName, database, pgQuery, sfQuery, year);
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

    const sqlRows = sfData.map(row => {
        const normalizedRow = {};
        Object.keys(row).forEach(column => {
            normalizedRow[column.toLowerCase()] = row[column];
        });
        return normalizedRow;
    });
    const differences = [];

    if(sfData.length !== pgData.length){
        console.log(`Row Count for table ${tableName} does not match. SQL Count:${sfData.length} VS PG Count:${pgData.length}`);
    }
    // Assuming both datasets have the same length
    for (let i = 0; i < sfData.length; i++) {
        const pgRow = pgRows[i];
        const sqlRow = sqlRows[i];

        const rowDifferences = compareDataRows(pgRow, sqlRow);

        if (rowDifferences.length > 0) {
            differences.push({
                index: i,
                differences: rowDifferences,
            });
        }
    }

    if (differences.length === 0) {
        console.log(`Data for table ${tableName} matches between PostgreSQL and Sql.`);
    } else {
        console.log(`Data for table ${tableName} does not match.`);
        console.log('Differences:', JSON.stringify(differences, null, 2));
    }

    resultStatus = differences.length === 0 ? 'Pass' : 'Fail';
    const comment = resultStatus === 'Pass' ? `Data for table ${tableName} matches between PostgreSQL and Sql. Processed batch from ${startDate} to ${endDate}` : `Data for table ${tableName} does not match. Processed batch from ${startDate} to ${endDate}`;
    writeDifferencesToCsv(differences, tableName, resultStatus,comment);

    function writeDifferencesToCsv(differences, tableName, resultStatus, comment){
        const headers = ['Index','TableName','Column','PGValue','SQLValue'];
        const csvRows = differences.flatMap(difference=>
            difference.differences.map(diff =>
                `${difference.index},${tableName},${diff.column},${diff.normalizedPgValue || diff.pgValue},${diff.normalizedSqlValue || diff.SqlValue}`
            )
        );
        const summaryRow = `${timestamp} - Summary, ${tableName}, ${resultStatus}, ${comment}`;
        if (resultStatus === 'Pass'){
            let csvContent2 = [summaryRow].join('\n');
            fs.appendFile(csvPath,csvContent2 + '\n',(err)=>{
                if(err){
                    console.error('Error writing in csv file');
                }
            })  
        }else{
            let csvContent = [summaryRow, headers.join(','), ...csvRows].join('\n');
            fs.appendFile(csvPath, csvContent + '\n',(err)=>{
                if(err){
                    console.error('Error writing in csv file');
                }
            })           
        }
    }
}

async function main() {
    try {
        const queries = await readQueriesFromFile(filePath);
        await compareTablesFromQueries(queries);
    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        logStream.end();
    }
}

main();