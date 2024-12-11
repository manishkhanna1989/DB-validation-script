const { getPostgresData, getSqlData, closePostgresConnection, closeSqlConnection } = require('../Migration_Class');
const moment = require('moment');
const fsPromises = require('fs').promises;
const fs = require('fs');
const { fail } = require('assert');
const database = process.env.sqlDatabase
// const database = 'SVM';
const logFilePath = `Log/${database}_test_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
let resultStatus = '';
let timestamp ;
const filePath = 'Queries/Test.json';
const csvPath = `Result_csv/${database}Data_Validation_test.csv`;

const normalizeDateTimeColumn =
['effectivedate','unbumpedmaturitydate','underlyingeffectivedate', 'underlyingunbumpedeffectivedate','tradedate','date',
    'tradeentrydate','tradelastamenddate', 'paymentdate','lastupdatetime','executiontime','originmktopdate','systemdate','fixingdate','unbumpedeffectivedate','maturitydate','underlyingmaturitydate','cmslastfixingdate','incomeenddate','incomestartdate',
    'lastmktopdate','executiondate','settlementdate','started','completed','fxdate','datecob1','datecob2','datesom','lasttradeabledate','expiry','upfrontpremiumdate','unbumpedunderlyingmaturitydate','barrierstartdate','barrierenddate','bondlegexpirydate','cob1','cob2','eom','eoy','changetime',
    'tradedate','tradeentrydate','tradelastamenddate','executiontime','originmktopdate','originmktopdate','systemdate','effectivedate','maturitydate','paymentdate','settlementdate','lastmktopdate','cmslastfixingdate',
    'lastupdatetime', 'underlyingexpirydate','fixingdate','modifieddateutc',
    'interestcalculationenddate','interestcalculationstartdate','eventdate','inv_manager_date','deletedat', 'fundclosedate', 'holiday','startdate','enddate','rundate','murexendtime','modifiedat','createdat','updatedat','approvedat','validfrom','validto','adjustmentdate','fxdate','closedate','expirydate','cobdate','lastmodified','generateddate','reportdate','publisheddate','statusmodifiedat'];
const columnsToRound = ['fxrate','pnlmtd','dtdreturn','ytdreturn','mtdreturn','pnlytd','pnldtd','adjustmentdtd','adjustmentmtd','adjustmentytd','financeytd'];

// Custom logging function
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
    const expectedTimeDifferenceMs = [19800000, 0, 19799999 ]; // Adjust this value as needed 5hrs:30min 

    Object.keys(pgRow).forEach(column => {
        const pgValue = pgRow[column];
        const SqlValue = sqlRow[column];

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
                        console.log('setsEqual',setsEqual) ; 
                        if (!setsEqual) {
                            console.log('setsEqual',setsEqual)
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

async function compareDataForView(viewName, database, pgQuery, sqlQuery) {
    try {
        await closePostgresConnection();
        await closeSqlConnection();

        console.log(`Comparing data for view: ${viewName}...`);

        const pgData = await getPostgresData(database, pgQuery);
        const sfData = await getSqlData(database, sqlQuery);

        await compareTableData(viewName, pgData, sfData);

    } catch (error) {
        console.error(`Error comparing data for view ${viewName}: ${error.message}`);
    }
}

async function compareTablesFromQueries(queries) {
    for (const { viewName, pgQuery, sfQuery } of queries) {
        await compareDataForView(viewName, database, pgQuery, sfQuery);
    }
}

async function compareTableData(viewName, pgData, sfData) {
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
        console.log(`Row Count for table ${viewName} does not match. SQL Count:${sfData.length} VS PG Count:${pgData.length}`);
    }
    else{
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
        console.log(`Data for View ${viewName} matches between PostgreSQL and Sql.`);

    } else {
        console.log(`Data for View ${viewName} does not match.`);
        console.log('Differences:', JSON.stringify(differences, null, 2));
 
    }
    }

    resultStatus = (differences.length === 0 && sfData.length === pgData.length) ? 'Pass' : 'Fail';

    let comment = '';
    if (resultStatus === 'Fail') {
        if (sfData.length !== pgData.length) {
            comment = `Row Count for View ${viewName} does not match. SQL Count: ${sfData.length} VS PG Count: ${pgData.length}`;
        } else if (differences.length > 0) {
            comment = `Data for View ${viewName} does not match.`;
        }
    }else{
        comment = `Data for View ${viewName} matches between PostgreSQL and Sql.`;
    }

    writeDifferencesToCsv(differences, viewName, resultStatus,comment);

    function writeDifferencesToCsv(differences, viewName, resultStatus, comment){
        const headers = ['Index','viewName','Column','PGValue','SQLValue'];
        const csvRows = differences.flatMap(difference=>
            difference.differences.map(diff =>
                `${difference.index},${viewName},${diff.column},${diff.normalizedPgValue || diff.pgValue},${diff.normalizedSqlValue || diff.SqlValue}`
            )
        );
        const summaryRow = `${timestamp} - Summary, ${viewName}, ${resultStatus}, ${comment}`;
        // if (fs.existsSync('Logs/Data_ValidationReporting.csv')) 
        //     {
        //         fs.unlinkSync('Logs/Data_ValidationReporting.csv');
        //     }
        if (resultStatus === 'Pass'){
            let csvContent2 = [summaryRow].join('\n');
            fs.appendFile(csvPath, csvContent2 + '\n',(err)=>{
                if(err){
                    console.error('Error writing in csv file');
                }
            })  
        }
        else {
            let csvContent;
            if (sfData.length !== pgData.length) {
                csvContent = [summaryRow].join('\n');
            } else {
                csvContent = [summaryRow, headers.join(','), ...csvRows].join('\n');
            }
            fs.appendFile(csvPath, csvContent + '\n', (err) => {
                if (err) {
                    console.error('Error writing in csv file');
                }
            });
        }
    }
}

async function main() {
    try {
        // Specify the path to your JSON file
        const queries = await readQueriesFromFile(filePath);
        await compareTablesFromQueries(queries);
    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        logStream.end();
    }
}

main();