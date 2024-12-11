const pgp = require('pg-promise')();
const moment = require('moment');
const fs = require('fs');
const fsPromises = require('fs').promises;
require('dotenv').config();
const sql = require('mssql');
const database = process.env.sqlDatabase
// Create a write stream to a log file
const logFilePath = `Log/${database}_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

// Custom logging function
console.log = function (...args) {
    return new Promise(async (resolve) => {
        const timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
        const formattedMessage = args.map(arg => {
            if (typeof arg === 'string') {
                return arg;
            } else {
                return JSON.stringify(arg, null, 2);
            }
        }).join('\n');

        logStream.write(`${timestamp} - ${formattedMessage}\n`);
        process.stdout.write(`${timestamp} - ${formattedMessage}\n`); //Also print to the console

        resolve();
    });
};
const filePath = `Queries/${database}/Row_Count_View.json`;
const csvPath1 = `Result_csv/${database}_Count_Validation_View.csv`;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
if (fs.existsSync(csvPath1)) {fs.unlinkSync(csvPath1);}

// Define the CSV writer
const csvWriter = createCsvWriter({
    path: csvPath1,
    header: [
        { id: 'tableName', title: 'TableName' },
        { id: 'sqlCount', title: 'SQL Count' },
        { id: 'pgCount', title: 'PG Count' },
        { id: 'status', title: 'Status' },
    ],
    append: false
});

// PostgreSQL connection parameters
const pgConfig = {
    user: process.env.pgUser,
    password: process.env.pgPassword,
    host: process.env.pgHost,
    port: process.env.pgPort,
    database: database,
    ssl: { rejectUnauthorized: false }
};

const sqlConfig = {
    user: process.env.sqlUser,
    password: process.env.sqlPassword,
    server: process.env.sqlServer,
    database: database,
    requestTimeout:360000,
    connectionTimeout :36000,
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

const pgDB = pgp(pgConfig);

async function executeSqlServerQuery(query) {
    const pool = await sql.connect(sqlConfig);
    const result = await pool.request().query(query);
    await sql.close();
    return result.recordset;
}

async function executeSqlServerQueries(queries) {
    const results = [];
    const pool = await sql.connect(sqlConfig);
    try {
        for (const query of queries) {
            const result = await pool.request().query(query);
            results.push(result.recordset);
        }
    } catch (err) {
        console.error('SQL Server error', err);
        throw err;
    } finally {
        await sql.close();
    }
    return results;
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

async function compareTablesFromQueries(queries) {
    for (const { pgQuery, sqlQuery } of queries) {
        await compareRowCount(pgQuery, sqlQuery);
    }
}

async function compareRowCount(pgQuery, sqlQuery) {
    try {
        const pgData = await pgDB.any(pgQuery);

        const sqlData = await executeSqlServerQuery(sqlQuery);
        //cmpare table name
        const pgOnlyTableNames = new Set(pgData.map(row => row.table_name));
        const sqlOnlyTableNames = new Set(sqlData.map(row => row.table_name));

        // compare the table size
        if (pgOnlyTableNames.size == sqlOnlyTableNames.size) {
            console.log("Table names count match between PostgreSQL and Sql.");
        } else {
            console.log("Table names count do not match.");
        }

        const pgNotInSql = [...pgOnlyTableNames].filter(name => !sqlOnlyTableNames.has(name));
        const sqlNotInPostgres = [...sqlOnlyTableNames].filter(name => !pgOnlyTableNames.has(name));

        // Results output from table name comparison 
        if (pgNotInSql.length > 0 || sqlNotInPostgres.length > 0) {
            if (pgNotInSql.length > 0) {
                console.log("Table names in PostgreSQL not found in Sql:", pgNotInSql);
            }
            if (sqlNotInPostgres.length > 0) {
                console.log("Table names in Sql not found in PostgreSQL:", sqlNotInPostgres);
            }
        } else {
            console.log(`All tables are present in both PostgreSQL and SQL.`);
        }

        // Find common table names
        const commonTableNames = [...pgOnlyTableNames].filter(name => sqlOnlyTableNames.has(name));

        const pgTableNames = new Set(pgData.map(row => ({ schema: row.table_schema, name: row.table_name })));

        function needsQuotes(tableName) {
            // Add conditions to determine if the table name needs quotes
            return /^\d/.test(tableName) || tableName === 'role';
        }

        //creating the quires 
        const pgQueries = Array.from(pgTableNames).map(table => {
            const quotedTableName = needsQuotes(table.name) ? `"${table.name}"` : table.name;
            return `select '${table.name}' as tablename, count(1) as count from ${table.schema}.${quotedTableName} ;`;
        });
        console.log("pgQueries", pgQueries);
        const sqlTableNames = new Set(sqlData.map(row => ({schema: row.table_schema, name: row.table_name })));

        const sfQueries = Array.from(sqlTableNames).map(table => {
            const quotedTableName = needsQuotes(table.name) ? `"${table.name}"` : table.name;
            return `select '${table.name}' as tablename, count(1) as count from ${table.schema}.${quotedTableName} ;`;
        });

        const sqlResults = await executeSqlServerQueries(sfQueries);
        const pgResults = await Promise.all(pgQueries.map(query => pgDB.any(query)));

        const csvRecords = [];
        // Iterate through common table names and compare counts
        for (const tableName of commonTableNames) {
            // Find the corresponding Sql result for the table
            const sqlResult = sqlResults.find(result => result[0].tablename === tableName);

            // Find the corresponding PostgreSQL result for the table
            const pgResult = pgResults.find(result => result[0].tablename === tableName);

            if (sqlResult && pgResult) {
                const sqlCount = Number(sqlResult[0].count);
                const pgCount = Number(pgResult[0].count);

                // Compare counts
                if (sqlCount === pgCount) {
                    console.log(`Counts match for table ${tableName}: Sql Count: ${sqlCount}, PostgreSQL Count: ${pgCount}`);
                } else {
                    console.log(`Counts do not match for table ${tableName}: Sql Count: ${sqlCount}, PostgreSQL Count: ${pgCount}`);
                }
                const status = sqlCount === pgCount ? 'Pass' : 'Fail';
                csvRecords.push({ tableName, sqlCount, pgCount, status });
            }
            else {
                console.log(`Table ${tableName} not found in either PostgreSQL or Sql results.`);
            }
        }
        const nonEmptyRecords = csvRecords.filter(record => {
            return record.tableName && record.pgCount != null && record.sqlCount != null && record.status;
        })
        if (nonEmptyRecords.length > 0) {
            await csvWriter.writeRecords(nonEmptyRecords);
        } else {
            console.log('No records to write to csv')
        }

    } catch (error) {
        console.error(error);
    }
    finally {
        pgp.end();
        sql.close();
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