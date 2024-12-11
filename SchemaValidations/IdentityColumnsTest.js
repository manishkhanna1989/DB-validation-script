const { getPostgresData, getSqlData } = require('../Migration_Class');
const moment = require('moment');
const fs = require('fs');
const fsPromises = require('fs').promises;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const database = process.env.sqlDatabase ; // const database ='TradeReporter';
const logFilePath = 'Log/log.txt';
const filePath = `Queries/${database}/Identity_Column.json`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
const csvPath = `Result_csv/${database}_Identity_Column_Validation.csv`;

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
        process.stdout.write(`${timestamp} - ${formattedMessage}\n`); // Also print to the console

        resolve();
    });
};

if (fs.existsSync(csvPath)){fs.unlinkSync(csvPath);}
const csvWriter = createCsvWriter({
    path: csvPath,
    header: [
        {id: 'tableName', title: 'TableName'},
        {id: 'pgColumns', title: 'PG Identity Column Name'},
        {id: 'sfColumns', title: 'SQL Identity Column Name'},
        {id: 'status', title: 'Status'},

    ],
    append: false 
});

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
        await compareIdentityColumns(pgQuery, sqlQuery);
    }
}

async function compareIdentityColumns(pgQuery, sqlQuery) {
    
    try {
        // Get data from PostgreSQL
        const pgData = await getPostgresData(database, pgQuery);

        // Get data from Sql
        const sqlData = await getSqlData(database, sqlQuery);

        //cmpare table name 
        const pgTableNames = new Set(pgData.map(row => row.table_name));
        const sqlTableNames = new Set(sqlData.map(row => row.table_name)); 

        if (pgTableNames.size == sqlTableNames.size) {
            console.log("Table names count match between PostgreSQL and Sql.");
        } else {
            console.log("Table names count do not match.");
        }
         
        const pgNotInSql = [...pgTableNames].filter(name => !sqlTableNames.has(name));
        const sfNotInPostgres = [...sqlTableNames].filter(name => !pgTableNames.has(name));

        // Output the results fro table name comparision 
        if (pgNotInSql.length > 0 ) {
            console.log("Table names in PostgreSQL not found in Sql:", pgNotInSql);
        } else {
            console.log("All table names in PostgreSQL are present in Sql.");
        }

        if (sfNotInPostgres.length > 0) {
            console.log("Table names in Sql not found in PostgreSQL:", sfNotInPostgres);
        } else {
            console.log("All table names in Sql are present in PostgreSQL.");
        }

        // Find common table names
        const commonTableNames = [...pgTableNames].filter(name => sqlTableNames.has(name));
        const csvRecords=[];
  
        for (const tableName of commonTableNames) {
            // Get columns for the current table from both PostgreSQL and Sql
            const pgColumns = pgData.filter(row => row.table_name === tableName).map(row => row.column_name);
            const sfColumns = sqlData.filter(row => row.table_name === tableName).map(row => row.column_name.toLowerCase());
            
            // Compare column names
            const pgNotInSql = pgColumns.filter(name => !sfColumns.includes(name));
            const sfNotInPostgres = sfColumns.filter(name => !pgColumns.includes(name));

            const status = (pgNotInSql.length === 0 && sfNotInPostgres.length === 0)? 'Pass' : 'Fail';

            csvRecords.push({tableName, pgColumns:pgColumns, sfColumns:sfColumns, status}); 

            if (pgNotInSql.length > 0 || sfNotInPostgres.length > 0) {
                if (pgNotInSql.length > 0) {
                    console.log(`Identity Column in ${tableName} table in PostgreSQL not found in SQL:`, pgNotInSql);
                }
                if (sfNotInPostgres.length > 0) {
                    console.log(`Identity column in ${tableName} in SQL not found in PostgreSQL:`, sfNotInPostgres);
                }
            } else {
                console.log(`Identity columns in ${tableName} table are present in both PostgreSQL and SQL.`);
            }
        }
        if(csvRecords.length>0){
            await csvWriter.writeRecords(csvRecords);
        }else{
            console.log('No records to write to csv')
        }
    
    } catch (error) {
        console.error(error);
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