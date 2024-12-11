const { getPostgresData, getSqlData } = require('../Migration_Class');
const moment = require('moment');
const fs = require('fs');
const database = process.env.sqlDatabase
// const database ='SVM';  // Replace with your actual database name

const logFilePath = `Log/${database}_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

console.log = function (...args) {
    return new Promise(async (resolve) => {
        const timestamp = moment().format('YYYY-MM-DD hh:mm:ss');
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
const csvPath = `Result_csv/${database}_ColumnName_Validation.csv`;

const createCsvWriter = require('csv-writer').createObjectCsvWriter;
if (fs.existsSync(csvPath)) 
    {
        fs.unlinkSync(csvPath); 
    }
// Define the CSV writer
const csvWriter = createCsvWriter({
    path: csvPath,
    header: [
        {id: 'tableName', title: 'TableName'},
        {id: 'pgColumns', title: 'PG Column Name'},
        {id: 'sfColumns', title: 'SQL Column Name'},
        {id: 'status', title: 'Status'},

    ],
    append: false 
});

    const sqlQuery = (`
        SELECT DISTINCT LOWER(c.table_name) as table_name, LOWER(c.column_name) as column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
            WHERE c.table_schema IN ('dbo','psp')
	        AND t.table_type = 'BASE TABLE'
            AND c.table_name not like 'risk_202%'
            ORDER BY table_name ASC;
    `);
    const pgQuery =(`
        SELECT DISTINCT t.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
            WHERE c.table_schema IN ('dbo','psp')
            AND t.table_type = 'BASE TABLE'
            AND c.table_name not like 'risk_202%'
        ORDER BY table_name ASC;	
    `);

async function compareTableAndColumnNames() {
    
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
        if (pgNotInSql.length > 0 || sfNotInPostgres.length > 0) {
            if (pgNotInSql.length > 0) {
                console.log("Table names in PostgreSQL not found in Sql:", pgNotInSql);
            }
            if (sfNotInPostgres.length > 0) {
                console.log("Table names in Sql not found in PostgreSQL:", sfNotInPostgres);
            }
        } else {
            console.log(`All table names are present in both PostgreSQL and SQL.`);
        }

        // Find common table names
        const commonTableNames = [...pgTableNames].filter(name => sqlTableNames.has(name));
        const csvRecords=[];
        // Perform column-wise comparison for common tables
        for (const tableName of commonTableNames) {
            // Get columns for the current table from both PostgreSQL and Sql
            const pgColumns = pgData.filter(row => row.table_name === tableName).map(row => row.column_name.toLowerCase());
            const sfColumns = sqlData.filter(row => row.table_name === tableName).map(row => row.column_name.toLowerCase());
            
            // const status = sfColumns == pgColumns ? 'Match' : 'Not Match';
            // Compare column names
            const pgNotInSql = pgColumns.filter(name => !sfColumns.includes(name));
            const sfNotInPostgres = sfColumns.filter(name => !pgColumns.includes(name));

            const status = (pgNotInSql.length === 0 && sfNotInPostgres.length === 0)? 'Pass' : 'Fail';
            csvRecords.push({tableName, pgColumns:pgColumns.join(', '), sfColumns:sfColumns.join(', '), status}); 

            // Output the results for each table
            if (pgNotInSql.length > 0 || sfNotInPostgres.length > 0) {
                if (pgNotInSql.length > 0) {
                    console.log(`Column in ${tableName} table in PostgreSQL not found in SQL:`, pgNotInSql);
                }
                if (sfNotInPostgres.length > 0) {
                    console.log(`Column in ${tableName} in SQL not found in PostgreSQL:`, sfNotInPostgres);
                }
            } else {
                console.log(`All columns in ${tableName} table are present in both PostgreSQL and SQL.`);
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

compareTableAndColumnNames();