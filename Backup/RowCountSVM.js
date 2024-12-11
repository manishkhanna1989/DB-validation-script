const pgp = require('pg-promise')();
const moment = require('moment');
const fs = require('fs');
require('dotenv').config();
const sql = require('mssql');
const database = process.env.sqlDatabase
// const database = 'SVM';
const logFilePath = `Log/${database}_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

// Custom logging function
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
        process.stdout.write(`${timestamp} - ${formattedMessage}\n`); // // Also print to the console

        resolve();
    });
};
const csvPath1 = `Result_csv/${database}_Count_Validation.csv`;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
if (fs.existsSync(csvPath1)) 
    {
        fs.unlinkSync(csvPath1); 
    }
// Define the CSV writer
const csvWriter = createCsvWriter({
    path: csvPath1,
    header: [
        {id: 'tableName', title: 'TableName'},
        {id: 'sqlCount', title: 'SQL Count'},
        {id: 'pgCount', title: 'PG Count'},
        {id: 'status', title: 'Status'},
    ],
    append: false 
});

 // PostgreSQL connection parameters
const pgConfig ={
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
    options: {
        encrypt: true,             
        trustServerCertificate: true,
        requestTimeout:360000 
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

async function compareRowCount() {
try {
// Connect to PostgreSQL
const pgData = await pgDB.any(`
    SELECT DISTINCT c.table_name, c.table_schema, 
       c.table_catalog 
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
        WHERE c.table_schema IN ('dbo','psp')
           AND t.table_type = 'BASE TABLE'
           AND c.table_name not like 'risk_202%'
        ORDER BY table_name ASC;
    `);

    // Connect to SQL Server
const sqlData = await executeSqlServerQuery(`
    SELECT DISTINCT LOWER(c.table_name) as table_name, LOWER(c.table_schema) as table_schema, 
       c.table_catalog as table_catalog
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
        WHERE c.table_schema IN ('dbo','psp')
           AND t.table_type = 'BASE TABLE'
           AND c.table_name not like 'risk_202%'
        ORDER BY table_name ASC;
    `);
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

        // Output the results fro table name comparison
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

    const sqlTableNames = new Set(sqlData.map(row => ({schema: row.table_schema, name: row.table_name })));
    
    const sfQueries = Array.from(sqlTableNames).map(table => {
        const quotedTableName = needsQuotes(table.name) ? `"${table.name}"` : table.name;
        return `select '${table.name}' as tablename, count(1) as count from ${table.schema}.${quotedTableName} ;`;
    });
    
    const sqlResults = await  executeSqlServerQueries(sfQueries);    
    const pgResults = await Promise.all(pgQueries.map(query => pgDB.any(query)));

    const csvRecords=[];
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
        csvRecords.push({tableName, sqlCount, pgCount,  status});    
    } 
    else {
        console.log(`Table ${tableName} not found in either PostgreSQL or Sql results.`);
    }   
}
// console.log('csvRecords',csvRecords);
const nonEmptyRecords = csvRecords.filter(record=>{
    return record.tableName && record.pgCount != null && record.sqlCount != null && record.status ;
})
// console.log('nonEmptyRecords',nonEmptyRecords);
if(nonEmptyRecords.length>0){
    await csvWriter.writeRecords(nonEmptyRecords);
}else{
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

compareRowCount();
