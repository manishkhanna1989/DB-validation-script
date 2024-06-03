const pgp = require('pg-promise')();
const snowflake = require('snowflake-sdk');
const fs = require('fs');
require('dotenv').config();

const sfSchemaName = 'GU_DW';  // Replace with your actual PostgreSQL schema name
const pgSchemaName = 'gu_dw'; // Replace with your actual Snowflake schema name

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
// PostgreSQL connection parameters
const pgConfig = {
    host: process.env.pghost,
    port : process.env.pgport,
    database : process.env.pgdatabase,
    user : process.env.pguser,
    password : process.env.pgpassword
};
// Snowflake connection parameters
const sfConfig = {
    account: process.env.account,
    region: process.env.region,
    username: process.env.DBusername,
    password: process.env.DBpassword,
    warehouse: process.env.warehouse,
    database: process.env.database,
    role: process.env.role
  };
const pgDB = pgp(pgConfig);
const sfConnection = snowflake.createConnection(sfConfig);
sfConnection.connect();
const sqlQuery =
    ` SELECT DISTINCT table_catalog, table_schema, LOWER(table_name) AS TABLE_NAME FROM information_schema.columns WHERE table_schema = '${sfSchemaName}' ORDER BY TABLE_NAME ASC;`

  async function executeSnowflakeQuery(sqlQuery) {
    return new Promise((resolve, reject) => {
      sfConnection.execute({
            sqlText: sqlQuery,
            complete: function (err, stmt, rows) {
                if (err) {
                    console.error('Error executing query: ', err);
                    reject(err);
                } else {
                    resolve(rows);
                }
            }
        });
    });
  }
  async function executeSnowflakeQueries(sfQueries) {
    return new Promise((resolve, reject) => {
      const results = [];
  
      function executeQuery(index) {
        if (index === sfQueries.length) {
          resolve(results);
          return;
        }
  
        const sqlQuery = sfQueries[index];
  
        sfConnection.execute({
          sqlText: sqlQuery,
          complete: function (err, stmt, rows) {
            if (err) {
              console.error('Error executing query: ', err);
              reject(err);
            } else {
              results.push(rows);
              executeQuery(index + 1);
            }
          }
        });
      }
  
      executeQuery(0);
    });
  }

async function compareRowCount() {
    await sfConnection.connect();
    try {
        // Connect to PostgreSQL
        const pgData = await pgDB.any(`
        SELECT DISTINCT c.table_catalog, c.table_schema, c.table_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
        WHERE c.table_schema IN ('${pgSchemaName}')
            AND t.table_type = 'BASE TABLE'  
        ORDER BY c.table_name ASC;
        `);
        // Connect to Snowflake
        const sfData = await executeSnowflakeQuery(sqlQuery);
        //cmpare table name
        const pgOnlyTableNames = new Set(pgData.map(row => row.table_name));
        const sfOnlyTableNames = new Set(sfData.map(row => row.TABLE_NAME)); 
        // compare the table size
        if (pgOnlyTableNames.size == sfOnlyTableNames.size) {
            console.log("Table names count match between PostgreSQL and Snowflake.");
        } else {
            console.log("Table names count do not match.");
        }
         
        const pgNotInSnowflake = [...pgOnlyTableNames].filter(name => !sfOnlyTableNames.has(name));
        const sfNotInPostgres = [...sfOnlyTableNames].filter(name => !pgOnlyTableNames.has(name));

        // Output the results fro table name comparison 
        if (pgNotInSnowflake.length > 0 ) {
            console.log("Table names in PostgreSQL not found in Snowflake:", pgNotInSnowflake);
        } else {
            console.log("All table names in PostgreSQL are present in Snowflake.");
        }

        if (sfNotInPostgres.length > 0) {
            console.log("Table names in Snowflake not found in PostgreSQL:", sfNotInPostgres);
        } else {
            console.log("All table names in Snowflake are present in PostgreSQL.");
        }

    // Find common table names
    const commonTableNames = [...pgOnlyTableNames].filter(name => sfOnlyTableNames.has(name));

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

    const sfTableNames = new Set(sfData.map(row => ({ catalog: row.TABLE_CATALOG, schema: row.TABLE_SCHEMA, name: row.TABLE_NAME })));
    
    const sfQueries = Array.from(sfTableNames).map(table => {
        const quotedTableName = needsQuotes(table.name) ? `"${table.name}"` : table.name;
        return `select '${table.name}' as tablename, count(1) as count from ${table.catalog}.${table.schema}.${quotedTableName} ;`;
    });
    
    const sfResults = await executeSnowflakeQueries(sfQueries);    
    const pgResults = await Promise.all(pgQueries.map(query => pgDB.any(query)));

// Iterate through common table names and compare counts
for (const tableName of commonTableNames) {
    // Find the corresponding Snowflake result for the table
    const sfResult = sfResults.find(result => result[0].TABLENAME === tableName);

    // Find the corresponding PostgreSQL result for the table
    const pgResult = pgResults.find(result => result[0].tablename === tableName);

    if (sfResult && pgResult) {
        const sfCount = Number(sfResult[0].COUNT);
        const pgCount = Number(pgResult[0].count);

        // Compare counts
        if (sfCount === pgCount) {
            console.log(`Counts match for table ${tableName}: PostgreSQL Count: ${pgCount}, Snowflake Count: ${sfCount}`);
        } else {
            console.log(`Counts do not match for table ${tableName}: PostgreSQL Count: ${pgCount}, Snowflake Count: ${sfCount}`);
        }
    } else {
        console.log(`Table ${tableName} not found in either PostgreSQL or Snowflake results.`);
    }
}
    } catch (error) {
        console.error(error);
    } 
    finally {
        pgp.end();
        sfConnection.destroy();
     }
}
compareRowCount();
