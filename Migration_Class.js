const pgp = require('pg-promise')();
const snowflake = require('snowflake-sdk');
let pgDB;
require('dotenv').config();

function getPostgresData(pgQuery) {
    const pgConfig = {
        host: process.env.pghost,
        port : process.env.pgport,
        database : process.env.pgdatabase,
        user : process.env.pguser,
        password : process.env.pgpassword
    };
    
    if (!pgDB) {
        pgDB = pgp(pgConfig);
    }

    return pgDB.any(pgQuery);
}

async function getSnowflakeData(sfQuery) {
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

    const sfConnection = snowflake.createConnection(sfConfig);
    await sfConnection.connect();

    return new Promise((resolve, reject) => {
        sfConnection.execute({
            sqlText: sfQuery,
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

function compareSets(set1, set2) {
    return set1.size === set2.size && [...set1].every(value => set2.has(value));
}

async function closePostgresConnection() {
    if (pgDB) {
        await pgDB.$pool.end(); // Close the pg-promise connection pool
        pgDB = null; 
    }
}

async function closeSnowflakeConnection(sfConnection) {
    if (sfConnection) {
        await sfConnection.destroy(); // Close the Snowflake connection
    }
}

module.exports = {
    getPostgresData,
    getSnowflakeData,
    compareSets,
    closePostgresConnection,
    closeSnowflakeConnection,
};
