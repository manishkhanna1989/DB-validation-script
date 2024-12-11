const pgp = require('pg-promise')();
let pgDB ;
require('dotenv').config();
const sql = require('mssql');

function getPostgresData(database, pgQuery) {     
    const config = {
        host: process.env.pgHost,        
        port: parseInt(process.env.pgPort),
        database: database,
        user: process.env.pgUser,        
        password: process.env.pgPassword,
        ssl: { 
            rejectUnauthorized: false 
        }    
    };
    if (!pgDB) {
        pgDB = pgp(config);
    }
    return pgDB.any(pgQuery);
}

async function getSqlData(database, query) {
    const config = {
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
    try {
        const pool = await sql.connect(config);
        const result = await pool.request().query(query);
        return result.recordset;
    } catch (err) {
        console.error('SQL error', err);
        throw err;
    } finally {
        
        sql.close();
    }
}

function compareSets(set1, set2) {
    return set1.size === set2.size && [...set1].every(value => set2.has(value));
}

async function closePostgresConnection() {
    if (pgDB) {
        await pgDB.$pool.end(); 
        pgDB = null; 
    }
}

async function closeSqlConnection() {
    try {
        await sql.close(); 
    } catch (err) {
        console.error('Error closing SQL connection', err);
    }
}

module.exports = {
    getPostgresData,
    getSqlData,
    compareSets,
    closePostgresConnection,
    closeSqlConnection,
};
