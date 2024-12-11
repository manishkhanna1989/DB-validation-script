const { getPostgresData, getSqlData } = require('../Migration_Class');
const moment = require('moment');
const fs = require('fs');
const fsPromises = require('fs').promises;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const database = process.env.sqlDatabase 
const csvPath = `Result_csv/${database}_Indexes_Validation.csv`;
const logFilePath = `Log/${database}_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
const filePath = `Queries/${database}/Index.json`;
console.log = function (...args) {
    return new Promise(async (resolve) => {
        const timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
        const formattedMessage = args.map(arg => (typeof arg === 'string' ? arg : JSON.stringify(arg, null, 2))).join('\n');

        await logStream.write(`${timestamp} - ${formattedMessage}\n`);
        process.stdout.write(`${timestamp} - ${formattedMessage}\n`);

        resolve();
    });
};
if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);

// Define the CSV writer
const csvWriter = createCsvWriter({
    path: csvPath,
    header: [
        { id: 'tableName', title: 'TableName' },
        { id: 'columnName', title: 'ColumnName' },
        {id : 'pgIndexName', title: 'PG Index Name'},
        {id : 'sqlIndexName', title: 'SQL Index Name'},
        { id: 'pgIndexes', title: 'PG Indexes' },
        { id: 'sqlIndexes', title: 'SQL Indexes' },
        { id: 'status', title: 'Status' },
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

async function compareIndexesFromQueries(queries) {
    for (const { pgQuery, sqlQuery } of queries) {
        await compareIndexes(pgQuery, sqlQuery);
    }
}

async function compareIndexes(pgQuery, sqlQuery) {
    try {
        const pgData = await getPostgresData(database, pgQuery);
        const sqlData = await getSqlData(database, sqlQuery);

        const csvRecords = [];
        const missMatchedIndex = [];
        
        // Create a map for PostgreSQL constraints by table and column
        const pgIndexesMap = new Map();
        pgData.forEach(row => {
            const key = `${row.table_name}:${row.column_name}`;
            if (!pgIndexesMap.has(key)) {
                pgIndexesMap.set(key, []);
            }
            pgIndexesMap.get(key).push({
                indexName : row.index_name,
                indexType : row.index_type
            });
        });

        // Create a map for SQL constraints by table and column
        const sqlIndexesMap = new Map();
        sqlData.forEach(row => {
            const key = `${row.table_name}:${row.column_name}`;
            if (!sqlIndexesMap.has(key)) {
                sqlIndexesMap.set(key, []);
            }
            sqlIndexesMap.get(key).push({
                indexName: row.index_name,
                indexType: row.index_type
            });
        });

        // Compare constraints
        for (const [key, sqlIndexes] of sqlIndexesMap.entries()) {
            const [tableName, columnName] = key.split(':');
            const pgIndexes = pgIndexesMap.get(key) || [];

        // Create arrays of index types for comparison
            const pgIndexTypes = pgIndexes.map(pg => pg.indexType);
            const sqlIndexTypes = sqlIndexes.map(sql => sql.indexType);

            const status = (JSON.stringify(pgIndexTypes.sort()) === JSON.stringify(sqlIndexTypes.sort())) ? 'Pass' : 'Fail';

            const pgIndexNames =  pgIndexes.map(pg => pg.indexName).join(', ');
            const sqlIndexNames =  sqlIndexes.map(sql => sql.indexName).join(', ');

            csvRecords.push({
                tableName,
                columnName,
                pgIndexName: pgIndexNames,
                sqlIndexName: sqlIndexNames, 
                pgIndexes: pgIndexTypes.join(', '),
                sqlIndexes: sqlIndexTypes.join(', '),
                status
            });

            // Log differences
            if (status === 'Pass'){
                console.log(`Indexes for ${tableName}.${columnName} matches correctly between MSSQL and PGSQL`);
            }
            else {
                missMatchedIndex.push({
                    tableName,
                    columnName,
                    pgIndexName: pgIndexNames,
                    sqlIndexName: sqlIndexNames
                    // pgIndexes: pgIndexTypes,
                    // sqlIndexes: sqlIndexTypes,
                    // status
                });
                console.log("Indexes mismatched for : ", missMatchedIndex);
            }
        }

        // Write records to CSV
        if (csvRecords.length > 0) {
            await csvWriter.writeRecords(csvRecords);
            console.log(`CSV file created at ${csvPath}`);
        } else {
            console.log('No records to write to CSV');
        }

    } catch (error) {
        console.error(error);
    }
}

async function main() {
    try {
       // Specify the path to your JSON file
        const queries = await readQueriesFromFile(filePath);
        await compareIndexesFromQueries(queries);
    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        logStream.end();
    }
} 

main();
