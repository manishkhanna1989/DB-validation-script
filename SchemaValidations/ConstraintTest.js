const { getPostgresData, getSqlData } = require('../Migration_Class');
const moment = require('moment');
const fs = require('fs');
const fsPromises = require('fs').promises;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const database = process.env.sqlDatabase
// const database = 'SVM'; 
const filePath = `Queries/${database}/Constraints.json`;
const logFilePath = `Log/${database}_log.txt`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
const csvPath = `Result_csv/${database}_Constraints_Validation.csv`;

console.log = function (...args) {
    return new Promise(async (resolve) => {
        const timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
        const formattedMessage = args.map(arg => (typeof arg === 'string' ? arg : JSON.stringify(arg, null, 2))).join('\n');

        logStream.write(`${timestamp} - ${formattedMessage}\n`);
        process.stdout.write(`${timestamp} - ${formattedMessage}\n`);

        resolve();
    });
};

if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
const csvWriter = createCsvWriter({
    path: csvPath,
    header: [
        { id: 'tableName', title: 'TableName' },
        { id: 'columnName', title: 'ColumnName' },
        { id: 'pgConstraints', title: 'PG Constraints' },
        { id: 'sqlConstraints', title: 'SQL Constraints' },
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

async function compareTablesFromQueries(queries) {
    for (const { pgQuery, sqlQuery } of queries) {
        await compareConstraints(pgQuery, sqlQuery);
    }
}

async function compareConstraints(pgQuery, sqlQuery) {
    try {
        const pgData = await getPostgresData(database, pgQuery);
        const sqlData = await getSqlData(database, sqlQuery);

        const csvRecords = [];

        // Create a map for PostgreSQL constraints by table and column
        const pgConstraintsMap = new Map();
        pgData.forEach(row => {
            const key = `${row.table_name}:${row.column_name}`;
            if (!pgConstraintsMap.has(key)) {
                pgConstraintsMap.set(key, []);
            }
            pgConstraintsMap.get(key).push(row.constraint_type);
        });

        // Create a map for SQL constraints by table and column
        const sqlConstraintsMap = new Map();
        sqlData.forEach(row => {
            const key = `${row.table_name}:${row.column_name}`;
            if (!sqlConstraintsMap.has(key)) {
                sqlConstraintsMap.set(key, []);
            }
            sqlConstraintsMap.get(key).push(row.constraint_type);
        });

        // Compare constraints
        for (const [key, sqlConstraints] of sqlConstraintsMap.entries()) {
            const [tableName, columnName] = key.split(':');
            const pgConstraints = pgConstraintsMap.get(key) || [];
            const sortedPgConstraints = [...pgConstraints].sort();
            const sortedSqlConstraints = [...sqlConstraints].sort();
            const status = (JSON.stringify(sortedPgConstraints) === JSON.stringify(sortedSqlConstraints)) ? 'Pass' : 'Fail';
            const mismatchedColumns = [];
            csvRecords.push({
                tableName,
                columnName,
                pgConstraints: sortedPgConstraints.join(', '),
                sqlConstraints: sortedSqlConstraints.join(', '),
                status
            });

            if(status === 'Pass'){
                console.log(`All constraints for ${tableName} table matches between PostgreSQL and Sql.`)
            } 
            else {
                mismatchedColumns.push({
                    TableName : tableName,
                    ColumnName : columnName,
                    PG_Constraints : sortedPgConstraints.join(', '),
                    SQL_Constraints : sortedSqlConstraints.join(', ')
                });
                console.log(`Constraints for ${tableName} table Not matches between PostgreSQL and Sql.`);
                console.log(mismatchedColumns);
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
        const queries = await readQueriesFromFile(filePath);
        await compareTablesFromQueries(queries);
    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        logStream.end();
    }
} 

main();