const { getPostgresData, getSqlData } = require('../Migration_Class');
const moment = require('moment');
const fs = require('fs');
const fsPromises = require('fs').promises;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const database = process.env.sqlDatabase
// const database = 'SVM';
const filePath = `Queries/${database}/DataType.json`;
const logFilePath = `Log/${database}_log.txt`;
const csvPath = `Result_csv/${database}_DataType_Validation.csv`;
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

console.log = async function (...args) {
    const timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
    const formattedMessage = args.map(arg => (typeof arg === 'string' ? arg : JSON.stringify(arg, null, 2))).join('\n');
    logStream.write(`${timestamp} - ${formattedMessage}\n`);
    process.stdout.write(`${timestamp} - ${formattedMessage}\n`);
};
if (fs.existsSync(csvPath)) fs.unlinkSync(csvPath);
// CSV Writer
const csvWriter = createCsvWriter({
    path: csvPath,
    header: [
        { id: 'tableName', title: 'TableName' },
        { id: 'columnName', title: 'ColumnName' },
        { id: 'sqlDataType', title: 'SQL Data Type' },
        { id: 'pgDataType', title: 'PG Data Type' },
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
        await compareDataType(pgQuery, sqlQuery);
    }
}

async function compareDataType(pgQuery, sqlQuery) {
    try {
        const pgData = await getPostgresData(database, pgQuery);
        const sqlData = await getSqlData(database, sqlQuery);

        const pgTableNames = new Set(pgData.map(row => row.table_name));
        const sqlTableNames = new Set(sqlData.map(row => row.table_name));

        if (pgTableNames.size === sqlTableNames.size) {
            console.log("Table names count match between PostgreSQL and MSSQL.");
        } else {
            console.log("Table names count do not match.");
        }

        const pgNotInMSSQL = [...pgTableNames].filter(name => !sqlTableNames.has(name));
        const sqlNotInPostgres = [...sqlTableNames].filter(name => !pgTableNames.has(name));

        if (pgNotInMSSQL.length > 0) {
            console.log("Table names in PostgreSQL not found in MSSQL:", pgNotInMSSQL);
        } else {
            console.log("All table names in PostgreSQL are present in MSSQL.");
        }

        if (sqlNotInPostgres.length > 0) {
            console.log("Table names in MSSQL not found in PostgreSQL:", sqlNotInPostgres);
        } else {
            console.log("All table names in MSSQL are present in PostgreSQL.");
        }

        const commonTableNames = [...pgTableNames].filter(name => sqlTableNames.has(name));

        const dataTypeMapping = {
            'integer': 'int',
            'character varying': 'varchar',
            'double precision': 'float',
            'timestamp without time zone': 'datetime',
            'character': 'char',
            'boolean': 'bit',
            'text': 'nvarchar',
            'uuid': 'uniqueidentifier'
        };

        for (const tableName of commonTableNames) {
            const pgColumns = pgData.filter(row => row.table_name === tableName);
            const sqlColumns = sqlData.filter(row => row.table_name === tableName);

            const mismatchedColumns = [];
            const csvRecords = [];

            for (const pgColumn of pgColumns) {
                const sqlColumn = sqlColumns.find(row => row.column_name === pgColumn.column_name);

                if (sqlColumn) {
                    let pgDataType = pgColumn.data_type.toLowerCase();
                    let sqlDataType = sqlColumn.data_type.toLowerCase();

                    let pgDataTypeMapped = dataTypeMapping[pgDataType] || pgDataType;

                    if (sqlDataType !== pgDataTypeMapped) {
                        const dataTypeReMapping = {
                            'character varying': 'nvarchar',
                            'character': 'nchar',
                            'numeric': 'decimal',
                            'timestamp without time zone': 'datetime2',
                            'text': 'varchar'
                        };
                        pgDataTypeMapped = dataTypeReMapping[pgDataType] || pgDataType;

                        if (sqlDataType !== pgDataTypeMapped) {
                            mismatchedColumns.push({
                                columnName: pgColumn.column_name,
                                pgDataType: pgDataTypeMapped,
                                sqlDataType: sqlDataType
                            });
                        }
                    }

                    const status = sqlDataType === pgDataTypeMapped ? 'Pass' : 'Fail';
                    csvRecords.push({
                        tableName,
                        columnName: pgColumn.column_name,
                        sqlDataType,
                        pgDataType,
                        status
                    });
                } else {
                    mismatchedColumns.push({
                        columnName: pgColumn.column_name,
                        pgDataType: pgColumn.data_type,
                        sqlDataType: 'Not found in MSSQL'
                    });
                }
            }

            if (mismatchedColumns.length > 0) {
                console.log(`Data type mismatches for table ${tableName}:`);
                console.log(mismatchedColumns);
            } else {
                console.log(`All data types match for table ${tableName}.`);
            }

            // Write CSV records
            if (csvRecords.length > 0) await csvWriter.writeRecords(csvRecords);
        }

        console.log(`CSV file created at ${csvPath}`);
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