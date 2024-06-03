const { getPostgresData, getSnowflakeData } = require('./Migration_Class');

const sfSchemaName = 'PUBLIC';  // Replace with your actual PostgreSQL schema name
const pgSchemaName = 'public'; // Replace with your actual Snowflake schema name

const sfQuery =(`SELECT DISTINCT LOWER(table_name) AS TABLE_NAME, LOWER(column_name) AS COLUMN_NAME ,LOWER(data_type) AS DATA_TYPE FROM information_schema.columns WHERE table_schema = '${sfSchemaName}' ORDER BY TABLE_NAME ASC;`)

const pgQuery = (`SELECT DISTINCT c.table_name, c.column_name, c.data_type FROM information_schema.columns c JOIN information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name WHERE c.table_schema IN ('${pgSchemaName}') AND t.table_type = 'BASE TABLE' ORDER BY c.table_name ASC;`)

                
async function compareDataType() {

    try {

        // Get data from PostgreSQL
        const pgData = await getPostgresData(pgQuery);
    
        // Get data from Snowflake
        const sfData = await getSnowflakeData(sfQuery);

        //cmpare table name 
        const pgTableNames = new Set(pgData.map(row => row.table_name));
        const sfTableNames = new Set(sfData.map(row => row.TABLE_NAME)); 

        if (pgTableNames.size == sfTableNames.size) {
            console.log("Table names count match between PostgreSQL and Snowflake.");
        } else {
            console.log("Table names count do not match.");
        }
         
        const pgNotInSnowflake = [...pgTableNames].filter(name => !sfTableNames.has(name));
        const sfNotInPostgres = [...sfTableNames].filter(name => !pgTableNames.has(name));

        // Output the results fro table name comparision 
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
const commonTableNames = [...pgTableNames].filter(name => sfTableNames.has(name));

// Define a mapping of PostgreSQL data types to Snowflake data types
const dataTypeMapping = {
    'double precision': 'float',
    'bigint': 'number',
    'character varying': 'text',
    'USER-DEFINED':'text',
    'integer':'number',
    'user-defined':'text',
    'timestamp with time zone':'timestamp_ntz',
    'timestamp without time zone':'timestamp_ntz',
    // Add more mappings as needed
};

// Perform column-wise comparison for common tables
for (const tableName of commonTableNames) {
    // Get columns for the current table from both PostgreSQL and Snowflake
    const pgColumns = pgData.filter(row => row.table_name === tableName);
    const sfColumns = sfData.filter(row => row.TABLE_NAME === tableName);

    // Compare data types
    const mismatchedColumns = [];

    for (const pgColumn of pgColumns) {
        const sfColumn = sfColumns.find(row => row.COLUMN_NAME.toLowerCase() === pgColumn.column_name.toLowerCase());

        if (sfColumn) {
            const pgDataType = pgColumn.data_type.toLowerCase();
            const sfDataType = sfColumn.DATA_TYPE.toLowerCase();

            // Replace PostgreSQL data type with Snowflake data type using the mapping
            const pgDataTypeMapped = dataTypeMapping[pgDataType] || pgDataType;

            if (sfDataType !== pgDataTypeMapped) {
                mismatchedColumns.push({
                    column: pgColumn.column_name,
                    pgDataType: pgDataTypeMapped,
                    sfDataType: sfDataType
                });
            }
        } else {
            // Column not found in Snowflake
            mismatchedColumns.push({
                column: pgColumn.column_name,
                pgDataType: pgColumn.data_type,
                sfDataType: 'Not found in Snowflake'
            });
        }
    }

    // Output the results for each table
    if (mismatchedColumns.length > 0) {
        console.log(`Data type mismatches for table  ${tableName}:`);
        console.table(mismatchedColumns);
    } else {
        console.log(`All data types matches for table ${tableName}.`);
    }
}
    
    } catch (error) {
        console.error(error);
    } 
    // finally {
    //     // Close connections
    //     // pgDB.end();
    //     sfConnection.destroy();
    //  }

}
// main();
compareDataType();