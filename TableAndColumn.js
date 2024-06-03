const { getPostgresData, getSnowflakeData } = require('./Migration_Class');

const sfSchemaName = 'GU_DW';  // Replace with your actual PostgreSQL schema name
const pgSchemaName = 'gu_dw'; // Replace with your actual Snowflake schema name

const sfQuery = (`
    SELECT DISTINCT LOWER(table_name) AS TABLE_NAME, LOWER(column_name) AS COLUMN_NAME
    FROM information_schema.columns
    WHERE table_schema = '${sfSchemaName}'
    ORDER BY TABLE_NAME ASC;
`);

const pgQuery =(`
        SELECT DISTINCT c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_schema = t.table_schema
            AND c.table_name = t.table_name
        WHERE c.table_schema IN ('${pgSchemaName}')
            AND t.table_type = 'BASE TABLE'  
        ORDER BY c.table_name ASC;
    `);

async function compareTableAndColumnNames() {
    
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

        // Perform column-wise comparison for common tables
        for (const tableName of commonTableNames) {
            // Get columns for the current table from both PostgreSQL and Snowflake
            const pgColumns = pgData.filter(row => row.table_name === tableName).map(row => row.column_name.toLowerCase());
            const sfColumns = sfData.filter(row => row.TABLE_NAME === tableName).map(row => row.COLUMN_NAME.toLowerCase());

            // Compare column names
            const pgNotInSnowflake = pgColumns.filter(name => !sfColumns.includes(name));
            const sfNotInPostgres = sfColumns.filter(name => !pgColumns.includes(name));

            // Output the results for each table
            if (pgNotInSnowflake.length > 0) {
                console.log(`Columns in ${tableName} table in PostgreSQL not found in Snowflake:`, pgNotInSnowflake);
            } else {
                console.log(`All columns in ${tableName} table in PostgreSQL are present in Snowflake.`);
            }

            if (sfNotInPostgres.length > 0) {
                console.log(`Columns in ${tableName} in Snowflake not found in PostgreSQL:`, sfNotInPostgres);
            } else {
                console.log(`All columns in ${tableName} in Snowflake are present in PostgreSQL.`);
            }
        }
        
    } catch (error) {
        console.error(error);
    } 
    
}

compareTableAndColumnNames();