const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Pegando os dados da API
const axiosData = async () => {
  const result = await axios('https://datausa.io/api/data?drilldowns=Nation&measures=Population');
  return result.data.data;
}


// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    const STARTYEAR = 2018
    const ENDYEAR = 2020

    try {
        await migrationUp();
        // INCLUINDO DADOS DA API NO DB
        const data = await axiosData();
        const result3 = await db[DATABASE_SCHEMA].api_data.insert({
                doc_record: JSON.stringify(data)
            })
        console.log('result3 >>>', result3);

        // SOMANDO A POPULACAO COM SELECT
        const result4 = await db.query(
            `SELECT SUM((total_population->> 'Population')::int)
            FROM api_data,
                jsonb_array_elements(doc_record->'data') AS total_population
            WHERE (total_population->>'Year')::int IN (2020, 2019, 2018);`
        );
        console.log('result4 >>>', result4);
        // [ { sum: '4870850665' } ]
        console.log(result4[0].sum);

        // CRIANDO UMA VIEW PARA POPULACAO COM SELECT
        const result5 = await db.query(
            `CREATE OR REPLACE VIEW population AS
                SELECT SUM((total_population->> 'Population')::int)
                FROM api_data,
                    jsonb_array_elements(doc_record->'data') AS total_population
                WHERE (total_population->>'Year')::int IN (2020, 2019, 2018);
            SELECT * FROM population;`
        );
            console.log('result5 >>>', result5);

        // SOMATORIA COM JS
        const YEARS = [2020, 2019, 2018];
        
        let sum = 0;
        const result6 = data.forEach(eachData => {
            if (YEARS.includes(Number(eachData.Year))) {
                return sum += eachData.Population
                };
        });
            console.log('result6 >>>', sum);

        //exemplo de insert
        // const result1 = await db[DATABASE_SCHEMA].api_data.insert({
        //     doc_record: { 'a': 'b' },
        // })
        // console.log('result1 >>>', result1);

        //exemplo select
        // const result2 = await db[DATABASE_SCHEMA].api_data.findOne(28,{
        //     fields: ['doc_record']
        // });

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();