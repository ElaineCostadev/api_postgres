const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');


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
    //     const data = await axiosData();
    //     const result3 = await db[DATABASE_SCHEMA].api_data.insert({
    //             doc_record: JSON.stringify(data)
    //         })

        // SOMANDO A POPULACAO COM SELECT
        // const result4 = await db.query(
        //     `SELECT SUM((total_population->> 'Population')::int)
        //     FROM api_data,
        //         jsonb_array_elements(doc_record->'data') AS total_population
        //     WHERE (total_population->>'Year')::int IN (2020, 2019, 2018);`
        // );
        // console.log('result2 >>>', result2);
        // [ { sum: '4870850665' } ]
        // console.log(result4[0].sum);

        
        


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