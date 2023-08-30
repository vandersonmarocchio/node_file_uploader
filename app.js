const express = require("express");
const fileUpload = require("express-fileupload");
const path = require("path");

const filesPayloadExists = require('./middleware/filesPayloadExists');
const fileExtLimiter = require('./middleware/fileExtLimiter');
const fileSizeLimiter = require('./middleware/fileSizeLimiter');

const fs = require('fs')
const csv = require('fast-csv')
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 8080;

const app = express();

const { Client } = require("pg")
const dotenv = require("dotenv")


app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "index.html"));
});

app.post('/upload',
    fileUpload({ createParentPath: true }),
    filesPayloadExists,
    fileExtLimiter(['.csv', '.png', '.jpg', '.jpeg', '.log']),
    fileSizeLimiter,
    (req, res) => {       

        const files = req.files

        Object.keys(files).forEach(key => {
            const filepath = path.join(__dirname, 'files', files[key].name)
            files[key].mv(filepath, (err) => {
                if (err) return res.status(500).json({ status: "error", message: err })
            })
            execSql(filepath)
        })

        return res.json({ status: 'success', message: Object.keys(files).toString() })
    }
)

const execSql = async (filepath) => {
    try {
        const client = new Client({
            // user: process.env.PGUSER,
            // host: process.env.PGHOST,
            // database: process.env.PGDATABASE,
            // password: process.env.PGPASSWORD,
            // port: process.env.PGPORT
            user: 'postgres',
            host: '192.168.2.110',
            database: 'CSVPARSE',
            password: '123',
            port: '5432'
        })

        dotenv.config()

        //console.log(client)

        client.connect()

        const stream = fs.createReadStream(filepath);
        let createdTable = false
        let fields = ''
        let tableName = '"' + uuidv4() + '"'
        //console.log('Execute : select * from ' + tableName)
        const streamCsv = csv.parse({
            delimiter: ',',
            quote: '"'
        })
            .on('data', data => {
                if (!createdTable) {
                    let createTable = 'create table ' + tableName + ' (sgid serial primary key,'
                    for (var i = 0; i < data.length; i++) {
                        if (i > 0) {
                            fields = fields + ',"' + i + '"'
                            createTable = createTable + ',"' + i + '" varchar(4000)'
                        } else {
                            fields = fields + '"' + i + '"'
                            createTable = createTable + '"' + i + '" varchar(4000)'
                        }
                    }
                    createTable = createTable + ');'
                    createdTable = true
                    const res = client.query(createTable);
                    console.log(res + ' - ' + createTable)
                }
                let field = ''
                insert = ''
                data.forEach(function callback(value, index) {
                    if (value === '') {
                        field = 'NULL'
                    } else {
                        field = "'" + value.replace(/'/g, '').substring(0, 4000) + "'"
                    }
                    if (index > 0) {
                        insert = insert + ',' + field
                    } else {
                        insert = 'insert into ' + tableName + ' (' + fields + ') values (' + field
                    }
                });
                insert = insert + ') RETURNING sgid;'
                client.query(insert);
                console.log('Inserindo : ' + (data[0].substring(0,20)))
            }
            )
            .on('end', () => console.log('Execute : select * from ' + tableName))

        stream.pipe(streamCsv)

        return 'Execute : select * from ' + tableName

    } catch (e) {
        throw new BadRequest('Erro ao mapear CSV : ' + insert + e);
    }
}

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));