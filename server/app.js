const express = require('express')
const cors = require('cors')
const cron = require('node-cron')
require('dotenv').config()
const {getTransactionsFromNearIndexerDatabase, getTransactionsFromPaymentReference} = require('./model/near_indexer_repository')
const getServerConfig = require('../src/near-utility-server.config')
const serverConfig = getServerConfig(process.env.NODE_ENV || 'development')

// Buffer for data received from the indexer
let indexerData = new Map()

async function refreshIndexerData() {
    console.log("Refreshing indexer data...")
    try {
        const data = await getTransactionsFromNearIndexerDatabase()
        indexerData.clear()
        data.map((item) => {
            console.log(`Transaction Hash: ${item.transaction_hash}`)
            indexerData.set(item.transaction_hash, item)
        })
    } catch (err) {
        console.log(err.message)
    }
}

// Starts the scheduled task.
cron.schedule('*/1 * * * *', () =>  {
    console.log('cronjob running every 1min')
    refreshIndexerData().then(() => console.log("Indexer data updated"))
}, { scheduled: true })

const app = express()
app.use(cors())
app.use(express.json())

app.get('/', (req, res) => {
    res.send('Hello World!')
})

app.get('/payments/:paymentAddress/:paymentReference', async (req, res) => {
    try {
        const data = await getTransactionsFromPaymentReference(req.params.paymentAddress, req.params.paymentReference);
        res.json({ success: true, data });
    } catch(e) {
        return res.status(500).send({ success: false, error: e.message})
    }
});

app.get('/transactions-from-indexer', async (req, res) => {
    try {
        let limitLines = req.query.top ? req.query.top : serverConfig.limitLinesOfResult
        const iterator = indexerData.values()
        let counter = indexerData.size < limitLines ? indexerData.size : limitLines
        let data = []
        while (counter > 0) {
            data.push(iterator.next().value)
            counter--
        }
        res.json({ success: true, data })
    } catch(e) {
        return res.status(500).send({ success: false, error: e.message})
    }
})

app.get('/one-transaction-from-indexer', async (req, res) => {
    try {
        let tx_id = req.query.id
        if (!tx_id || !indexerData.has(tx_id)) {
            return res.status(404).send({ success: false, error: "Transaction not found"})
        }
        res.json({ success: true, data: indexerData.get(tx_id) })
    } catch(e) {
        return res.status(500).send({ success: false, error: e.message})
    }
})

app.listen(serverConfig.serverPort, () => {
    console.log(`\nLocal Server\nListening at http://localhost:${serverConfig.serverPort}`)
})
