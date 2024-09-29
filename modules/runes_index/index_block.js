require('dotenv').config();
const axios = require('axios');
const { Pool } = require('pg')


var db_pool = new Pool({
    user: process.env.DB_USER || 'postgres',
    host: process.env.DB_HOST || 'psql-svc',
    database: process.env.DB_DATABASE || 'postgres',
    password: process.env.DB_PASSWD,
    port: parseInt(process.env.DB_PORT || "5432"),
    max: process.env.DB_MAX_CONNECTIONS || 10, // maximum number of clients!!
    ssl: process.env.DB_SSL == 'true' ? true : false
})

const MAX_RETRIES = 5;
const RETRY_DELAY = 1000; // 1 second

async function execute_on_db(query, params, retries = MAX_RETRIES) {
    try {
        await db_pool.query(query, params);
    } catch (err) {
        if (retries > 0) {
            console.warn(`Query failed. Retrying in ${RETRY_DELAY / 1000} seconds... (${retries} retries left). Error: ${err}`);
            await new Promise(res => setTimeout(res, RETRY_DELAY));
            return execute_on_db(query, params, retries - 1);
        } else {
            console.error("Max retries reached. Throwing error.");
            throw err;
        }
    }
}

async function createTablesIfNotExist() {
    const createTxsTableQuery = `
        CREATE TABLE IF NOT EXISTS txs (
            block INT4 NOT NULL,
            txid TEXT PRIMARY KEY,
            version INT2,
            size INT4,
            vsize INT4,
            weight INT4,
            locktime BIGINT,
            fee BIGINT,
            fee_rate INT4,
            vin_count INT2,
            vout_count INT2
        )`;

    // const createVinsTableQuery = `
    //     CREATE TABLE IF NOT EXISTS vins (
    //         txid TEXT,
    //         coinbase TEXT,
    //         vin_txid TEXT,
    //         vin_vout INTEGER,
    //         sequence BIGINT,
    //     )`;

    // const createVoutsTableQuery = `
    //     CREATE TABLE IF NOT EXISTS vouts (
    //         txid TEXT,
    //         n INTEGER,
    //         value BIGINT,
    //         hex TEXT,
    //         wallet_addr TEXT,
    //     )`;

    const createIndexOnBlockQuery = `
        CREATE INDEX IF NOT EXISTS idx_block ON txs (block)`;

    await execute_on_db(createTxsTableQuery, []);
    // await execute_on_db(createVinsTableQuery, []);
    // await execute_on_db(createVoutsTableQuery, []);
    await execute_on_db(createIndexOnBlockQuery, []);
    console.log("Create tables SUCCESS!")
}

async function getLastBlockInDB() {
    try {
        const query = 'SELECT MAX(block) as last_block FROM txs';
        const res = await db_pool.query(query);
        return res.rows[0].last_block;
    } catch (err) {
        console.error("ERROR FETCHING LAST BLOCK FROM DB!!!");
        console.error(err);
        throw err;
    }
}


// Fetch block data from Bitcoin Core by block height
const fetchBlockHashByHeight = async (blockHeight) => {
    const response = await axios.post(
        'http://' + process.env.BITCOIN_RPC_URL,
        {
            jsonrpc: '1.0',
            id: '1',
            method: 'getblockhash',
            params: [blockHeight]
        },
        {
            headers: {
                'Content-Type': 'text/plain',
            },
            auth: {
                username: process.env.BITCOIN_RPC_USER,
                password: process.env.BITCOIN_RPC_PASSWD.replace('\\', '')
            },
        }
    );

    return response.data.result;

};


const getLastBlockHeight = async () => {
    const response = await axios.post(
        'http://' + process.env.BITCOIN_RPC_URL,
        {
            jsonrpc: '1.0',
            id: '1',
            method: 'getblockcount',
            params: []
        },
        {
            headers: {
                'Content-Type': 'text/plain',
            },
            auth: {
                username: process.env.BITCOIN_RPC_USER,
                password: process.env.BITCOIN_RPC_PASSWD.replace('\\', '')
            },
        }
    );

    return parseInt(response.data.result);
}

// Fetch block data from Bitcoin Core
const fetchBlockData = async (blockHash) => {
    const response = await axios.post(
        'http://' + process.env.BITCOIN_RPC_URL,
        {
            jsonrpc: '1.0',
            id: '3',
            method: 'getblock',
            params: [blockHash, 2]
        },
        {
            headers: {
                'Content-Type': 'text/plain',
            },
            auth: {
                username: process.env.BITCOIN_RPC_USER,
                password: process.env.BITCOIN_RPC_PASSWD.replace('\\', '')
            },
        }
    );

    return response.data.result;
};

// Insert block data into the database
const insertBlockData = async (block) => {

    try {
        // Insert transactions data
        const txQuery = `
            INSERT INTO txs (block, txid, version, size, vsize, weight, locktime, fee, fee_rate, vin_count, vout_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`;


        // const vinQuery = `
        //     INSERT INTO vins (txid, coinbase, vin_txid, vin_vout, sequence)
        //     VALUES ($1, $2, $3, $4, $5)`;

        // const voutQuery = `
        //     INSERT INTO vouts (txid, n, value, hex, wallet_addr)
        //     VALUES ($1, $2, $3, $4, $5)`;

        const blockH = block.height;
        let running_promises = []
        for (const tx of block.tx) {
            const feeInSatoshis = Math.round((tx.fee || 0) * 10 ** 8);
            const feeRate = Math.round(feeInSatoshis / tx.vsize);

            running_promises.push(execute_on_db(txQuery, [
                blockH, tx.txid, tx.version, tx.size, tx.vsize, tx.weight, tx.locktime, feeInSatoshis, feeRate, tx.vin.length, tx.vout.length
            ]));


            // for (const vin of tx.vin) {
            //     running_promises.push(
            //         execute_on_db(vinQuery, [
            //             tx.txid, vin.coinbase != undefined ? vin.coinbase : null,
            //             vin.txid != undefined ? vin.txid : null,
            //             vin.vout || -1,
            //             vin.sequence
            //         ])
            //     );
            // }

            // for (const vout of tx.vout) {
            //     const valueInSatoshis = Math.round((vout.value || 0) * 10 ** 8);
            //     running_promises.push(
            //         execute_on_db(voutQuery, [
            //             tx.txid, vout.n, valueInSatoshis, vout.scriptPubKey.hex, vout.scriptPubKey.address
            //         ])
            //     );
            // }
        }

        await Promise.all(running_promises)

    } catch (err) {
        console.log(`[ERROR] At block: ${block.height},  ${err}`);
        console.error(`[ERROR] At block: ${block.height},  ${err}`);
        // process.exit(1)
    }
};

const processBlock = async (blockHeight) => {
    const startTime = Date.now();
    const blockHash = await fetchBlockHashByHeight(blockHeight);
    const blockData = await fetchBlockData(blockHash);
    const fetchTime = (Date.now() - startTime) / 1000;
    const startTime2 = Date.now();
    await insertBlockData(blockData);
    console.log(`[index-block-${blockHeight}] use ${fetchTime} secs fetcing, use ${(Date.now() - startTime2) / 1000} seconds insert block data.`);
}

// Main function to fetch and store block data
const indexBlocks = async () => {
    const fromBlock = 840000;

    await createTablesIfNotExist();

    const lastBlockOfDBRaw = await getLastBlockInDB();
    const lastBlockOfDB = parseInt(lastBlockOfDBRaw);
    let blockStart = lastBlockOfDB > fromBlock ? lastBlockOfDB + 1 : fromBlock;
    try {
        while (true) {
            const latestBlockHeight = await getLastBlockHeight();
            if (blockStart >= latestBlockHeight) {
                console.log(`[${new Date().toISOString()}] No new blocks to index(` + blockStart + " >= " + latestBlockHeight + ")");
                continue;
            } else {
                for (let i = blockStart; i <= latestBlockHeight; i++) {
                    try {
                        await processBlock(i);
                    } catch (err) {
                        console.log(`[ERROR] Failed to process block ${i}: ${err}`);
                        i--;
                    }
                }
                blockStart = latestBlockHeight + 1;
            }
        }

    } catch (err) {
        console.log("ERROR IN indexBlocks FUNCTION!!!, err: ", err);
    }
    finally {
        console.log("Closing DB connection...");
        await db_pool.end();
    }

};

indexBlocks();