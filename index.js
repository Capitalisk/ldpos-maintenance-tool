#!/usr/bin/env node

const { Pool, Client } = require('pg');
const util = require('util');

const CONNECT_TIMEOUT = 5000;

let argv = require('minimist')(process.argv.slice(2));
let command = argv._[0];

(async () => {
  let client = new Client({
    host: argv.h,
    user: argv.u,
    password: argv.w,
    database: argv.d,
    port: argv.p,
    connectionTimeoutMillis: argv.t == null ? CONNECT_TIMEOUT : argv.t
  });

  let fromBlockHeight = parseInt(argv.s || 1);

  try {
    await util.promisify(client.connect).call(client);
  } catch (error) {
    console.error(
      `Failed to connect to database because of error: ${error.message}`
    );
    process.exit(1);
    return;
  }

  if (command === 'prune') {
    if (argv._.length < 2) {
      throw new Error(
        `Command arguments were missing - Expected 2; keepSignatureCount and toBlockHeight`
      );
    }
    let keepSignatureCount = parseInt(argv._[1]);
    let toBlockHeight = parseInt(argv._[2]);

    for (let i = fromBlockHeight; i < toBlockHeight; i++) {
      let signaturesBase64;
      try {
        let res = await util.promisify(client.query).call(client, 'SELECT signatures FROM blocks WHERE height=$1', [i]);
        signaturesBase64 = res.rows[0].signatures;
      } catch (error) {
        console.error(error.message);
        process.exit(1);
        return;
      }

      let signatureList;
      try {
        signatureList = JSON.parse(Buffer.from(signaturesBase64, 'base64').toString('utf8'));
      } catch (error) {
        console.error(`Format of block signatures at height ${i} was invalid`);
        process.exit(1);
        return;
      }
      let prunedSignatureList = signatureList.slice(0, keepSignatureCount);
      let prunedSignaturesBase64 = Buffer.from(JSON.stringify(prunedSignatureList), 'utf8').toString('base64');

      try {
        await util.promisify(client.query).call(client, 'UPDATE blocks SET signatures=$1 WHERE height=$2', [prunedSignaturesBase64, i]);
      } catch (error) {
        console.error(error.message);
        process.exit(1);
        return;
      }
      console.log(`Pruned block signatures at height ${i}`);
    }

    try {
      await util.promisify(client.query).call(client, 'VACUUM blocks');
    } catch (error) {
      console.error(`Failed to vacuum the database after cleanup because of error: ${error.message}`);
      process.exit(1);
    }

    console.log(
      `Finished pruning block signatures between heights ${
        fromBlockHeight
      } and ${
        toBlockHeight
      } to length ${
        keepSignatureCount
      }`
    );
    client.end();
    process.exit(0);
  } else {
    console.log(`Command ${command} was invalid`);
    client.end();
    process.exit(1);
  }
})();
