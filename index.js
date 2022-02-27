const { Pool, Client } = require('pg');
const util = require('util');

let argv = require('minimist')(process.argv.slice(2));
let command = argv._[0];

(async () => {
  let client = new Client({
    host: argv.h,
    user: argv.u,
    password: argv.w,
    database: argv.d,
    port: argv.p,
  });

  let fromBlockHeight = parseInt(argv.s || 1);

  client.connect();

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
        let res = await util.promisify(client.query('SELECT signatures from blocks where height=$1', [i]));
        signaturesBase64 = res.rows[0];
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
      let prunedSignaturesBase64 = Buffer.from(json.stringify(prunedSignatureList), 'utf8').toString('base64');

      try {
        await util.promisify(client.query('UPDATE blocks SET signatures=$1 where height=$2', [prunedSignaturesBase64, i]));
      } catch (error) {
        console.error(error.message);
        process.exit(1);
        return;
      }
      console.log(`Pruned block signatures at height ${i}`);
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
  } else {
    console.log(`Command ${command} was invalid`);
    client.end();
    process.exit(1);
  }
})();
