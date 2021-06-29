const dbConnect = require('@hkube/db');

class Helper {
    async init(options) {
        const db = dbConnect({ mongo: options });
        await db.client.connect();
        const client = db.client.db(options.dbName);
        return client;
    }
}

module.exports = new Helper();
