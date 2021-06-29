const EventEmitter = require('events');
const Queue = require('agenda');
const mongoHelper = require('../../helpers/mongo');

class Consumer extends EventEmitter {
    constructor(options) {
        super();
        this._connection = options;
    }

    async register(jobOptions, cb) {
        await this._createClient();
        const { name, options } = jobOptions;
        this._client.define(name, options, (job, done) => {
            cb({ job: job.attrs.data, done });
        });
    }

    async _createClient() {
        if (!this._client) {
            const mongoClient = await mongoHelper.init(this._connection);
            this._client = new Queue({ mongo: mongoClient });
            await this._client.start();
        }
    }

    async close({ type }) {
        await queue.close();
    }

    pause(options) {
        return queue.pause(true, true);
    }

    resume(options) {
        return queue.resume(true);
    }
}

module.exports = Consumer;
