const EventEmitter = require('events');
const Queue = require('agenda');
const Events = require('../../consts/Events');
const mongoHelper = require('../../helpers/mongo');

class MongoConsumer extends EventEmitter {
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
            this._client.on('ready', (job) => {
                this.emit(Events.ACTIVE, job);
            }).on('error', (error) => {
                this.emit(Events.ERROR, error);
            }).on('start', (job) => {
                this.emit(Events.ACTIVE, job);
            }).on('complete', (job) => {
                this.emit(Events.ACTIVE, job);
            }).on('success', (job) => {
                this.emit(Events.COMPLETED, job);
            }).on('fail', (err, job) => {
                this.emit(Events.FAILED, job);
            });
            await this._client.start();
        }
    }

    async close(options) {
        await this._client.close(options);
    }

    pause(options) {
        return this._client.pause(options);
    }

    resume(options) {
        return this._client.resume(options);
    }
}

module.exports = MongoConsumer;
