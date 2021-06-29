const EventEmitter = require('events');
const Queue = require('@hkube/bull');
const redis = require('../../helpers/redis');

class Consumer extends EventEmitter {
    constructor(connection) {
        super();
        this._queues = new Map();
        this._connection = connection;
        redis.init(connection);
        this._setting = Object.assign({}, this._setting, redis.client);
    }

    register(jobOptions, cb) {
        const { type, prefix, options } = jobOptions;
        let queue = this._queues.get(type);
        if (!queue) {
            queue = new Queue(type, { ...this._setting, prefix });
            queue.process(type, options.concurrency, (job, done) => {
                cb({ job: job.data, done });
            });
            this._queues.set(type, queue);
        }
    }

    async close({ type }) {
        const queue = this._queues.get(type);
        if (queue) {
            await queue.close();
            this._queues.delete(type);
        }
    }

    pause(options) {
        const queue = this._queues.get(options.type);
        if (!queue) {
            throw new Error(`unable to find handler for ${options.type}`);
        }
        return queue.pause(true, true);
    }

    resume(options) {
        const queue = this._queues.get(options.type);
        if (!queue) {
            throw new Error(`unable to find handler for ${options.type}`);
        }
        return queue.resume(true);
    }
}

module.exports = Consumer;
