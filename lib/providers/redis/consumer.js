const EventEmitter = require('events');
const Queue = require('@hkube/bull');
const redis = require('../../helpers/redis');

class RedisConsumer extends EventEmitter {
    constructor(connection) {
        super();
        this._queues = new Map();
        redis.init(connection);
        this._redisClient = redis.client;
    }

    register(jobOptions, cb) {
        const { name, options } = jobOptions;
        let queue = this._queues.get(name);
        if (!queue) {
            queue = new Queue(name, this._redisClient);
            queue.process(name, options.concurrency, (job, done) => {
                cb({ job: job.data, done });
            });
            this._queues.set(name, queue);
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

module.exports = RedisConsumer;
