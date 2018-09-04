const EventEmitter = require('events');
const Queue = require('bull');
const validate = require('djsv');
const { consumerSettingSchema, consumerSchema } = require('./schema');
const redis = require('../helpers/redis');

class Consumer extends EventEmitter {
    constructor(options) {
        super();
        const result = validate(consumerSettingSchema, options.setting);
        if (!result.valid) {
            throw new Error(result.error);
        }
        this._queues = new Map();
        this._setting = result.instance;
        redis.init(this._setting.redis);
        this._setting = Object.assign({}, this._setting, redis.client);
    }

    register(option) {
        const result = validate(consumerSchema, option);
        if (!result.valid) {
            throw new Error(result.error);
        }
        const options = result.instance;
        let queue = this._queues.get(options.job.type);
        if (!queue) {
            queue = new Queue(options.job.type, this._setting);
            queue.process(options.job.type, options.job.concurrency, (job, done) => {
                let span;
                const resultData = {
                    id: job.id,
                    data: job.data,
                    type: job.name,
                    prefix: job.queue.keyPrefix,
                    done: (error, res) => {
                        if (this._setting.tracer) {
                            if (span) {
                                span.finish(error);
                            }
                        }
                        done(error, res);
                    }
                };
                if (this._setting.tracer) {
                    span = this._setting.tracer.startSpan({
                        name: `${options.job.type} start`,
                        parent: job.data.spanId,
                        ...options.tracing
                    });
                    span.addTag({
                        jobID: job.id
                    });
                }
                this.emit('job', resultData);
            });
            this._queues.set(options.job.type, queue);
        }
    }

    pause(options) {
        const queue = this._queues.get(options.type);
        if (!queue) {
            throw new Error(`unable to find handler for ${options.type}`);
        }
        return queue.pause(true);
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
