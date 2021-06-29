const EventEmitter = require('events');
const Validator = require('ajv');
const { uuid } = require('@hkube/uid');
const Events = require('../consts/Events');
const providers = require('../providers/providers');
const { producerSchema, createJobSchema } = require('./schema');
const TRACE_NAME = 'producer';
const validator = new Validator({ useDefaults: true, coerceTypes: true });

class ProducerClass extends EventEmitter {
    constructor(config) {
        super();
        const valid = validator.validate(producerSchema, config);
        if (!valid) {
            throw new Error(validator.errorsText(validator.errors));
        }
        const { provider, options, tracer, connection } = config;
        const { Producer } = providers[provider];
        this._producer = new Producer(connection);
        this._producer.on(Events.WAITING, (job) => {
            this.emit(Events.WAITING, job);
        });
        this._producer.on(Events.ACTIVE, (job) => {
            this.emit(Events.ACTIVE, job);
        });
        this._producer.on(Events.FAILED, (err, job) => {
            this._finishTraceSpan(job.jobId, new Error(err));
            this.emit(Events.FAILED, err, job);
        });
        this._producer.on(Events.COMPLETED, (job) => {
            this._finishTraceSpan(job.jobId);
            this.emit(Events.COMPLETED, job);
        });
        this._producer.on(Events.ERROR, (err) => {
            this.emit(Events.ERROR, err);
        });
        this._producer.on(Events.STALLED, (job) => {
            this.emit(Events.STALLED, job);
        });
        this._setting = options;
        this._tracer = tracer;
        this._checkStalledJobs();
    }

    _checkStalledJobs() {
        if (this._setting.enableCheckStalledJobs) {
            this._stalledJobsInterval();
        }
    }

    _stalledJobsInterval() {
        if (this._interval) {
            return;
        }
        this._interval = setInterval(async () => {
            if (this._isIntervalActive) {
                return;
            }
            try {
                this._isIntervalActive = true;
                await this._producer.checkStalledJobs();
            }
            catch (e) {
                this.emit('check-stalled-error', e);
            }
            finally {
                this._isIntervalActive = false;
            }
        }, this._setting.checkStalledJobsInterval);
    }

    async close({ type }) {
        const queue = this._queues.get(type);
        if (queue) {
            await queue.close();
            this._queues.delete(type);
        }
        clearInterval(this._interval);
        this._interval = null;
    }

    getQueueByJobType(jobType) {
        return this._queues.get(jobType);
    }

    getQueues() {
        return this._queues;
    }

    async createJob(jobOptions) {
        const valid = validator.validate(createJobSchema, jobOptions);
        if (!valid) {
            throw new Error(validator.errorsText(validator.errors));
        }
        const { id, type, prefix, data, tracing, options } = jobOptions;
        const name = `${prefix}:${type}`;
        const jobId = id || this.createJobID(type);
        options.jobId = jobId;

        if (this._tracer) {
            const span = this._tracer.startSpan({
                id: jobId,
                name: TRACE_NAME,
                ...tracing
            });
            span.addTag({
                jobId
            });
            data.spanId = span.context();
        }
        const res = await this._producer.createJob({ name, jobId, type, prefix, data, options });
        return res;
    }

    _finishTraceSpan(id, error) {
        if (this._tracer) {
            const span = this._tracer.topSpan(id);
            if (span) {
                span.finish(error);
            }
        }
    }

    async getJob(options) {
        return this._producer.getJob(options);
    }

    async stopJob(options) {
        return this._producer.stopJob(options);
    }

    createJobID(type) {
        return [type, uuid()].join(':');
    }
}

module.exports = ProducerClass;
