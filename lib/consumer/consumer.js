const EventEmitter = require('events');
const Validator = require('ajv');
const providers = require('../providers/providers');
const { consumerSchema, registerSchema } = require('./schema');
const validator = new Validator({ useDefaults: true, coerceTypes: true });

class ConsumerClass extends EventEmitter {
    constructor(config) {
        super();
        const valid = validator.validate(consumerSchema, config);
        if (!valid) {
            throw new Error(validator.errorsText(validator.errors));
        }
        const { provider, connection, tracer } = config;
        const { Consumer } = providers[provider];
        this._consumer = new Consumer(connection);
        this._tracer = tracer;
    }

    async register(jobOptions) {
        const valid = validator.validate(registerSchema, jobOptions);
        if (!valid) {
            throw new Error(validator.errorsText(validator.errors));
        }
        const { type, prefix, options } = jobOptions;
        const name = `${prefix}:${type}`;

        await this._consumer.register({ type, prefix, name, options }, ({ job, done }) => {
            let span;
            const resultData = {
                data: job,
                done: (error, res) => {
                    if (this._tracer) {
                        if (span) {
                            span.finish(error);
                        }
                    }
                    done(error, res);
                }
            };
            if (this._tracer) {
                span = this._tracer.startSpan({
                    name: `${type} start`,
                    parent: job.spanId,
                    id: job.jobId,
                    ...options.tracing
                });
                span.addTag({
                    jobId: job.id
                });
            }
            this.emit('job', resultData);
        });
    }

    async close(options) {
        return this._consumer.close(options);
    }

    pause(options) {
        return this._consumer.pause(options);
    }

    resume(options) {
        return this._consumer.resume(options);
    }
}

module.exports = ConsumerClass;
