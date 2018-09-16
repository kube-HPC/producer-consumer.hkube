const EventEmitter = require('events');
const Queue = require('bull');
const validate = require('djsv');
const uuidv4 = require('uuid/v4');
const { producerSchema, producerSettingsSchema } = require('./schema');
const json = require('../helpers/json');
const Events = require('../consts/Events');
const redis = require('../helpers/redis');
const TRACE_NAME = 'producer';

class Producer extends EventEmitter {
    constructor(options) {
        super();
        const result = validate(producerSettingsSchema, options.setting);
        if (!result.valid) {
            throw new Error(result.error);
        }
        this._jobMap = new Map();
        this._queues = new Map();
        this._setting = result.instance;
        redis.init(this._setting.redis);
        this._setting = Object.assign({}, this._setting, redis.client);
        this._checkJobs();
    }

    _checkJobs() {
        setInterval(() => {
            this._queues.forEach((q) => {
                this._checkFailedJobs(q);
            });
        }, this._setting.checkFailedJobsInterval);
    }

    getQueueByJobType(jobType) {
        return this._queues.get(jobType);
    }

    getQueues() {
        return this._queues;
    }

    createJob(options) {
        return new Promise((resolve, reject) => {
            const result = validate(producerSchema, options);
            if (!result.valid) {
                return reject(new Error(result.error));
            }
            const queue = this._createQueue(options.job.type);
            let job;
            let timer;
            const jobId = options.job.id || this.createJobID(options.job.type);
            options.queue.jobId = jobId;

            if (options.job.waitingTimeout > 0) {
                timer = setTimeout(() => {
                    if (job) {
                        job.discard();
                        job.remove();
                    }
                    this._jobMap.delete(jobId);
                    return reject(new Error(`job-waiting-timeout (id: ${jobId})`));
                }, options.job.waitingTimeout);
            }
            const jobData = {
                id: jobId,
                timeout: timer,
                resolve,
                reject,
                options: options.job
            };

            this._jobMap.set(jobId, jobData);

            if (this._setting.tracer) {
                const span = this._setting.tracer.startSpan({
                    id: jobId,
                    name: TRACE_NAME,
                    ...options.tracing
                });
                span.addTag({
                    jobId
                });
                options.job.data = options.job.data || {};
                options.job.data.spanId = span.context();
            }
            queue.add(options.job.type, options.job.data, options.queue).then((result) => {
                job = result;
                if (this._shouldResolveOnCreate(options.job)) {
                    return resolve(jobId);
                }
            }).catch((error) => {
                return reject(error);
            });
        });
    }

    _createQueue(jobType) {
        let queue = this._queues.get(jobType);
        if (!queue) {
            queue = new Queue(jobType, this._setting);
            queue.on('global:waiting', (jobId, type) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    const jobData = this._createJobData(jobId, job.options);
                    this.emit(Events.WAITING, jobData);
                    if (job.options.resolveOnWaiting) {
                        job.resolve(jobData);
                    }
                }
            }).on('global:active', (jobId, jobPromise) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    clearTimeout(job.timeout);
                    const jobData = this._createJobData(jobId, job.options);
                    this.emit(Events.ACTIVE, jobData);
                    if (job.options.resolveOnStart) {
                        job.resolve(jobData);
                    }
                }
            }).on('global:failed', (jobId, err) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    clearTimeout(job.timeout);
                    this._finishTraceSpan(jobId, new Error(err));
                    const jobData = this._createJobData(jobId, job.options, err);
                    this.emit(Events.FAILED, jobData);
                    this._jobMap.delete(job.id);
                    if (!this._shouldResolveOnCreate(job.options)) {
                        job.reject(err);
                    }
                }
            }).on('global:completed', (jobId, result) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    const jobData = this._createJobData(jobId, job.options, null, json.tryParse(result));
                    this._finishTraceSpan(jobId);
                    this.emit(Events.COMPLETED, jobData);
                    this._jobMap.delete(job.id);
                    if (job.options.resolveOnComplete) {
                        job.resolve(jobData);
                    }
                }
            }).on('error', (error) => {
                this.emit(Events.ERROR, error);
            }).on('global:stalled', (jobId) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    const jobData = this._createJobData(jobId, job.options);
                    this.emit(Events.STALLED, jobData);
                    if (!this._shouldResolveOnCreate(job.options)) {
                        job.reject(new Error(`job-stalled (id: ${jobId})`));
                    }
                }
            });
            this._queues.set(jobType, queue);
        }
        return queue;
    }

    _shouldResolveOnCreate(options) {
        return !options.resolveOnStart && !options.resolveOnComplete;
    }

    _finishTraceSpan(id, error) {
        if (this._setting.tracer) {
            const span = this._setting.tracer.topSpan(id);
            if (span) {
                span.finish(error);
            }
        }
    }

    setJobsState(jobs) {
        jobs.forEach((j) => {
            this._jobMap.set(j.key, {
                id: j.key,
                options: j.value
            });
        });
    }

    async getJob(options) {
        let queue = this._queues.get(options.type);
        if (!queue) {
            queue = new Queue(options.type, this._setting);
        }
        return queue.getJob(options.jobId);
    }

    async _checkFailedJobs(queue) {
        const failed = await queue.getFailed(0, 1000000);
        failed.forEach((j) => {
            const jobId = j.data.jobId;
            this.stopJob({ type: j.name, jobId });
            if (j.failedReason === 'job stalled more than allowable limit') {
                const jobData = this._createJobData(jobId, j.data, 'CrashLoopBackOff');
                this.emit(Events.CRASHED, jobData);
            }
        });
    }

    async stopJob(options) {
        const job = await this.getJob(options);
        if (job) {
            await job.discard();
            await job.remove();
        }
    }

    _createJobData(jobId, options, error, result) {
        return {
            jobId,
            options,
            prefix: this._setting.prefix,
            error,
            result
        };
    }

    createJobID(type) {
        return [type, uuidv4()].join(':');
    }
}

module.exports = Producer;
