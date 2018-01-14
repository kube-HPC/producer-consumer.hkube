const EventEmitter = require('events');
const Queue = require('bull');
const validate = require('djsv');
const uuidv4 = require('uuid/v4');
const { producerSchema, producerSettingsSchema } = require('./schema');
const json = require('../helpers/json');
const redis = require('../helpers/redis');
const JobData = require('../entities/JobData')
const TRACE_NAME = 'producer'

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
    }

    createJob(option) {
        return new Promise((resolve, reject) => {
            const result = validate(producerSchema, option);
            if (!result.valid) {
                return reject(new Error(result.error));
            }
            const options = result.instance;
            let queue = this._queues.get(options.job.type);
            if (!queue) {
                queue = new Queue(options.job.type, this._setting);
                queue.on('global:waiting', (jobID, type) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        const jobData = this._createJobData(jobID, job.options);
                        this.emit('job-waiting', jobData);
                        if (job.options.resolveOnWaiting) {
                            job.resolve(jobData);
                        }
                    }
                }).on('global:active', (jobID, jobPromise) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        clearTimeout(job.timeout);
                        const jobData = this._createJobData(jobID, job.options);
                        this.emit('job-active', jobData);
                        if (job.options.resolveOnStart) {
                            job.resolve(jobData);
                        }
                    }
                }).on('global:failed', (jobID, err) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        clearTimeout(job.timeout);
                        this._finishTraceSpan(jobID, new Error(err));
                        const jobData = this._createJobData(jobID, job.options, err);
                        this.emit('job-failed', jobData);
                        this._jobMap.delete(job.id);
                        if (!this._shouldResolveOnCreate(job.options)) {
                            job.reject(err);
                        }
                    }
                }).on('global:completed', (jobID, result) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        const jobData = this._createJobData(jobID, job.options, null, json.tryParse(result));
                        this._finishTraceSpan(jobID);
                        this.emit('job-completed', jobData);
                        this._jobMap.delete(job.id);
                        if (job.options.resolveOnComplete) {
                            job.resolve(jobData);
                        }
                    }
                }).on('error', (error) => {
                    this.emit('job-error', error);
                }).on('global:stalled', (jobID) => {
                    const job = this._jobMap.get(jobID);
                    if (job) {
                        const jobData = this._createJobData(jobID, job.options);
                        this.emit('job-stalled', jobData);
                        if (!this._shouldResolveOnCreate(job.options)) {
                            job.reject(new Error(`job-stalled (id: ${jobID})`));
                        }
                    }
                });
                this._queues.set(options.job.type, queue);
            }
            let job, timer;
            const jobID = options.job.id || this.createJobID(options.job.type);
            options.queue.jobId = jobID;

            if (options.job.waitingTimeout > 0) {
                timer = setTimeout(() => {
                    if (job) {
                        job.discard();
                        job.remove();
                    }
                    this._jobMap.delete(jobID);
                    return reject(new Error(`job-waiting-timeout (id: ${jobID})`));
                }, options.job.waitingTimeout);
            }
            const jobData = {
                id: jobID,
                timeout: timer,
                resolve: resolve,
                reject: reject,
                options: options.job
            };

            this._jobMap.set(jobID, jobData);

            if (this._setting.tracer) {
                const span = this._setting.tracer.startSpan({
                    id: jobID,
                    name: TRACE_NAME,
                    ...options.tracing
                });
                span.addTag({
                    jobID
                })
                options.job.data = options.job.data || {};
                options.job.data.spanId = span.context();
            }
            queue.add(options.job.type, options.job.data, options.queue).then((result) => {
                job = result;
                if (this._shouldResolveOnCreate(options.job)) {
                    return resolve(jobID);
                }
            }).catch((error) => {
                return reject(error);
            });
        })
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
        jobs.forEach(j => {
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
        return await queue.getJob(options.jobID);
    }

    async stopJob(options) {
        let job = await this.getJob(options);
        if (job) {
            await job.discard();
            await job.remove();
        }
    }

    _createJobData(jobID, options, error, result) {
        return new JobData({
            id: jobID,
            options: options,
            prefix: this._setting.prefix,
            error: error,
            result: result
        });
    }

    createJobID(type) {
        return [type, uuidv4()].join(':');
    }
}

module.exports = Producer;


