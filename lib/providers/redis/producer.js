const EventEmitter = require('events');
const Queue = require('@hkube/bull');
const Events = require('../../consts/Events');
const redis = require('../../helpers/redis');

class RedisProducer extends EventEmitter {
    constructor(connection) {
        super();
        this._jobMap = new Map();
        this._queues = new Map();
        redis.init(connection);
        this._redisClient = redis.client;
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

    createJob(jobOptions) {
        return new Promise((resolve, reject) => {
            const { jobId, name, data, options } = jobOptions;
            const queue = this._createQueue(name);

            queue.add(name, data, options).then(() => {
                this._jobMap.set(jobId, data);
                return resolve(jobId);
            }).catch((error) => {
                return reject(error);
            });
        });
    }

    _createQueue(name) {
        let queue = this._queues.get(name);
        if (!queue) {
            queue = new Queue(name, this._redisClient);
            queue.on('global:waiting', (jobId) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    this.emit(Events.WAITING, job);
                }
            }).on('global:active', (jobId) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    this.emit(Events.ACTIVE, job);
                }
            }).on('global:failed', (jobId, err) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    this.emit(Events.FAILED, err, job);
                    this._jobMap.delete(jobId);
                }
            }).on('global:completed', (jobId) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    this.emit(Events.COMPLETED, job);
                    this._jobMap.delete(jobId);
                }
            }).on('error', (error) => {
                this.emit(Events.ERROR, error);
            }).on('global:stalled', (jobId) => {
                const job = this._jobMap.get(jobId);
                if (job) {
                    this.emit(Events.STALLED, job);
                }
            });
            this._queues.set(name, queue);
        }
        return queue;
    }

    async getJob(options) {
        let queue = this._queues.get(options.type);
        if (!queue) {
            queue = new Queue(options.type, this._setting);
        }
        return queue.getJob(options.jobId);
    }

    async checkStalledJobs() {
        await Promise.all([...this._queues.values()].map(q => this._getStalledJobs(q)));
    }

    async _getStalledJobs(queue) {
        const stalled = await queue.getStalledJobs();
        stalled.forEach(async (j) => {
            const { jobId } = j.data;
            await this._cleanJob(j);
            const jobData = this._createJobData(jobId, j.data, 'StalledJob');
            this.emit(Events.STUCK, jobData);
        });
        return stalled;
    }

    async stopJob(options) {
        const job = await this.getJob(options);
        if (job) {
            await this._cleanJob(job);
        }
    }

    async _cleanJob(job) {
        await job.discard();
        await job.remove();
    }
}

module.exports = RedisProducer;
