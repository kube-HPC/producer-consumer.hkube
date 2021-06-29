const EventEmitter = require('events');
const Queue = require('@hkube/bull');
const json = require('../../helpers/json');
const Events = require('../../consts/Events');
const redis = require('../../helpers/redis');

class Producer extends EventEmitter {
    constructor(connection) {
        super();
        this._jobMap = new Map();
        this._queues = new Map();
        this._connection = connection;
        redis.init(connection);
        this._setting = Object.assign({}, this._setting, redis.client);
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
            const { jobId, type, prefix, data, options } = jobOptions;
            const queue = this._createQueue(type, prefix);

            queue.add(type, data, options).then(() => {
                this._jobMap.set(jobId, data);
                return resolve(jobId);
            }).catch((error) => {
                return reject(error);
            });
        });
    }

    _createQueue(jobType, prefix) {
        let queue = this._queues.get(jobType);
        if (!queue) {
            queue = new Queue(jobType, { ...this._setting, prefix });
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
            this._queues.set(jobType, queue);
        }
        return queue;
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

module.exports = Producer;
