const EventEmitter = require('events');
const Queue = require('agenda');
const Events = require('../../consts/Events');
const mongoHelper = require('../../helpers/mongo');

class Producer extends EventEmitter {
    constructor(connection) {
        super();
        this._connection = connection;
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

    async createJob(options) {
        const { name, jobId, data } = options;
        await this._createClient();
        this._client.now(name, data);
        return jobId;
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

    async getJob(options) {
        let queue = this._queues.get(options.type);
        if (!queue) {
            queue = new Queue(options.type, this._setting);
        }
        return queue.getJob(options.jobId);
    }

    async checkStalledJobs(queue) {
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
