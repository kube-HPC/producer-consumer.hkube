const { expect } = require('chai');
const sinon = require('sinon');
const { uuid } = require('@hkube/uid');
const { Producer, Consumer } = require('../index');

const provider = 'redis';

const connection = {
    host: '127.0.0.1',
    port: 6379,
    sentinel: false
};

const producerOptions = {
    provider,
    connection,
    options: {
        enableCheckStalledJobs: false,
    },
    tracer: null,
};

const producerJobOptions = {
    prefix: 'jobs',
    type: 'test-job',
    data: { jobId: uuid(), action: 'bla' },
    options: {
        priority: 1,
        delay: 1000,
        timeout: 5000,
        attempts: 3,
        removeOnComplete: true,
        removeOnFail: false
    }
};

const consumerOptions = {
    provider,
    connection,
    tracer: null,
};

const consumerJobOptions = {
    prefix: 'jobs',
    type: 'test-job',
    options: {
        concurrency: 1,
    }
};

const delay = d => new Promise(r => setTimeout(r, d));

describe('Test', function () {
    describe('Producer', function () {
        describe('Validation', function () {
            it('should throw validation error is no options', function () {
                const options = {
                    setting: {
                        prefix: [],
                        redis: redisConfig
                    }
                };
                expect(() => new Producer(options)).to.throw('data.prefix should be string');
            });
            it('should throw validation error is required', function (done) {
                const options = {
                    job: {
                    },
                    setting: {
                        redis: redisConfig
                    }
                };
                const producer = new Producer(options);
                producer.createJob(options).catch((error) => {
                    expect(error.message).to.equal("data.job should have required property 'type'");
                    done();
                });
            });
        });
        describe('CreateJob', function () {
            it('should create job and return job id', function (done) {
                const producer = new Producer(globalOptions);
                producer.createJob(globalOptions).then((jobId) => {
                    expect(jobId).to.be.a('string');
                    done();
                });
            });
            it('should create job fire event job-failed', function (done) {
                const producer = new Producer(producerOptions);
                producer.on('job-failed', (err, job) => {
                    expect(job.jobId).to.be.a('string');
                    expect(err).to.equal('test-job has been failed');
                    done();
                });
                const consumer = new Consumer(consumerOptions);
                consumer.on('job', (job) => {
                    job.done(new Error('test-job has been failed'))
                });
                consumer.register(consumerJobOptions);
                producer.createJob(producerJobOptions);
            });
            it('should create call job-failed when queue.process callback throws', function (done) {
                const options = {
                    job: {
                        type: 'test-job-job-event-failed-throw',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                producer.on('job-failed', (data) => {
                    expect(data.jobId).to.be.a('string');
                    expect(data.error).to.equal('No!!!!!!');
                    done();
                });
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    throw new Error('No!!!!!!');
                });
                consumer.register(options);
                producer.createJob(options);
            });
            it('should create job fire event job-completed', function (done) {
                let job = null;
                const res = { success: true };
                const options = {
                    job: {
                        type: 'test-job-job-event-completed',
                        data: { action: 'bla' },
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                producer.on('job-completed', (data) => {
                    expect(data.jobId).to.be.a('string');
                    expect(data.result).to.deep.equal(res);
                    done();
                });
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    job.done(null, res);
                });
                consumer.register(options);
                producer.createJob(options);
            });
            it('should create job with prefix fire event job-completed', function (done) {
                const res = { success: true };
                const options = {
                    job: {
                        prefix: 'job-prefix',
                        type: 'test-job-job-event-completed',
                        data: { action: 'bla' },
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                producer.on('job-completed', (data) => {
                    expect(data.jobId).to.be.a('string');
                    expect(data.result).to.deep.equal(res);
                    done();
                });
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    job.done(null, res);
                });
                consumer.register(options);
                producer.createJob(options);
            });
            it('should create job fire event job-active', function (done) {
                this.timeout(5000);
                const options = {
                    job: {
                        type: 'test-job-job-event-active',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                producer.on('job-active', (data) => {
                    expect(data.jobId).to.be.a('string');
                    done();
                });
                const consumer = new Consumer(options);
                consumer.register(options);
                producer.createJob(options);
            });
            it('should create two different jobs', async function () {
                const options1 = {
                    job: {
                        type: 'test-job-ids',
                        data: { action: 'test-1' }
                    },
                    setting: {
                        prefix: 'sf-jobs1',
                        redis: redisConfig
                    }
                }
                const options2 = {
                    job: {
                        type: 'test-job-ids',
                        data: { action: 'test-2' }
                    },
                    setting: {
                        prefix: 'sf-jobs2',
                        redis: redisConfig
                    }
                }
                const res1 = { success: 'consumer-result-1' };
                const res2 = { success: 'consumer-result-2' };
                const consumer1 = new Consumer(options1);
                const consumer2 = new Consumer(options2);
                consumer1.register(options1);
                consumer2.register(options2);

                consumer1.on('job', (job) => {
                    job.done(null, res1)
                });
                consumer2.on('job', (job) => {
                    job.done(null, res2)
                });

                const producer1 = new Producer(options1);
                const producer2 = new Producer(options2);

                const results = await Promise.all([producer1.createJob(options1), producer2.createJob(options2)]);
                expect(results).to.have.lengthOf(2);
            });
        });
        describe('StalledJobs', function () {
            it('should get stalled jobs array', async function () {
                const producer = new Producer(globalOptions);
                const queue = producer._createQueue('stalled');
                await delay(1000)
                const stalled = await producer._getStalledJobs(queue);
                expect(stalled).to.be.an('array');
            });
            it('should get stalled jobs array', async function () {
                const producer = new Producer(globalOptions);
                const queue = producer._createQueue('stalled');
                await delay(1000);
                const stalled = await producer._getStalledJobs(queue);
                expect(stalled).to.be.an('array');
            });
        });
    });
    describe('Consumer', function () {
        describe('Validation', function () {
            it('should throw validation error prefix is not of a type', function () {
                const options = {
                    job: {
                        type: 'test-job',
                    },
                    setting: {
                        prefix: [],
                        redis: redisConfig
                    }
                };
                const func = () => new Consumer(options)
                expect(func).to.throw(Error, 'data.prefix should be string');
            });
        });
        describe('ConsumeJob', function () {
            it('should consume a job with properties', async function () {
                const options = {
                    job: {
                        type: 'test-job-properties',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    expect(job).to.have.property('id');
                    expect(job).to.have.property('data');
                    expect(job).to.have.property('type');
                    expect(job).to.have.property('key');
                    expect(job).to.have.property('done');
                    done();
                });
                consumer.register(options);
                await producer.createJob(options);
            });
        });
        describe('PauseJob', function () {
            it('should consume a job with properties', async function () {
                const options = {
                    job: {
                        type: 'test-job-pause',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    consumer.pause();
                    done();
                });
                consumer.register(options);
                await producer.createJob(options);
            });
            it('should not consume when calling pause', async function () {
                const options = {
                    job: {
                        type: 'test-job-not-consume-after-pause',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const callback = sinon.spy();
                const producer = new Producer(options);
                const consumer = new Consumer(options);
                consumer.on('job', callback);
                consumer.register(options);
                await consumer.pause({ type: options.job.type });
                await producer.createJob(options);
                await delay(200);
                expect(callback.called).to.be.false;
            });
        });
        describe('ResumeJob', function () {
            it('should consume a job with properties', async function () {
                const options = {
                    job: {
                        type: 'test-job-resume',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    consumer.resume();
                    done();
                });
                consumer.register(options);
                await producer.createJob(options);
            });
        });
    });
    describe('Stress', function () {
        describe('CreateJob', function () {
            it('should create job multiple times and set of results', function (done) {
                this.timeout(5000);
                const options = {
                    job: {
                        type: 'test-job-stress-produce',
                        data: { action: 'test' }
                    },
                    setting: {
                        prefix: 'jobs-stress',
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                const numOfJobs = 100;
                const range = Array.from({ length: numOfJobs }, (value, key) => (`queue-stress:jobs-stress:${key + 1}`));
                const promises = range.map(() => producer.createJob(options));
                Promise.all(promises).then((result) => {
                    expect(result).to.have.lengthOf(numOfJobs);
                    done();
                })
            });
            it('should create and consume job multiple times', function (done) {
                this.timeout(5000);
                const options = {
                    job: {
                        type: 'test-job-stress-consume',
                        data: { action: 'test' }
                    },
                    setting: {
                        prefix: 'jobs-stress-2',
                        redis: redisConfig
                    }
                }
                const producer = new Producer(options);
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    job.done(null, job.id);
                });
                consumer.register(options);
                const numOfJobs = 100;
                const range = Array.from({ length: numOfJobs }, (value, key) => (`queue-stress-2:jobs-stress-2:${key + 1}`));
                const promises = range.map(() => producer.createJob(options));
                Promise.all(promises).then((result) => {
                    expect(result).to.have.lengthOf(numOfJobs);
                    done();
                })
            });
        });
    });
});
