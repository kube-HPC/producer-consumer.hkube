const { expect } = require('chai');
const Redis = require('ioredis');
const { Producer, Consumer } = require('../index');
const Queue = require('bull');

const redisHost = process.env.REDIS_CLUSTER_SERVICE_HOST || '127.0.0.1';
const redisPort = process.env.REDIS_CLUSTER_SERVICE_PORT || "6379";
const useCluster = process.env.REDIS_CLUSTER_SERVICE_HOST ? true : false;
const redisConfig = { host: redisHost, port: redisPort, cluster: useCluster };

const globalOptions = {
    job: {
        type: 'test-job-global',
        data: { action: 'bla' },
        waitingTimeout: 5000
    },
    queue: {
        priority: 1,
        delay: 1000,
        timeout: 5000,
        attempts: 3,
        removeOnComplete: true,
        removeOnFail: false
    },
    setting: {
        prefix: 'sf-jobs',
        redis: {
            host: '127.0.0.1',
            port: "6379",
            cluster: true,
            sentinel: false
        }
    }
}

describe('Test', function () {
    describe('Producer', function () {
        describe('Validation', function () {
            it('should throw validation error is no options', function () {
                const options = {
                    setting:{
                        prefix:1
                    }
                };
                expect(()=>new Producer(options)).to.throw('data.prefix should be string');
            });
            it('should throw validation error is not typeof', function (done) {
                const options = {
                    job: {
                        type: 'test-job',
                        waitingTimeout: 'bla'
                    }
                };
                const producer = new Producer(options);
                producer.createJob(options).catch((error) => {
                    expect(error.message).to.equal('data.job.waitingTimeout should be integer');
                    done();
                });
            });
            it('should throw validation error is required', function (done) {
                const options = { job: {} };
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
                producer.createJob(globalOptions).then((jobID) => {
                    expect(jobID).to.be.a('string');
                    done();
                });
            });
            it('should create job fire event job-failed', function (done) {
                const options = {
                    job: {
                        type: 'test-job-job-event-failed',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: {
                            host: '127.0.0.1',
                            port: "6379",
                        }
                    }
                }
                const producer = new Producer(options);
                producer.on('job-failed', (data) => {
                    expect(data.jobID).to.be.a('string');
                    expect(data.error).to.equal('test-job has been failed');
                    done();
                });
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    job.done(new Error('test-job has been failed'))
                });
                consumer.register(options);
                producer.createJob(options);
            });
            it('should create call job-failed when queue.process callback throws', function (done) {
                const options = {
                    job: {
                        type: 'test-job-job-event-failed-throw',
                        data: { action: 'bla' }
                    },
                    setting: {
                        redis: {
                            host: '127.0.0.1',
                            port: "6379",
                        }
                    }
                }
                const producer = new Producer(options);
                producer.on('job-failed', (data) => {
                    expect(data.jobID).to.be.a('string');
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
                    }
                }
                const producer = new Producer(options);
                producer.on('job-completed', (data) => {
                    expect(data.jobID).to.be.a('string');
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
                    }
                }
                const producer = new Producer(options);
                producer.on('job-active', (data) => {
                    expect(data.jobID).to.be.a('string');
                    done();
                });
                const consumer = new Consumer(options);
                consumer.register(options);
                producer.createJob(options);
            });
            it('should create job fire event job-active (resolveOnStart)', function (done) {
                this.timeout(5000);
                const options = {
                    job: {
                        type: 'test-job-job-event-active-resolveOnStart',
                        data: { action: 'bla' },
                        resolveOnStart:true
                    }
                }
                const producer = new Producer(options);
                let activeFlag=false;
                producer.on('job-active', (data) => {
                    expect(data.jobID).to.be.a('string');
                    activeFlag=true;
                });
                const consumer = new Consumer(options);
                consumer.register(options);
                producer.createJob(options).then((data)=>{
                    expect(data.jobID).to.be.a('string');
                    expect(activeFlag).to.be.true;
                    done();
                });
            });
            it('should create job fire event job-waiting (resolveOnWaiting)', function (done) {
                this.timeout(5000);
                const options = {
                    job: {
                        type: 'test-job-job-event-active-resolveOnStart',
                        data: { action: 'bla' },
                        resolveOnWaiting:true
                    }
                }
                const producer = new Producer(options);
                let activeFlag=false;
                producer.on('job-waiting', (data) => {
                    expect(data.jobID).to.be.a('string');
                    activeFlag=true;
                });
                const consumer = new Consumer(options);
                consumer.register(options);
                producer.createJob(options).then((data)=>{
                    expect(data.jobID).to.be.a('string');
                    expect(activeFlag).to.be.true;
                    done();
                });
            });

            it('should create job and resolve on completed', async function () {
                const res = { success: true };
                const options = {
                    job: {
                        type: 'test-job-job-completed',
                        data: { action: 'bla' },
                        resolveOnComplete: true
                    }
                }
                const producer = new Producer(options);
                const consumer = new Consumer(options);
                consumer.on('job', (job) => {
                    job.done(null, res);
                });
                consumer.register(options);
                const data = await producer.createJob(options);
                expect(data.jobID).to.be.a('string');
                expect(data.result).to.deep.equal(res);
            });
            it('should create job and reject on timeout', function (done) {
                this.timeout(5000);
                let job = null;
                const res = { success: true };
                const options = {
                    job: {
                        type: 'test-job-reject-timeout',
                        data: { action: 'bla' },
                        waitingTimeout: 2000,
                        resolveOnStart: true
                    },
                    setting: {
                        prefix: 'sf-jobs-reject-timeout'
                    }
                }
                const producer = new Producer(options);
                producer.createJob(options).catch(error => {
                    expect(error.message).to.have.string('job-waiting-timeout');
                    done();
                }); 
            });
            it('should create two differnt jobs', async function () {
                const options1 = {
                    job: {
                        type: 'test-job-ids',
                        data: { action: 'test-1' },
                        waitingTimeout: 5000
                    },
                    setting: {
                        prefix: 'sf-jobs1'
                    }
                }
                const options2 = {
                    job: {
                        type: 'test-job-ids',
                        data: { action: 'test-2' },
                        waitingTimeout: 5000
                    },
                    setting: {
                        prefix: 'sf-jobs2'
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
    });
    describe('Consumer', function () {
        describe('Validation', function () {
            it('should throw validation error prefix is not of a type', function () {
                const options = {
                    job: {
                        type: 'test-job',
                    },
                    setting: {
                        prefix: []
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
        });
        describe('ResumeJob', function () {
            it('should consume a job with properties', async function () {
                const options = {
                    job: {
                        type: 'test-job-resume',
                        data: { action: 'bla' }
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
                        prefix: 'jobs-stress'
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
                        prefix: 'jobs-stress-2'
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
