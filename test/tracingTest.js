const { expect } = require('chai');
const Redis = require('ioredis');
const { Producer, Consumer } = require('../index');
const Queue = require('bull');

const redisHost = process.env.REDIS_CLUSTER_SERVICE_HOST || '127.0.0.1';
const redisPort = process.env.REDIS_CLUSTER_SERVICE_PORT || "6379";
const useCluster = process.env.REDIS_CLUSTER_SERVICE_HOST ? true : false;
const redisConfig = { host: redisHost, port: redisPort, cluster: useCluster };

const { tracer } = require('@hkube/metrics')
const { InMemoryReporter, ConstSampler, RemoteReporter } = require('jaeger-client');
const opentracing = require('opentracing')
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

describe('Tracing', () => {
    // beforeEach((done) => {
    //     tracer._spanStack = [];
    //     if (tracer._tracer) {
    //         tracer._tracer.close(() => {
    //             tracer._tracer = null;
    //             done();
    //         });
    //     }
    //     else {
    //         done();
    //     }
    // });
    it('should work without tracing', (done) => {
        let job = null;
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test',
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

    it('should work with job-completed', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }

        });
        let job = null;
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test-2',
                data: { action: 'bla' },
            },
            tracing: {

            },
            setting: {
                tracer
            }
        }
        return new Promise((resolve, reject) => {
            const producer = new Producer(options);
            producer.on('job-completed', (data) => {
                expect(data.jobID).to.be.a('string');
                expect(data.result).to.deep.equal(res);
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                resolve();
            });
            const consumer = new Consumer(options);
            consumer.on('job', (job) => {
                expect(job.data.spanId).to.not.be.empty
                job.done(null, res);
            });
            consumer.register(options);
            producer.createJob(options);
        });
        
    });

    it('should add tags', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }

        });
        let job = null;
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test-tags',
                data: { action: 'bla' },
            },
            tracing: {
                tags: {
                    tag1:'val1'
                }
            },
            setting: {
                tracer
            }
        }
        return new Promise((resolve, reject) => {
            const producer = new Producer(options);
            producer.on('job-completed', (data) => {
                expect(data.jobID).to.be.a('string');
                expect(data.result).to.deep.equal(res);
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'jobID', value: data.jobID });
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'tag1', value: 'val1' });
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: 'jobID', value: data.jobID });
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: 'tag1', value: 'val1' });
                resolve();
            });
            const consumer = new Consumer(options);
            consumer.on('job', (job) => {
                expect(job.data.spanId).to.not.be.empty
                job.done(null, res);
            });
            consumer.register(options);
            producer.createJob(options);
        });
        
    });
    it('should work without options.tracing job-completed', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }

        });
        let job = null;
        const res = { success: true };
        const optionsProducer = {
            job: {
                type: 'tracing-test-2a',
                data: { action: 'bla' },
            },
            setting: {
                tracer
            }
        }
        const optionsConsumer = {
            job: {
                type: 'tracing-test-2a',
            },
            setting: {
                tracer
            }
        }
        const prom =  new Promise((resolve, reject) => {
            const producer = new Producer(optionsProducer);
            producer.on('job-completed', (data) => {
                expect(data.jobID).to.be.a('string');
                expect(data.result).to.deep.equal(res);
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                resolve();
            });
            const consumer = new Consumer(optionsConsumer);
            consumer.on('job', (job) => {
                expect(job.data.spanId).to.not.be.empty
                job.done(null, res);
            });
            consumer.register(optionsConsumer);
            producer.createJob(optionsProducer);
        });
        await prom;
    });
    it('should work with job-failed', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }

        });
        let job = null;
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test-3',
                data: { action: 'bla' },
            },
            tracing: {

            },
            setting: {
                tracer
            }
        }

        return new Promise((resolve, reject) => {
            const producer = new Producer(options);
            producer.on('job-failed', (data) => {
                expect(data.jobID).to.be.a('string');
                expect(data.error).to.equal('Nooooooo!!!!!');
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: opentracing.Error, value: true });
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'errorMessage', value: 'Nooooooo!!!!!' });
                expect(tracer._tracer._reporter.spans[0]._operationName).to.eq('tracing-test-3 start');
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: opentracing.Error, value: true });
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: 'errorMessage', value: 'Nooooooo!!!!!' });
                expect(tracer._tracer._reporter.spans[1]._operationName).to.eq('producer');
                resolve();
            });
            const consumer = new Consumer(options);
            consumer.on('job', (job) => {
                expect(job.data.spanId).to.not.be.empty
                job.done(new Error('Nooooooo!!!!!'));
            });
            consumer.register(options);
            producer.createJob(options);
        });
        
    });
});