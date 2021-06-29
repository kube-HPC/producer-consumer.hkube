const { expect } = require('chai');
const { Producer, Consumer } = require('../index');
const { tracer } = require('@hkube/metrics')
const { InMemoryReporter } = require('jaeger-client');
const opentracing = require('opentracing');


describe('Tracing', () => {
    it('should work without tracing', (done) => {
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test',
                data: { action: 'bla' },
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
    it('should work with job-completed', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }
        });
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
                expect(data.jobId).to.be.a('string');
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
        const res = { success: true };
        const options = {
            job: {
                type: 'tracing-test-tags',
                data: { action: 'bla' },
            },
            tracing: {
                tags: {
                    tag1: 'val1'
                }
            },
            setting: {
                tracer
            }
        }
        return new Promise((resolve, reject) => {
            const producer = new Producer(options);
            producer.on('job-completed', (data) => {
                expect(data.jobId).to.be.a('string');
                expect(data.result).to.deep.equal(res);
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'jobId', value: data.jobId });
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'tag1', value: 'val1' });
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: 'jobId', value: data.jobId });
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
        const prom = new Promise((resolve, reject) => {
            const producer = new Producer(optionsProducer);
            producer.on('job-completed', (data) => {
                expect(data.jobId).to.be.a('string');
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
                expect(data.jobId).to.be.a('string');
                expect(data.error).to.equal('Nooooooo!!!!!');
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: opentracing.Tags.ERROR, value: true });
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'errorMessage', value: 'Nooooooo!!!!!' });
                expect(tracer._tracer._reporter.spans[0]._operationName).to.eq('tracing-test-3 start');
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: opentracing.Tags.ERROR, value: true });
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
    xit('should work with job-failed empty error', async () => {
        await tracer.init({
            tracerConfig: {
                serviceName: 'test',
            },
            tracerOptions: {
                reporter: new InMemoryReporter()
            }

        });
        const options = {
            job: {
                type: 'tracing-test-4',
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
                expect(data.jobId).to.be.a('string');
                expect(data.error).to.not.exist
                expect(data.options.data.spanId).to.not.be.empty
                expect(tracer._tracer._reporter.spans).to.have.lengthOf(2);
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: opentracing.Tags.ERROR, value: true });
                expect(tracer._tracer._reporter.spans[0]._tags).to.deep.include({ key: 'errorMessage', value: undefined });
                expect(tracer._tracer._reporter.spans[0]._operationName).to.eq('tracing-test-4 start');
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: opentracing.Tags.ERROR, value: true });
                expect(tracer._tracer._reporter.spans[1]._tags).to.deep.include({ key: 'errorMessage', value: "" });
                expect(tracer._tracer._reporter.spans[1]._operationName).to.eq('producer');
                resolve();
            });
            const consumer = new Consumer(options);
            consumer.on('job', (job) => {
                expect(job.data.spanId).to.not.be.empty
                job.done({});
            });
            consumer.register(options);
            producer.createJob(options);
        });

    });
});