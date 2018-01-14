# Producer consumer

[![Build Status](https://travis-ci.org/kube-HPC/producer-consumer.hkube.svg?branch=master)](https://travis-ci.org/kube-HPC/producer-consumer.hkube)
[![Coverage Status](https://coveralls.io/repos/github/kube-HPC/producer-consumer.hkube/badge.svg?branch=master)](https://coveralls.io/github/kube-HPC/producer-consumer.hkube?branch=master)

producer consumer message queue based on Redis built for Node.js    

## Installation

```bash
$ npm install @hkube/producer-consumer
```

## Basic usage

### Producer

```js

const { Producer } = require('@hkube/producer-consumer');
const options = {
    job: {
        type: 'test-job',
        data: { action: 'bla' },
    }
}
const producer = new Producer(options);
const job = await producer.createJob(options);

```

### Consumer

```js

const { Consumer } = require('@hkube/producer-consumer');
const options = {
    job: {
        type: 'test-job'
    }
}
const consumer = new Consumer(options);
consumer.on('job', (job) => {
    // do some work...
    job.done(null, {result: true}); // success

    // or
    job.done(new Error('oopps..')); // failed
});
consumer.register(options);

```

### Schema

The createJob method will validate the options against the schema

```js
const schema = {
    "properties": {
        "job": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "description": "the job type"
                },
                "waitingTimeout": {
                    "type": "integer",
                    "description": "time wait before the job is active/failed/completed"
                },
                "resolveOnStart": {
                    "type": "boolean",
                    "description": "should resolve when the job is in active state"
                },
                "resolveOnComplete": {
                    "type": "boolean",
                    "description": "should resolve when the job is in completed state"
                }
            }
        },
        "queue": {
            "type": "object",
            "properties": {
                "priority": {
                    "type": "integer",
                    "description": "ranges from 1 (highest) to MAX_INT"
                },
                "delay": {
                    "type": "integer",
                    "description": "miliseconds to wait until this job can be processed."
                },
                "timeout": {
                    "type": "integer",
                    "description": "milliseconds after which the job should be fail with a timeout error"
                },
                "attempts": {
                    "type": "integer",
                    "description": "total number of attempts to try the job until it completes"
                },
                "removeOnComplete": {
                    "type": "boolean",
                    "description": "If true, removes the job when it successfully completes",
                    "default": false
                },
                "removeOnFail": {
                    "type": "boolean",
                    "description": "If true, removes the job when it fails after all attempts",
                    "default": false
                }
            }
        },
        "setting": {
            "type": "object",
            "properties": {
                "prefix": {
                    "type": "string",
                    "default": "queue",
                    "description": "prefix for all queue keys"
                },
                "redis": {
                    "type": "object",
                    "properties": {
                        "host": {
                            "type": "string",
                            "default": "localhost"
                        },
                        "port": {
                            "type": "integer",
                            "default": 6379
                        }
                    }
                }
            }
        }
    }
}
```

## Events

```js
const { Producer } = require('@hkube/producer-consumer');
producer.on('job-failed', (jobID, err) => {       
}).on('job-completed', (jobID, result) => {           
}).on('job-active', (jobID) => {             
});
producer.createJob(options);

const options = {
    job: {
        type: 'test-job',
        data: { action: 'bla' },
    }
}
const producer = new Producer(options);
const job = await producer.createJob(options);
```

## Full Detailed Example

```js
const { producer } = require('@hkube/producer-consumer');
const options = {
    job: {
        resolveOnStart: false,
        resolveOnComplete: false,
        type: 'test-job',
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
        prefix: 'sf-queue',
        redis: {
            host: '127.0.0.1',
            port: 6379,
            cluster: true,
            sentinel: false
        }
    }
}

const job = await producer.createJob(options);

```

## License

[MIT](LICENSE)

