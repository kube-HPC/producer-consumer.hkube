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
    provider: 'mongo/redis',
    connection,
    options: {
        enableCheckStalledJobs: false,
    },
    tracer: null,
};

const producer = new Producer(options);
const job = await producer.createJob(options);

```

### Consumer

```js

const { Consumer } = require('@hkube/producer-consumer');
const options = {
    provider: 'mongo/redis',
    connection,
    tracer: null,
};
const consumer = new Consumer(options);
consumer.on('job', (job) => {
    // do some work...
    job.done(null, {result: true}); // success

    // or
    job.done(new Error('oopps..')); // failed
});
consumer.register(options);

```

## License

[MIT](LICENSE)

