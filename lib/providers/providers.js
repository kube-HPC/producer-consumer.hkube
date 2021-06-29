const ProducerMongo = require('./mongo/producer');
const ConsumerMongo = require('./mongo/consumer');
const ProducerRedis = require('./redis/producer');
const ConsumerRedis = require('./redis/consumer');

const providers = {
    mongo: {
        Producer: ProducerMongo,
        Consumer: ConsumerMongo,
    },
    redis: {
        Producer: ProducerRedis,
        Consumer: ConsumerRedis,
    }
};

module.exports = providers;
