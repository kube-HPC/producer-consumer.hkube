const redisFactory = require('@hkube/redis-utils').Factory;

class Helper {
    init(options) {
        if (this._init) {
            return;
        }
        this._client = redisFactory.getClient(options);
        this._subscriber = redisFactory.getClient(options);
        this._redisOptions = {
            createClient: (type) => {
                switch (type) {
                    case 'client':
                        return this._client;
                    case 'subscriber':
                        return this._subscriber;
                    default:
                        return redisFactory.getClient(options);
                }
            }
        };
        this._init = true;
    }

    updateMaxListeners(clientMaxListeners, subscriberMaxListeners) {
        if (this._client && clientMaxListeners) {
            this._client.setMaxListeners(clientMaxListeners);
        }
        if (this._subscriber && subscriberMaxListeners) {
            this._subscriber.setMaxListeners(subscriberMaxListeners);
        }
    }

    get client() {
        return this._redisOptions;
    }
}

module.exports = new Helper();
