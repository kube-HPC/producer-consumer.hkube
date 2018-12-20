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

    get client() {
        return this._redisOptions;
    }
}

module.exports = new Helper();
