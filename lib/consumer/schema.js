const consumerSchema = {
    type: 'object',
    properties: {
        provider: {
            type: 'string',
            default: 'mongo',
        },
        connection: {
            type: 'object'
        },
        tracer: {
            type: 'object',
            nullable: true
        }
    },
};

const registerSchema = {
    type: 'object',
    properties: {
        prefix: {
            type: 'string',
            default: 'jobs',
            description: 'prefix for all queue keys'
        },
        type: {
            type: 'string',
            description: 'the job type'
        },
        concurrency: {
            type: 'integer',
            default: 1
        },
        tracing: {
            type: 'object',
            properties: {
                tags: { type: 'object' }
            },
            additionalProperties: false,
        }
    }
};

module.exports = {
    consumerSchema,
    registerSchema
};
