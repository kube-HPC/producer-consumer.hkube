const consumerSchema = {
    name: 'options',
    type: 'object',
    properties: {
        job: {
            type: 'object',
            properties: {
                type: {
                    type: 'string',
                    description: 'the job type'
                }
            },
            required: [
                'type'
            ]
        },
        tracing: {
            type: 'object',
            properties: {
                tags: { type: 'object' }
            },
            additionalProperties: false,
        }
    },
    'default': {}
}

const consumerSettingSchema = {
    type: 'object',
    properties: {
        prefix: {
            type: 'string',
            'default': 'jobs',
            description: 'prefix for all queue keys'
        },
        redis: {
            type: 'object',
            properties: {
                host: {
                    type: 'string',
                    'default': 'localhost'
                },
                port: {
                    anyOf: [
                        {
                            type: [
                                'integer',
                                'string'
                            ]
                        }
                    ],
                    'default': 6379
                }
            },
            'default': {}
        },
        tracer: { type: 'object' }
    },
    'default': {}
}
module.exports = {
    consumerSchema,
    consumerSettingSchema
}