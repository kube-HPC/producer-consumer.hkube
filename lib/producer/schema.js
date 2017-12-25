const parentRelationships = {
    childOf: 'childOf',
    follows: 'follows'
}

const producerSchema = {
    name: "options",
    type: "object",
    properties: {
        job: {
            type: 'object',
            properties: {
                type: {
                    type: 'string',
                    description: 'the job type'
                },
                waitingTimeout: {
                    type: 'integer',
                    description: 'time wait before the job is active/failed/completed'
                },
                resolveOnWaiting: {
                    type: 'boolean',
                    description: 'should resolve when the job is in waiting state'
                },
                resolveOnStart: {
                    type: 'boolean',
                    description: 'should resolve when the job is in active state'
                },
                resolveOnComplete: {
                    type: 'boolean',
                    description: 'should resolve when the job is in completed state'
                }
            },
            required: [
                'type'
            ]
        },
        queue: {
            type: 'object',
            properties: {
                priority: {
                    type: 'integer',
                    description: 'ranges from 1 (highest) to MAX_INT'
                },
                delay: {
                    type: 'integer',
                    description: 'mils to wait until this job can be processed.'
                },
                timeout: {
                    type: 'integer',
                    description: 'milliseconds after which the job should be fail with a timeout error'
                },
                attempts: {
                    type: 'integer',
                    description: 'total number of attempts to try the job until it completes'
                },
                removeOnComplete: {
                    type: 'boolean',
                    description: 'If true, removes the job when it successfully completes',
                    'default': true
                },
                removeOnFail: {
                    type: 'boolean',
                    description: 'If true, removes the job when it fails after all attempts',
                    'default': true
                }
            },
            'default': {}
        },

        tracing: {
            type: 'object',
            properties: {
                id: { type: 'string' },
                name: { type: 'string' },
                parentRelationship: {
                    type: 'string',
                    enum: Object.values(parentRelationships),
                    default: parentRelationships.childOf
                },
                parent: { type: 'object' }
            },
            additionalProperties: false,
        }
    },
    "default": {}
}

const producerSettingsSchema = {
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
    producerSchema,
    parentRelationships,
    producerSettingsSchema
}
