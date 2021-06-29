const parentRelationships = {
    childOf: 'childOf',
    follows: 'follows'
};

const producerSchema = {
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
        },
        options: {
            type: 'object',
            properties: {
                enableCheckStalledJobs: {
                    type: 'boolean',
                    default: true
                },
                checkStalledJobsInterval: {
                    type: 'integer',
                    default: 15000
                }
            }
        }
    }
};

const createJobSchema = {
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
        options: {
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
                    default: true
                },
                removeOnFail: {
                    type: 'boolean',
                    description: 'If true, removes the job when it fails after all attempts',
                    default: false
                }
            }
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
                parent: { type: 'object' },
                tags: { type: 'object' }
            },
            additionalProperties: false,
        },
    }
};

module.exports = {
    producerSchema,
    createJobSchema
};
