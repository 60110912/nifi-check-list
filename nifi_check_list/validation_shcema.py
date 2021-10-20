from jsonschema import Draft201909Validator, exceptions

nifiValidationShcemas = {}

# ##################ControllerServices#################
# org.apache.nifi.avro.AvroReader
nifiValidationShcemas['org.apache.nifi.avro.AvroReader'] = {
    "type": "object",
    "properties": {
        "componentType": {"type": "string", "enum": ["CONTROLLER_SERVICE"]},
        "properties": {
            "type": "object",
            "properties": {
                "schema-name": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "schema-text": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"}
            },
            "required": ["schema-name", "schema-text"]
        },
    },
    "required": ["componentType"],
}

# org.apache.nifi.ssl.StandardSSLContextService
nifiValidationShcemas['org.apache.nifi.ssl.StandardSSLContextService'] = {
    "type": "object",
    "properties": {
        "componentType": {"type": "string", "enum": ["CONTROLLER_SERVICE"]},
        "properties": {
            "type": "object",
            "properties": {
                "Truststore Type": {"type": "string", "enum": ['JKS']},
                "SSL Protocol": {"type": "string", "enum": ['TLSv1.2']},
                "Truststore Filename": {"type": "string", "enum": ['/etc/nifi/current/adeo_empty_pass.jks']},
            },
            "required": ["Truststore Type", "SSL Protocol", "Truststore Filename"]
        },
    },
    "required": ["componentType"],
} 

# org.apache.nifi.json.JsonRecordSetWriter
nifiValidationShcemas['org.apache.nifi.json.JsonRecordSetWriter'] = {
    "type": "object",
    "properties": {
        "componentType": {"type": "string", "enum": ["CONTROLLER_SERVICE"]},
        "properties": {
            "type": "object",
            "properties": {
                "schema-name": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "schema-text": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
            },
            "required": ["schema-name", "schema-text"]
        },
    },
    "required": ["componentType"],
}

# org.apache.nifi.dbcp.DBCPConnectionPool
nifiValidationShcemas['org.apache.nifi.dbcp.DBCPConnectionPool'] = {
    "type": "object",
    "properties": {
        "componentType": {"type": "string", "enum": ["CONTROLLER_SERVICE"]},
        "properties": {
            "type": "object",
            "properties": {
                "database-driver-locations": {"type": "string", "enum": ['/etc/nifi/current/lib/']},
                "Database Driver Class Name": {"type": "string"}
            },
            "required": ["Database Driver Class Name", "database-driver-locations"],
            "if": {
                "properties": {"Database Driver Class Name": {"const": "org.postgresql.Driver"}}
            },
            "then": {
                "properties": {"Database Connection URL": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"}}
            }
        },
    },
    "required": ["componentType"]
}

#  ############### Processors###################
# All ENABLED
nifiValidationShcemas['processors_enabled'] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]}
    },
    "required": ["scheduledState"]
}

# org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_0
nifiValidationShcemas['org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_0'] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "concurrentlySchedulableTaskCount": {"type": "integer", "enum": [1]},
        "properties": {
            "type": "object",
            "properties": {
                "sasl.username": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "group.id": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "bootstrap.servers": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "topic": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "auto.offset.reset": {"type": "string", "enum": ['earliest']},
                "ssl.context.service": {
                    "type": "string",
                    "pattern": '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                    "minLength": 36,
                    "maxLength": 36},
                "record-reader": {
                    "type": "string",
                    "pattern": '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                    "minLength": 36,
                    "maxLength": 36},
                "record-writer": {
                    "type": "string",
                    "pattern": '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                    "minLength": 36,
                    "maxLength": 36}
            },
            "required": [
                "sasl.username", "group.id", "topic", "auto.offset.reset", "ssl.context.service",
                "record-reader", "record-writer"
                ]
        },
    },
    "required": ["scheduledState", "concurrentlySchedulableTaskCount"] 
}
# org.apache.nifi.processors.standard.PutSQL
nifiValidationShcemas["org.apache.nifi.processors.standard.PutSQL"] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "properties": {
            "type": "object",
            "properties": {
                "JDBC Connection Pool": {
                    "type": "string",
                    "pattern": '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                    "minLength": 36,
                    "maxLength": 36}
            },
            "required": ["JDBC Connection Pool"]
        },
    },
    "required": ["scheduledState"]
}

# org.apache.nifi.processors.script.ExecuteScript

nifiValidationShcemas["org.apache.nifi.processors.script.ExecuteScript"] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "properties": {
            "type": "object",
            "properties": {
                "Script File": {"type": "string", "pattern": "^/etc/nifi/current/scripts/*"},
                "Module Directory": {"type": "string", "pattern": "^/etc/nifi/current/lib/*"},
                "Script Body": {"type": "null"}
            },
            "required": ["Script File", "Module Directory"]
        },
    },
    "required": ["scheduledState"] 
}

# org.apache.nifi.processors.standard.MergeContent
nifiValidationShcemas["org.apache.nifi.processors.standard.MergeContent"] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "properties": {
            "type": "object",
            "properties": {
            },
        },
    },
    "required": ["scheduledState"]
}

# org.apache.nifi.processors.aws.s3.PutS3Object
nifiValidationShcemas["org.apache.nifi.processors.aws.s3.PutS3Object"] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "properties": {
            "type": "object",
            "properties": {
                "FullControl User List": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Owner": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Access Key": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Endpoint Override URL": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "canned-acl": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Secret Key": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Write ACL User List": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Read ACL User List": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Object Key": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Bucket": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Write Permission User List": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
                "Read Permission User List": {"type": "string", "pattern": "(\${.+?})|(\#{.+?})"},
            },
            "required": [
                "FullControl User List",
                "Owner", "Access Key", "Endpoint Override URL",
                "canned-acl", "Secret Key", "Write ACL User List", "Read ACL User List",
                "Object Key", "Bucket", "Write Permission User List", "Read Permission User List"
            ]
        },
    },
    "required": ["scheduledState"]
}
# Individual validation
nifiValidationShcemas["MergeContent_before_Put"] = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "properties": {
            "type": "object",
            "properties": {
                "Max Bin Age": {"type": "string", "pattern": "^([3-9]\d\d *sec)|^(\d+\d{3} *sec)|(\${.+?})|(\#{.+?})"},
                "Minimum Number of Entries": {"type": "string", "pattern": "(^[5-9]\d\d\d$)|(^[1-9]\d{3}\d+$)|(\${.+?})|(\#{.+?})"},
            },
            "required": ["Max Bin Age", "Minimum Number of Entries"]
        },
    },
    "required": ["scheduledState"]
}

# ###########variables
nifiValidationShcemas['variables'] = {
        "type": "object",
        "properties": {
            "kafka.schema.registry": {"type": "string"},
            "kafka.hosts": {"type": "string"},
            "gp.server.port": {"type": "string"},
            "gp.server.database": {"type": "string"},
            "kafka.topic": {"type": "string"},
            "gp.schema": {"type": "string"},
            "kafka.username": {"type": "string"},
            "gp.server.name": {"type": "string"},
            "kafka.group": {"type": "string"}
        },
        "required": [
            "kafka.schema.registry", "kafka.hosts",
            "gp.server.port", "gp.server.database", "kafka.topic",
            "gp.schema", "kafka.username", "gp.server.name",
            "kafka.group"
        ],
}

# ###########versionControlInformation Объект должен быть сахранен в объектное хранение
nifiValidationShcemas['versionControlInformation'] = {
    "type": "object",
    "properties": {
        "state": {"type": "string", "enum": ["UP_TO_DATE"]}
    },
    "required": ["state"],
}

for (key, value) in nifiValidationShcemas.items():
    try:
        Draft201909Validator(value)
    except exceptions.SchemaError as ve:
        print(f"key={ve} Error")
        exit(1)
