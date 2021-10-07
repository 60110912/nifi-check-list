
import json
from jsonschema import validate, Draft201909Validator
from validation_shcema import nifiValidationShcemas


# Тест
testSchema = 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_0'
with open(f'unit_test/{testSchema}.json') as json_file:
    data = json_file.read()
jsonobj = json.loads(data)

schema = {
    "type": "object",
    "properties": {
        "scheduledState": {"type": "string", "enum": ["ENABLED"]},
        "concurrentlySchedulableTaskCount": {"type": "integer", "enum": [1]},
        "properties": {
            "type": "object",
            "properties": {
                "sasl.username": {"type": "string", "pattern": "(\${.*?})|(\#{.*?})"},
                "group.id": {"type": "string", "pattern": "(\${.*?})|(\#{.*?})"},
                "bootstrap.servers": {"type": "string", "pattern": "(\${.*?})|(\#{.*?})"},
                "topic": {"type": "string", "pattern": "(\${.*?})|(\#{.*?})"},
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
v = Draft201909Validator(schema)
validate(jsonobj, schema)

print('validate from schema')
validate(jsonobj, nifiValidationShcemas[testSchema])
