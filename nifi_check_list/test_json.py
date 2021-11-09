
import json
from jsonschema import validate, Draft201909Validator
# from validation_shcema import nifiValidationSchemas


# Тест
testSchema = 'check_status'
with open(f'nifi_check_list/template/{testSchema}.json') as json_file:
    data = json_file.read()
jsonobj = json.loads(data)

schema = {
    "type": "object",
    "properties": {
        "stoppedCount": {"type": "integer", "enum": [0]},
        "invalidCount": {"type": "integer", "enum": [0]},
        "disabledCount": {"type": "integer", "enum": [0]},
    },
    "required": ["stoppedCount", "invalidCount", "disabledCount"],
}
v = Draft201909Validator(schema)
validate(jsonobj, schema)

# print('validate from schema')
# validate(jsonobj, nifiValidationSchemas[testSchema])
