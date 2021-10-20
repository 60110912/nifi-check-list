
import json
from jsonschema import validate, Draft201909Validator
from validation_shcema import nifiValidationShcemas


# Тест
testSchema = 'versionControlInformation'
with open(f'nifi_check_list/template/{testSchema}.json') as json_file:
    data = json_file.read()
jsonobj = json.loads(data)

schema = {
    "type": "object",
    "properties": {
        "state": {"type": "string", "enum": ["UP_TO_DATE"]}
    },
    "required": ["state"],
}
v = Draft201909Validator(schema)
validate(jsonobj, schema)

# print('validate from schema')
# validate(jsonobj, nifiValidationShcemas[testSchema])
