# jsonpath_lagou.py

from os import name
import re
import jsonpath
import json
import jsonschema
from jsonschema import validate, Draft201909Validator, Draft7Validator
from validation_shcema import nifiValidationShcemas




 # Начиная с корневого узла, сопоставьте имя узла
#print(jsonobj)
def allProcessorsIsEnables(jsonobj):
    processors = jsonpath.jsonpath(jsonobj,'$..processors.*')
    for idx, item in enumerate(processors):
        try:
            validate(item, nifiValidationShcemas['processors_enabled'])
            print("Record #{}: OK\n".format(item['name']))
        except jsonschema.exceptions.ValidationError as ve:
            print("Record #{}: ERROR\n".format(item['name']))

def allProcessorValidName(jsonobj):
    name_pattern = jsonpath.jsonpath(jsonobj, '$.flowContents.name')[0]
    name_pattern = '^' + name_pattern 
    processors = jsonpath.jsonpath(jsonobj,'$..processors.*')
    for item in processors:
        if re.match(name_pattern, item['name']):
            print("Record #{}: OK\n".format(item['name']))
        else:
            print("Record #{}: ERROR\n".format(item['name']))


#if __name__ == "__main__":
# Преобразовать строку формата json в объект python
    # with open('outlook_exchange.json') as json_file:
    #     data=json_file.read()
    # jsonobj = json.loads(data)
    # allProcessorsIsEnables(jsonobj)
    # allProcessorValidName(jsonobj)

#Тест
with open('unit_test/org.apache.nifi.processors.standard.PutSQL.json') as json_file:
    data=json_file.read()
jsonobj = json.loads(data)

schema = {
   "type": "object",
        "properties" : {
            "scheduledState":{"type": "string", "enum": ["ENABLED"]},
            "properties":{
                "type" :"object",
                "properties" : {
                    "JDBC Connection Pool":{"type": "string", 
                            "pattern": '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                            "minLength": 36,
                            "maxLength": 36}
                },
                "required": ["JDBC Connection Pool"]
            },
        },
        "required": ["scheduledState"] 
}

v = Draft201909Validator(schema)
validate(jsonobj, schema)


