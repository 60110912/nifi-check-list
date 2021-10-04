import json

from jsonpath_ng import jsonpath, parse

json_string = '{"test":[{"id":1, "name":"Pankaj"}, {"id":2, "name":"Vasiliy"}, {"i":6}]}'
json_data = json.loads(json_string)

jsonpath_expression = parse('$..id')

match = jsonpath_expression.find(json_data)
print(type(json_data))
#print(match)
for id in match:
    print(id.context)