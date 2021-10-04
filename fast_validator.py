from typing_extensions import Required
import fastjsonschema
from typing_extensions import Required


schema = {
    "type" : "object",
    "properties" : {
        "price" : {"type" : "number"},
        "name" : {"type" : "string"},
        "email": {"type": "string"},
    },
    "required": ["price", "name", "email"]

}
validate = fastjsonschema.compile(schema)
data = \
[
    { "name": "Apples"},
    { "name": "Bananas", "price": 20},
    { "name": "Cherries", "price": "thirty"},
    { "name": 40, "price": 40},
    { "name": 50, "price": "fifty"}
]

for idx, item in enumerate(data):
    try:
        validate(item)
        print("Record #{}: OK\n".format(idx))
    except fastjsonschema.exceptions.JsonSchemaValueException as t:
        print(f"Record #{idx} : ERROR\n")
 