# def schema_to_dict(schema):
#     primary_key = schema.primary_keys()
#     columns = []
#     for i in range(len(schema)):
#         columns.append(schema.at(i).name + " " + str(schema.at(i).type))
#         if schema.at(i).name in primary_key:
#             columns[-1] += " PRIMARY KEY"
#     return columns

def schema_to_dict(table):
    table_dict = {"columns": []}
    for i in range(len(table.schema)):
        column = table.schema.at(i)
        table_dict["columns"].append({"name": column.name, "type": str(
            column.type), "nullable": column.nullable, "primary_key": column.name in table.schema.primary_keys()})
    return table_dict