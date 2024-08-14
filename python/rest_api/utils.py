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
        type = str(column.type)
        type = type[type.index("(")+1:type.index(")")]
        table_dict["columns"].append({"name": column.name, "type": type, "nullable": column.nullable,
                                     "primary_key": column.name in table.schema.primary_keys()})
    return table_dict


def column_names(schema):
    column_names = []
    for i in range(len(schema)):
        column = schema.at(i)
        column_names.append(column.name)
    return column_names
