def schema_to_dict(schema):
    primary_key = schema.primary_keys()
    columns = []
    for i in range(len(schema)):
        columns.append(schema.at(i).name + " " + str(schema.at(i).type))
        if schema.at(i).name in primary_key:
            columns[-1] += " PRIMARY KEY"
    return columns