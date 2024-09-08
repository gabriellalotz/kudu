def table_to_dict(table):
    # compression, encoding, non_unique_primary_key, blocksize, default
    table_dict = {"name": table.name, "table_schema": {"columns": []}, "comment": table.comment}
    for i in range(len(table.schema)):
        column = table.schema.at(i)
        type = str(column.type)
        type = type[type.index("(")+1:type.index(")")]
        table_dict["table_schema"]["columns"].append({"name": column.name, "type": type,
                                      "nullable": column.nullable, "primary_key": column.name in
                                      table.schema.primary_keys(), "precision": column.type_attributes.precision,
                                      "scale": column.type_attributes.scale, "length": column.type_attributes.length,
                                      "comment": column.comment})
    return table_dict


def column_names(schema):
    column_names = []
    for i in range(len(schema)):
        column = schema.at(i)
        column_names.append(column.name)
    return column_names
