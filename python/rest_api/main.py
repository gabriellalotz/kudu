import kudu
from fastapi import FastAPI

# GET api/v1/tables - lists tables
# GET api/v1/tables/{table} - load table
# HEAD api/v1/tables/{table} - checks if table exists
# POST api/v1/tables - create new table
# DELETE api/v1/tables/{table} - drop table
# PUT api/v1/tables/{table} - update table
# GET api/v1/tables/{table}/partitions - list partitions
# GET api/v1/tables/{table}/tablets - list tablets

app = FastAPI(root_path="/api/v1")
client = kudu.connect(host='localhost', port=8764)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/tables")
def get_tables():
    tables = client.list_tables()
    return {"tables": tables}

@app.get("/tables/{table}")
def get_table(table: str):
    table = client.table(table)
    print(table.schema)
    table_dict = schema_to_dict(table.schema)
    print(table_dict)
    return {"table": table.schema}

def schema_to_dict(schema):
    # TODO: Implement this
    columns = []
    for column in schema.columns:
        columns.append(column)
    return columns