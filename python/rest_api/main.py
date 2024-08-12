import uvicorn
import kudu
from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse
from utils import schema_to_dict
from kudu.client import Partitioning
from pydantic import BaseModel
from typing import List, Union
from typing_extensions import Annotated

# Define the schemas for the API.


class Column(BaseModel):
    name: str
    type: str
    nullable: bool
    primary_key: bool


class Schema(BaseModel):
    columns: List[Column]


class Table(BaseModel):
    name: str
    table_schema: Schema


class HashPartition(BaseModel):
    column_names: List[str]
    num_buckets: int


class RangePartition(BaseModel):
    lower_bound: None
    upper_bound: None
    lower_bound_type: str
    upper_bound_type: str


app = FastAPI(root_path="/api/v1")
client = kudu.connect(host='localhost', port=8764)


@app.get("/")
def read_root():
    return RedirectResponse(url="/docs")

# GET api/v1/tables - lists tables


@app.get("/tables")
def get_tables():
    tables = client.list_tables()
    return {"tables": tables}

# GET api/v1/tables/{table} - load table


@app.get("/tables/{table}")
def get_table(table: str, details: str = Query("schema", description="schema or full", enum=["schema", "full"])):
    table = client.table(table)
    if details == "schema":
        table_dict = schema_to_dict(table.schema)
    else:
        table_dict = {"columns": []}
        for i in range(len(table.schema)):
            column = table.schema.at(i)
            table_dict["columns"].append({"name": column.name, "type": str(
                column.type), "nullable": column.nullable, "primary_key": column.name in table.schema.primary_keys()})
        # TODO: add partitions, tablets, etc.
    return {table.name: table_dict}

# POST api/v1/tables - create new table
# TODO: try out different examples, query parameters could be in different formats


@app.post("/tables")
def post_table(name: str, table_schema: Schema, hash_partitioning: HashPartition, range_partitioning: RangePartition):
    # Define a schema for a new table.
    builder = kudu.schema_builder()
    for column in table_schema.columns:
        if column.primary_key:
            builder.add_column(column.name).type(
                column.type).nullable(column.nullable).primary_key()
        else:
            builder.add_column(column.name).type(
                column.type).nullable(column.nullable)
    schema = builder.build()
    # Define the partitioning schema.
    partitioning = Partitioning()
    if hash_partitioning:
        partitioning.add_hash_partitions(
            column_names=hash_partitioning.column_names, num_buckets=hash_partitioning.num_buckets)
    if range_partitioning:
        partitioning.add_range_partition(lower_bound=range_partitioning.lower_bound, upper_bound=range_partitioning.upper_bound,
                                         lower_bound_type=range_partitioning.lower_bound_type, upper_bound_type=range_partitioning.upper_bound_type)
    # TODO: Throw error if table already exists.
    # Create a new table.
    client.create_table(name, schema, partitioning)
    table = client.table(name)
    # TODO: talk to the team about the return value
    return {table.name: schema_to_dict(table.schema)}

# DELETE api/v1/tables/{table} - drop table


@app.delete("/tables/{table}")
def delete_table(table: str):
    client.delete_table(table)
    # should return 204 with empty body

# PUT api/v1/tables/{table} - update table


@app.put("/tables/{table}")
def put_table(table: str, table_schema: Schema):
    table = client.table(table)
    if len(table.schema) != len(table_schema.columns):
        # add new column
        pass
    # partition check
    return {"table": table}
    

# GET api/v1/tables/{table}/partitions - list partitions
# GET api/v1/tables/{table}/tablets - list tablets


if __name__ == "__main__":
    uvicorn.run("main:app", reload=True)
