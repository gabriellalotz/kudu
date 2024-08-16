import uvicorn
import kudu
from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse
from utils import schema_to_dict, column_names
from kudu.client import Partitioning
from pydantic import BaseModel
from pydantic.fields import Field
from typing import List, Union, Optional
from typing_extensions import Annotated
from fastapi.exceptions import HTTPException

# Define the schemas for the API.


class Column(BaseModel):
    name: str
    type: str = Field(default='', description='The type of the column')
    # originally bool but default is None
    nullable: Optional[bool] = Field(default=None)
    # compression: str = Field(default='None', 
    #   description='One of {'default', 'none', 'snappy', 'lz4', 'zlib'}')
    # encoding: str = Field(default='None', 
    #   description='One of {'auto', 'plain', 'prefix', 'bitshuffle', 'rle', 'dict'}')
    primary_key: bool = Field(default=False)
    # non_unique_primary_key: bool = Field(default=False)
    # block_size: int = Field(default='None')
    # default: object = Field(default='None')
    # The precision must be between 1 and 38.
    precision: Optional[int] = Field(default=None)
    scale: Optional[int] = Field(default=None)
    # The length must be between 1 and 65,535 (inclusive).
    length: Optional[int] = Field(default=None)
    comment: str = Field(default='')


class Schema(BaseModel):
    columns: List[Column]


class Table(BaseModel):
    name: str
    table_schema: Schema


class HashPartition(BaseModel):
    column_names: List[str] = Field()
    num_buckets: int = Field(default=0)
    seed: str = Field(default='None')


class RangePartition(BaseModel):
    # PartialRow/list/tuple/dict
    lower_bound: dict = Field()
    # PartialRow/list/tuple/dict
    upper_bound: dict = Field()
    # {'inclusive', 'exclusive'} or constants
    lower_bound_type: str = Field(default='inclusive')
    # {'inclusive', 'exclusive'} or constants
    upper_bound_type: str = Field(default='exclusive')


class RangePartitionSplit(BaseModel):
    split_row: List[Union[int, float, str]]


class TablesResponse(BaseModel):
    tables: List[str]


app = FastAPI(root_path='/api/v1')
client = kudu.connect(host='localhost', port=8764)


@app.get('/')
def read_root():
    return RedirectResponse(url='/docs')

# GET api/v1/tables - lists tables


@app.get('/tables')
def get_tables() -> TablesResponse:
    tables = client.list_tables()
    return {'tables': tables}

# GET api/v1/tables/{table} - load table


@app.get('/tables/{table}')
def get_table(table: str):
    table = client.table(table)
    table_dict = schema_to_dict(table)
    return table_dict

# POST api/v1/tables - create new table
# TODO: try out different examples, query parameters could be in different formats


@app.post('/tables', status_code=201)
def post_table(name: str, table_schema: Schema, hash_partitioning: HashPartition, range_partitioning: RangePartition):
    builder = kudu.schema_builder()
    for column in table_schema.columns:
        if column.primary_key:
            (builder
             .add_column(column.name)
             .type(column.type)
             .nullable(column.nullable)
             .primary_key())
        else:
            (builder
             .add_column(column.name)
             .type(column.type)
             .nullable(column.nullable))
        if column.precision:
            builder.precision(column.precision)
        if column.scale:
            builder.scale(column.scale)
        if column.length:
            builder.length(column.length)

    schema = builder.build()

    partitioning = Partitioning()
    if hash_partitioning:
        partitioning.add_hash_partitions(
            column_names=hash_partitioning.column_names, num_buckets=hash_partitioning.num_buckets)
    # if range_partitioning:
    #     partitioning.add_range_partition(lower_bound=range_partitioning.lower_bound, upper_bound=range_partitioning.upper_bound,
    #                                      lower_bound_type=range_partitioning.lower_bound_type, upper_bound_type=range_partitioning.upper_bound_type)

    if client.table_exists(name):
        raise HTTPException(status_code=409, detail='Table already exists.')
    client.create_table(name, schema, partitioning)
    table = client.table(name)
    return schema_to_dict(table)

# DELETE api/v1/tables/{table} - drop table


@app.delete('/tables/{table}', status_code=204)
def delete_table(table: str):
    client.delete_table(table)
    return

# PUT api/v1/tables/{table} - update table


@app.put('/tables/{table}')
def put_table(table: str, table_schema: Schema = None):
    table = client.table(table)

    if len(column_names(table.schema)) < len(table_schema.columns):
        alterer = client.new_table_alterer(table)
        for column in table_schema.columns:
            if column.name not in column_names(table.schema):
                # name, type, nullable, complession, encoding, default
                alterer.add_column(column.name).type(column.type).nullable(
                    column.nullable)
                table = alterer.alter()

    elif len(column_names(table.schema)) > len(table_schema.columns):
        alterer = client.new_table_alterer(table)
        for column_name in column_names(table.schema):
            if column_name not in [column.name for column in table_schema.columns]:
                alterer.drop_column(column_name)
                table = alterer.alter()

    else:
        alterer = client.new_table_alterer(table)
        for i, column in enumerate(table_schema.columns):
            if column.name not in column_names(table.schema):
                alterer.alter_column(table.schema.at(
                    i).name, rename_to=column.name)
                table = alterer.alter()
    # partition check
    return schema_to_dict(table)

# PUT api/v1/tables/{table}/rename - rename table


@app.put('/tables/{table}/rename')
def put_table_rename(table: str, new_table_name: str):
    table = client.table(table)
    if client.table_exists(new_table_name):
        raise HTTPException(status_code=409, detail='Table already exists.')
    alterer = client.new_table_alterer(table)
    table = alterer.rename(new_table_name).alter()
    return schema_to_dict(table)

# GET api/v1/tables/{table}/partitions - list partitions
# GET api/v1/tables/{table}/tablets - list tablets


if __name__ == '__main__':
    uvicorn.run('main:app', reload=True)
