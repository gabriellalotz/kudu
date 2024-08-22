import uvicorn
import kudu
import argparse
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from utils import schema_to_dict, column_names
from kudu.client import Partitioning
from fastapi.exceptions import HTTPException
from schemas import Schema, HashPartition, RangePartitionColumns, RangePartition, TablesResponse


app = FastAPI(root_path='/api/v1')
parser = argparse.ArgumentParser(description='REST API for Kudu.')
parser.add_argument('--masters', '-m', nargs='+', default='localhost',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('--ports', '-p', nargs='+', default='7051',
                    help='The master server port(s) to connect to Kudu.')
args = parser.parse_args()

client = kudu.connect(host=args.masters, port=args.ports)

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


@app.post('/tables', status_code=201)
def post_table(name: str, table_schema: Schema, hash_partitioning: HashPartition, range_partition_columns: RangePartitionColumns, range_partitioning: RangePartition):
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
            if column.comment:
                builder.comment(column.comment)
    try:
        schema = builder.build()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    partitioning = Partitioning()
    if len(hash_partitioning.column_names) > 0:
        partitioning.add_hash_partitions(
            column_names=hash_partitioning.column_names, num_buckets=hash_partitioning.num_buckets)
    if len(range_partition_columns.columns) > 0:
        partitioning.set_range_partition_columns(
            range_partition_columns.columns)
        partitioning.add_range_partition(lower_bound=range_partitioning.lower_bound, upper_bound=range_partitioning.upper_bound,
                                         lower_bound_type=range_partitioning.lower_bound_type, upper_bound_type=range_partitioning.upper_bound_type)

    if client.table_exists(name):
        raise HTTPException(status_code=409, detail='Table already exists.')
    try:
        client.create_table(name, schema, partitioning)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    table = client.table(name)
    return schema_to_dict(table)

# DELETE api/v1/tables/{table} - drop table


@app.delete('/tables/{table}', status_code=204)
def delete_table(table: str):
    if not client.table_exists(table):
        raise HTTPException(status_code=404, detail='Table does not exist.')
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
                # name, type, nullable, compression, encoding, default
                try:
                    alterer.add_column(column.name).type(column.type).nullable(
                    column.nullable)
                except Exception as e:
                    raise HTTPException(status_code=400, detail=str(e))
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
