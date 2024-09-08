import uvicorn
import kudu
import argparse
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from utils import table_to_dict, column_names
from kudu.client import Partitioning
from fastapi.exceptions import HTTPException
from schemas import TablesResponse, Table


app = FastAPI(root_path='/api/v1')

parser = argparse.ArgumentParser(description='REST API for Kudu.')
parser.add_argument('testing', nargs='?', default=None)
parser.add_argument('--masters', '-m', nargs='+', default='localhost',
                    help='The master address(es) to connect to Kudu.')
parser.add_argument('--ports', '-p', nargs='+', default='7051',
                    help='The master server port(s) to connect to Kudu.')
args = parser.parse_args()

app.client = None
if args.testing is None:
    app.client = kudu.connect(host=args.masters, port=args.ports)


@app.get('/')
def read_root():
    return RedirectResponse(url='/docs')

# GET api/v1/tables - lists tables


@app.get('/tables')
def get_tables() -> TablesResponse:
    tables = app.client.list_tables()
    return {'tables': tables}

# GET api/v1/tables/{table} - load table


@app.get('/tables/{name}')
def get_table(name: str) -> Table:
    if not app.client.table_exists(name):
        raise HTTPException(status_code=404, detail='Table does not exist.')
    name = app.client.table(name)
    table_dict = table_to_dict(name)
    return table_dict

# POST api/v1/tables - create new table


@app.post('/tables', status_code=201)
def post_table(table: Table) -> Table:
    builder = kudu.schema_builder()
    for column in table.table_schema.columns:
        (builder.add_column(column.name,
                            type_=column.type,
                            nullable=column.nullable,
                            primary_key=column.primary_key,
                            precision=column.precision or None,
                            scale=column.scale or None,
                            length=column.length or None,
                            comment=column.comment))
    try:
        schema = builder.build()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    partitioning = Partitioning()
    if len(table.hash_partitioning.column_names) > 0:
        partitioning.add_hash_partitions(
            column_names=table.hash_partitioning.column_names, num_buckets=table.hash_partitioning.num_buckets)
    if len(table.range_partition_columns.columns) > 0:
        partitioning.set_range_partition_columns(
            table.range_partition_columns.columns)
        partitioning.add_range_partition(lower_bound=table.range_partitioning.lower_bound,
                                         upper_bound=table.range_partitioning.upper_bound,
                                         lower_bound_type=table.range_partitioning.lower_bound_type,
                                         upper_bound_type=table.range_partitioning.upper_bound_type)

    if app.client.table_exists(table.name):
        raise HTTPException(status_code=409, detail='Table already exists.')
    try:
        app.client.create_table(table.name, schema, partitioning, comment=table.comment)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    table = app.client.table(table.name)
    return table_to_dict(table)

# DELETE api/v1/tables/{table} - drop table


@app.delete('/tables/{name}', status_code=204)
def delete_table(name: str):
    if not app.client.table_exists(name):
        raise HTTPException(status_code=404, detail='Table does not exist.')
    app.client.delete_table(name)
    return

# PUT api/v1/tables/{table} - update table


@app.put('/tables/{name}')
def put_table(name: str, table: Table) -> Table:
    original_table = app.client.table(name)

    if len(column_names(original_table.schema)) < len(table.table_schema.columns):
        alterer = app.client.new_table_alterer(original_table)
        for column in table.table_schema.columns:
            if column.name not in column_names(original_table.schema):
                # name, type, nullable, compression, encoding, default
                try:
                    alterer.add_column(column.name).type(column.type).nullable(
                        column.nullable)
                except Exception as e:
                    raise HTTPException(status_code=400, detail=str(e))
                original_table = alterer.alter()

    elif len(column_names(original_table.schema)) > len(table.table_schema.columns):
        alterer = app.client.new_table_alterer(original_table)
        for column_name in column_names(original_table.schema):
            if column_name not in [column.name for column in table.table_schema.columns]:
                alterer.drop_column(column_name)
                original_table = alterer.alter()

    else:
        alterer = app.client.new_table_alterer(original_table)
        for i, column in enumerate(table.table_schema.columns):
            if column.name not in column_names(original_table.schema):
                alterer.alter_column(original_table.schema.at(
                    i).name, rename_to=column.name)
                original_table = alterer.alter()
    # partition check

    if name != table.name:
        if app.client.table_exists(table.name):
            raise HTTPException(
                status_code=409, detail='Table already exists.')
        alterer = app.client.new_table_alterer(original_table)
        original_table = alterer.rename(table.name).alter()

    return table_to_dict(original_table)

# GET api/v1/tabletServers - list tablet servers


@app.get('/tabletServers')
def get_tablet_servers():
    tablet_servers = app.client.list_tablet_servers()
    tablet_servers = [ts.uuid() for ts in tablet_servers]
    return {'tabletServers': tablet_servers}


def main():
    uvicorn.run('main:app', reload=True)


if __name__ == '__main__':
    main()
