from pydantic import BaseModel
from pydantic.fields import Field
from typing import List, Union, Optional


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
    hash_partitioning: Optional['HashPartition'] = Field(default=None)
    range_partition_columns: Optional['RangePartitionColumns'] = Field(default=None)
    range_partitioning: Optional['RangePartition'] = Field(default=None)


class HashPartition(BaseModel):
    column_names: List[str] = Field(default=[])
    num_buckets: int = Field(default=0)
    seed: str = Field(default='None')


class RangePartitionColumns(BaseModel):
    columns: List[str] = Field(default=[])
    

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