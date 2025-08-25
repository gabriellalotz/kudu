---
layout: post
title: Introducing REST API to Apache Kudu
author: Gabriella Lotz
---

Apache Kudu has long provided powerful client APIs in C++, Java, and Python for building high-performance applications. Today, we're excited to announce the introduction of a REST API that makes Kudu even more accessible to developers and integration tools.

<!--more-->

## Overview

The REST API in Apache Kudu provides a comprehensive HTTP interface for table metadata management operations. Built on top of Kudu's existing catalog management system, the API follows RESTful principles and returns JSON responses. This makes it easy to integrate Kudu with a wide range of applications and tools that can communicate over HTTP.

## Key Features

* **Table Metadata Management**: Create, read, update, and delete tables through HTTP endpoints
* **JSON-based**: All requests and responses use JSON format for easy parsing
* **RESTful Design**: Follows standard HTTP methods and status codes
* **Kerberos/SPNEGO Authentication**: Integrates with Kudu's existing authentication mechanisms
* **Leader-only Operations**: In multi-master setups, only the leader responds to requests
* **Leader Discovery**: Built-in endpoint to discover the current cluster leader

## API Overview

The REST API is available at `/api/v1/` and provides endpoints for table metadata management. The API supports standard HTTP methods (GET, POST, PUT, DELETE) and returns JSON responses.

### Available Endpoints

- **GET** `/api/v1/tables` - List all tables
- **POST** `/api/v1/tables` - Create a new table
- **GET** `/api/v1/tables/<table_id>` - Get table details
- **PUT** `/api/v1/tables/<table_id>` - Update table metadata
- **DELETE** `/api/v1/tables/<table_id>` - Delete a table
- **GET** `/api/v1/leader` - Discover the current leader master

### Example Responses

**List Tables:**
```json
{
  "tables": [
    {
      "table_id": "1234567890abcdef1234567890abcdef",
      "table_name": "my_table"
    }
  ]
}
```

**Table Details:**
```json
{
  "name": "example_table",
  "id": "a2810622a25b4a3e8ce0be3ece103c50",
  "schema": {
    "columns": [
      {
        "id": 10,
        "name": "column_name",
        "type": "INT8",
        "is_key": true,
        "is_nullable": false,
        "encoding": "AUTO_ENCODING",
        "compression": "DEFAULT_COMPRESSION",
        "cfile_block_size": 0,
        "immutable": false
      }
    ]
  },
  "partition_schema": {
    "hash_schema": [
      {
        "columns": [
          {
            "id": 10
          }
        ],
        "num_buckets": 2,
        "seed": 0
      }
    ],
    "range_schema": {
      "columns": [
        {
          "id": 10
        }
      ]
    }
  },
  "owner": "default",
  "comment": "",
  "extra_config": {}
}
```

For complete API documentation and interactive examples, check out our Swagger UI:
![png]({{ site.github.url }}/img/swagger.png){: .img-responsive}

## Important Notes

### Authentication
The REST API supports Kerberos/SPNEGO authentication, integrating seamlessly with Kudu's existing security mechanisms.

### Multi-Master Setup
In multi-master configurations, only the leader master will respond to REST API requests. Follower masters will return errors, so applications should use the leader discovery endpoint to find the correct master to connect to.

### Metadata Only
This REST API focuses exclusively on table metadata management. Data read/write operations are not supported through this interface and should continue to use Kudu's native client APIs for optimal performance.

## Configuration

The REST API must be explicitly enabled using the `--enable_rest_api` flag on Kudu masters.
