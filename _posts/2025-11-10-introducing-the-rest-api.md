---
layout: post
title: Introducing REST API for metadata management to Apache Kudu
author: Gabriella Lotz
---

Apache Kudu has long provided client APIs in C++, Java, and Python for building client applications. Today, we're excited to announce the introduction of a REST API for metadata management that makes Kudu even more accessible to developers and integration tools.

<!--more-->

## Overview

The Apache Kudu REST API exposes table-metadata and administrative operations over HTTP on top of Kudu's catalog. It lets users handle these tasks with lightweight REST calls instead of building a full client application.

## Key Features

* **Table Metadata Management**: Create, read, update, and delete tables through HTTP endpoints
* **JSON-based**: All requests and responses use JSON format for easy parsing
* **RESTful Design**: Follows standard HTTP methods and status codes
* **Kerberos/SPNEGO Authentication**: Integrates with Kudu's existing authentication mechanisms

## API Overview

The REST API is available on the master's webserver at `/api/v1/` and provides endpoints for table metadata management. The API supports standard HTTP methods (GET, POST, PUT, DELETE) and returns JSON responses.

### Available Endpoints

- **GET** `/api/v1/tables` - List all tables
- **POST** `/api/v1/tables` - Create a new table
- **GET** `/api/v1/tables/<table_id>` - Get table details
- **PUT** `/api/v1/tables/<table_id>` - Update table metadata
- **DELETE** `/api/v1/tables/<table_id>` - Delete a table
- **GET** `/api/v1/leader` - Discover the current leader master

### Example Request

**GET** `/api/v1/tables/<table_id>`

```bash
curl http://master-host:8765/api/v1/tables/a2810622a25b4a3e8ce0be3ece103c50
```

**Response:**
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

For complete API documentation, the OpenAPI specification is available in the [Kudu repository](https://github.com/apache/kudu/blob/master/www/swagger/kudu-api.json). On a Kudu cluster running this feature, the interactive API documentation is accessible through the master's web UI:

![png]({{ site.github.url }}/img/swagger.png){: .img-responsive}

## Limitations

### Multi-Master Setup
In multi-master configurations, only the leader master will respond to REST API requests. Follower masters will return errors, so applications should use the `/api/v1/leader` endpoint to get the leader address and connect to the correct master.

### Metadata Only
This REST API focuses exclusively on table metadata management. Data read/write operations are not supported through this interface and should continue to use Kudu's native client APIs for optimal performance.

## Configuration

The REST API must be explicitly enabled using the `--enable_rest_api` flag on Kudu masters. The master's webserver must also be enabled (via the `--webserver_enabled` flag) since the REST API is served through the webserver.
