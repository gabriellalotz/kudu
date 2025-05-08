#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Automatic Swagger/OpenAPI generator for Kudu REST API.

This script parses the REST API handler source files and generates
OpenAPI 3.0 documentation automatically.
"""

import argparse
import json
import re
import sys
from typing import Dict, List, Set, Tuple, Optional

class EndpointInfo:
    """Information about a REST API endpoint."""
    def __init__(self, path: str, methods: Set[str], handler_method: str):
        self.path = path
        self.methods = methods
        self.handler_method = handler_method
        self.description = ""
        self.parameters = []
        self.responses = {}

class SwaggerGenerator:
    """Generates OpenAPI documentation from C++ REST handler source code."""
    
    def __init__(self):
        self.endpoints = []
        self.base_info = {
            "openapi": "3.0.3",
            "info": {
                "title": "Kudu REST API",
                "description": "REST API for Kudu cluster management and table operations",
                "version": "1.0.0",
                "contact": {
                    "name": "Apache Kudu",
                    "url": "https://kudu.apache.org"
                },
                "license": {
                    "name": "Apache 2.0",
                    "url": "https://www.apache.org/licenses/LICENSE-2.0"
                }
            },
            "servers": [
                {
                    "url": "/api/v1",
                    "description": "Kudu REST API v1"
                }
            ]
        }

    def parse_source_file(self, filepath: str) -> None:
        """Parse the C++ source file to extract endpoint information."""
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Extract endpoint registrations from the Register() method
        self._extract_endpoint_registrations(content)
        
        # Extract HTTP method mappings from dispatcher methods
        self._extract_http_method_mappings(content)
        
        # Extract documentation from method implementations
        self._extract_method_documentation(content)

    def _extract_endpoint_registrations(self, content: str) -> None:
        """Extract endpoint registrations from RegisterPrerenderedPathHandler calls."""
        # Pattern to match RegisterPrerenderedPathHandler calls
        pattern = r'server->RegisterPrerenderedPathHandler\(\s*"([^"]+)",\s*"[^"]*",\s*\[this\]\([^)]+\)\s*{\s*this->(\w+)\([^)]+\);\s*}'
        
        matches = re.findall(pattern, content, re.MULTILINE | re.DOTALL)
        
        for path, handler_method in matches:
            # Skip non-API paths
            if not path.startswith('/api/v1/'):
                continue
                
            endpoint = EndpointInfo(path, set(), handler_method)
            self.endpoints.append(endpoint)
            print(f"Found endpoint: {path} -> {handler_method}")

    def _extract_http_method_mappings(self, content: str) -> None:
        """Extract HTTP method to handler mappings from dispatcher methods."""
        for endpoint in self.endpoints:
            # Find the dispatcher method implementation
            method_pattern = rf'void\s+RestCatalogPathHandlers::{endpoint.handler_method}\s*\([^{{]+\{{'
            match = re.search(method_pattern, content)
            
            if not match:
                continue
                
            # Find the method body
            start = match.end()
            brace_count = 1
            pos = start
            
            while pos < len(content) and brace_count > 0:
                if content[pos] == '{':
                    brace_count += 1
                elif content[pos] == '}':
                    brace_count -= 1
                pos += 1
            
            method_body = content[start:pos-1]
            
            # Extract HTTP method checks
            http_method_pattern = r'if\s*\(\s*req\.request_method\s*==\s*"(\w+)"\s*\)\s*{\s*(\w+)\('
            method_matches = re.findall(http_method_pattern, method_body)
            
            for http_method, handler in method_matches:
                endpoint.methods.add(http_method)
                print(f"  {http_method} -> {handler}")
            
            # Special handling for endpoints that only support one method
            if not method_matches and 'req.request_method !=' in method_body:
                # Check for single-method endpoints (like leader endpoint)
                if 'req.request_method != "GET"' in method_body:
                    endpoint.methods.add('GET')
                    print(f"  GET -> {endpoint.handler_method} (single method)")
                elif endpoint.handler_method == 'HandleLeaderEndpoint':
                    # Leader endpoint only supports GET
                    endpoint.methods.add('GET')
                    print(f"  GET -> {endpoint.handler_method} (leader endpoint)")

    def _extract_method_documentation(self, content: str) -> None:
        """Extract documentation from method implementations and comments."""
        for endpoint in self.endpoints:
            # Generate basic descriptions based on method names and paths
            endpoint.description = self._generate_description(endpoint.path, endpoint.methods)
            endpoint.parameters = self._extract_path_parameters(endpoint.path)
            endpoint.responses = self._generate_standard_responses()

    def _generate_description(self, path: str, methods: Set[str]) -> str:
        """Generate a description based on the endpoint path and methods."""
        if '/tables/' in path and '<table_id>' in path:
            return "Operations on specific tables"
        elif path.endswith('/tables'):
            return "Operations on the table collection"
        elif '/leader' in path:
            return "Get information about the current leader master"
        
        return f"Handle operations on {path}"

    def _extract_path_parameters(self, path: str) -> List[Dict]:
        """Extract path parameters from the URL pattern."""
        parameters = []
        
        # Find path parameters in angle brackets
        param_pattern = r'<(\w+)>'
        matches = re.findall(param_pattern, path)
        
        for param_name in matches:
            param_info = {
                "name": param_name,
                "in": "path",
                "required": True,
                "description": f"The {param_name} identifier",
                "schema": {"type": "string"}
            }
            
            # Special handling for known parameters
            if param_name == "table_id":
                param_info.update({
                    "description": "The unique identifier of the table (32 characters)",
                    "schema": {
                        "type": "string",
                        "pattern": "^[a-f0-9]{32}$",
                        "minLength": 32,
                        "maxLength": 32
                    }
                })
            
            parameters.append(param_info)
        
        return parameters

    def _generate_standard_responses(self) -> Dict:
        """Generate standard HTTP responses for REST endpoints."""
        return {
            "200": {
                "description": "Successful operation",
                "content": {
                    "application/json": {
                        "schema": {"type": "object"}
                    }
                }
            },
            "400": {
                "description": "Bad request - invalid parameters",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            },
            "404": {
                "description": "Resource not found",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            },
            "500": {
                "description": "Internal server error",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            },
            "503": {
                "description": "Service unavailable - master not ready or not leader",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            }
        }

    def generate_openapi_spec(self) -> Dict:
        """Generate the complete OpenAPI specification."""
        spec = self.base_info.copy()
        spec["paths"] = {}
        
        for endpoint in self.endpoints:
            # Convert Kudu path format to OpenAPI format
            openapi_path = self._convert_path_format(endpoint.path)
            
            if openapi_path not in spec["paths"]:
                spec["paths"][openapi_path] = {}
            
            for method in endpoint.methods:
                method_spec = {
                    "summary": self._generate_summary(method, endpoint.path),
                    "description": endpoint.description,
                    "operationId": self._generate_operation_id(method, endpoint.path),
                    "responses": endpoint.responses
                }
                
                if endpoint.parameters:
                    method_spec["parameters"] = endpoint.parameters
                
                # Add request body for POST and PUT methods
                if method in ['POST', 'PUT']:
                    method_spec["requestBody"] = self._generate_request_body(endpoint.path)
                
                spec["paths"][openapi_path][method.lower()] = method_spec
        
        # Add component schemas
        spec["components"] = self._generate_components()
        
        return spec

    def _convert_path_format(self, path: str) -> str:
        """Convert Kudu path format to OpenAPI format."""
        # Convert <param> to {param}
        return re.sub(r'<(\w+)>', r'{\1}', path)

    def _generate_summary(self, method: str, path: str) -> str:
        """Generate a summary for the operation."""
        summaries = {
            ('GET', '/api/v1/tables'): "List all tables",
            ('POST', '/api/v1/tables'): "Create a new table", 
            ('GET', '/api/v1/tables/{table_id}'): "Get table details",
            ('PUT', '/api/v1/tables/{table_id}'): "Update table",
            ('DELETE', '/api/v1/tables/{table_id}'): "Delete table",
            ('GET', '/api/v1/leader'): "Get leader master information"
        }
        
        openapi_path = self._convert_path_format(path)
        return summaries.get((method, openapi_path), f"{method} {openapi_path}")

    def _generate_operation_id(self, method: str, path: str) -> str:
        """Generate an operation ID for the endpoint."""
        operation_ids = {
            ('GET', '/api/v1/tables'): "listTables",
            ('POST', '/api/v1/tables'): "createTable",
            ('GET', '/api/v1/tables/{table_id}'): "getTable", 
            ('PUT', '/api/v1/tables/{table_id}'): "updateTable",
            ('DELETE', '/api/v1/tables/{table_id}'): "deleteTable",
            ('GET', '/api/v1/leader'): "getLeader"
        }
        
        openapi_path = self._convert_path_format(path)
        return operation_ids.get((method, openapi_path), f"{method.lower()}{path.replace('/', '_')}")

    def _generate_request_body(self, path: str) -> Dict:
        """Generate request body specification for POST/PUT methods."""
        if '/tables' in path:
            return {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/TableRequest"}
                    }
                }
            }
        
        return {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"type": "object"}
                }
            }
        }

    def _generate_components(self) -> Dict:
        """Generate the components section with reusable schemas."""
        return {
            "schemas": {
                "ErrorResponse": {
                    "type": "object",
                    "properties": {
                        "error": {
                            "type": "string",
                            "description": "Error message"
                        }
                    },
                    "required": ["error"]
                },
                "TableRequest": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Table name"
                        },
                        "schema": {
                            "type": "object",
                            "description": "Table schema definition"
                        },
                        "partition_schema": {
                            "type": "object", 
                            "description": "Table partitioning schema"
                        }
                    },
                    "required": ["name", "schema", "partition_schema"]
                },
                "TablesResponse": {
                    "type": "object",
                    "properties": {
                        "tables": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "table_id": {
                                        "type": "string",
                                        "description": "Unique table identifier"
                                    },
                                    "table_name": {
                                        "type": "string",
                                        "description": "Table name"
                                    }
                                }
                            }
                        }
                    }
                },
                "TableResponse": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "id": {"type": "string"},
                        "schema": {"type": "object"},
                        "partition_schema": {"type": "object"},
                        "owner": {"type": "string"},
                        "comment": {"type": "string"},
                        "extra_config": {"type": "object"}
                    }
                }
            }
        }

def main():
    parser = argparse.ArgumentParser(description='Generate OpenAPI documentation from Kudu REST API source')
    parser.add_argument('--source-file', required=True,
                       help='Path to the C++ source file containing REST handlers')
    parser.add_argument('--output', required=True,
                       help='Output path for the generated OpenAPI JSON file')
    parser.add_argument('--header-file', 
                       help='Path to the header file (optional, for additional parsing)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Parsing source file: {args.source_file}")
    
    generator = SwaggerGenerator()
    
    try:
        generator.parse_source_file(args.source_file)
        openapi_spec = generator.generate_openapi_spec()
        
        with open(args.output, 'w') as f:
            json.dump(openapi_spec, f, indent=2, sort_keys=True)
        
        if args.verbose:
            print(f"Generated OpenAPI specification with {len(generator.endpoints)} endpoints")
            print(f"Output written to: {args.output}")
        
        print("Swagger documentation generated successfully!")
        
    except Exception as e:
        print(f"Error generating swagger documentation: {e}", file=sys.stderr)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 