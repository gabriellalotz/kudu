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

class KuduException(Exception):
    """
    Base exception class for all Kudu-related errors.
    
    All Kudu Python client exceptions inherit from this class,
    allowing you to catch all Kudu errors with a single except clause.
    """
    pass


class KuduBadStatus(KuduException):
    """
    A Kudu C++ client operation returned an error Status
    """
    pass


class KuduNotFound(KuduBadStatus):
    """
    Exception raised when a requested resource (table, tablet, etc.) is not found.
    
    This typically occurs when trying to access a table that doesn't exist
    or a tablet that has been deleted.
    """
    pass


class KuduNotSupported(KuduBadStatus):
    """
    Exception raised when an operation is not supported.
    
    This can occur when attempting to use a feature that is not available
    in the current version of Kudu or with the current configuration.
    """
    pass


class KuduInvalidArgument(KuduBadStatus):
    """
    Exception raised when an invalid argument is provided to an operation.
    
    This typically occurs when providing invalid column names, types,
    or values that don't match the schema constraints.
    """
    pass


class KuduNotAuthorized(KuduBadStatus):
    """
    Exception raised when the client is not authorized to perform an operation.
    
    This occurs when authentication is enabled and the client lacks
    the necessary permissions for the requested operation.
    """
    pass


class KuduAborted(KuduBadStatus):
    """
    Exception raised when an operation is aborted.
    
    This can occur during tablet leadership changes or when an
    operation times out or is explicitly cancelled.
    """
    pass


cdef check_status(const Status& status):
    if status.ok():
        return

    cdef string c_message = status.message().ToString()

    if status.IsNotFound():
        raise KuduNotFound(c_message)
    elif status.IsNotSupported():
        raise KuduNotSupported(c_message)
    elif status.IsInvalidArgument():
        raise KuduInvalidArgument(c_message)
    else:
        raise KuduBadStatus(status.ToString())
