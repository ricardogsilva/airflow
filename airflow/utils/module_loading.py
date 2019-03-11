# -*- coding: utf-8 -*-
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
import os
import sys

from airflow import configuration as conf
from importlib import import_module


def prepare_classpath():
    """
    Ensures that the Airflow home directory is on the classpath
    """
    config_path = os.path.join(conf.get('core', 'airflow_home'), 'config')
    config_path = os.path.expanduser(config_path)

    if config_path not in sys.path:
        sys.path.append(config_path)


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError:
        raise ImportError("{} doesn't look like a module path".format(dotted_path))

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        raise ImportError('Module "{}" does not define a "{}" attribute/class'.format(
            module_path, class_name)
        )

# TODO: Make sure not to use any python3 exclusive stuff, such as f-strings, or pathlib

def lazy_import(path: str):
    """Import a python named object dynamically.

    This function is used mainly in operators that derive from PythonOperator.

    Parameters
    ----------
    path: str
        Path to import. It can be either:

         -  a python class path, like `airflow.utils.module_loading.lazy_import`
         -  a filesystem path like `~/processes/callbacks.py:process_stuff`

    """

    try:
        python_name = _lazy_import_filesystem_path(path)
    except ImportError:
        python_name = _lazy_import_filesystem_path(path, assume_package=True)
    except (RuntimeError, KeyError):
        python_name = import_string(path)
    return python_name


def _lazy_import_filesystem_path(path:str, assume_package: bool=False):
    """Lazily import a python name from a filesystem path

    This function attempts to import the input ``path`` by assuming it is
    part of a python package. This is useful for those cases where the code
    in ``path`` uses relative imports.

    Parameters
    ----------
    path: str
        A colon separated string with the path to the module to load and the
        name of the object to import

    Returns
    -------
    The imported object

    Raises
    ------
    KeyError
        If the name is not found on the loaded python module
    RuntimeError
        If the path is not valid

    """

    filesystem_path, python_name = path.rpartition(":")[::2]
    full_path = os.path.abspath(os.path.expanduser(filesystem_path))
    if os.path.isfile(full_path):
        module_dir_path, module_filename = os.path.split(full_path)
        module_name = os.path.splitext(module_filename)[0]
        if assume_package:
            module_dir_ancestor_path = os.path.split(module_dir_path)
            sys.path.append(module_dir_ancestor_path)
        else:
            sys.path.append(module_dir_path)
        loaded_module = import_module("{}.{}".format(
            os.path.basename(module_dir_path),
            module_name)
        )
        return loaded_module.__dict__.get(python_name)
    else:
        raise RuntimeError(f"Invalid path {full_path}")
