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

import mock
import unittest

from airflow.utils import module_loading


class ModuleImportTestCase(unittest.TestCase):

    def test_import_string(self):
        cls = module_loading.import_string('airflow.utils.module_loading.import_string')
        self.assertEqual(cls, module_loading.import_string)

        # Test exceptions raised
        with self.assertRaises(ImportError):
            module_loading.import_string('no_dots_in_path')
        msg = 'Module "airflow.utils" does not define a "nonexistent" attribute'
        with self.assertRaisesRegexp(ImportError, msg):
            module_loading.import_string('airflow.utils.nonexistent')

    @mock.patch('airflow.utils.module_loading.import_module')
    @mock.patch('airflow.utils.module_loading.os')
    @mock.patch('airflow.utils.module_loading.sys')
    def test_lazy_import_file_system_path(
            self, patched_import_module, patched_os, patched_sys):
        fake_dir = "/some/dummy/path"
        fake_module_name = "fakemodule"
        fake_module = "{}.py".format(fake_module_name)
        fake_func_name = "phony_func"
        fake_path = "{}/{}:{}".format(fake_dir, fake_module, fake_func_name)
        fake_callable = "hi, I'm a fake callable"
        patched_os.abspath.return_value = fake_path
        patched_os.expanduser.return_value = fake_path
        patched_os.isfile.return_value = True
        patched_os.split.return_value = (
            fake_dir, "{}:{}".format(fake_module, fake_func_name))
        patched_os.splitext.return_value = (fake_module_name, ".py")
        patched_import_module.return_value = mock.MagickMock(
            __dict__={fake_func_name: fake_callable})
        result = module_loading.lazy_import(fake_path)
        self.assertEquals(result, fake_callable)
