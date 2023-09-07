# Copyright (c) 2013 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from sahara.plugins import base as pb
from sahara.plugins import edp
from sahara_plugin_spark.plugins.spark import plugin as pl
from sahara_plugin_spark.tests.unit import base


class SparkPluginTest(base.SaharaWithDbTestCase):
    def setUp(self):
        super(SparkPluginTest, self).setUp()
        self.override_config("plugins", ["spark"])
        pb.setup_plugins()

    def _init_cluster_dict(self, version):
        cluster_dict = {
            'name': 'cluster',
            'plugin_name': 'spark',
            'hadoop_version': version,
            'default_image_id': 'image'}
        return cluster_dict


class SparkProviderTest(base.SaharaTestCase):
    def setUp(self):
        super(SparkProviderTest, self).setUp()

    def test_supported_job_types(self):
        provider = pl.SparkProvider()

        res = provider.get_edp_job_types()
        self.assertEqual([edp.JOB_TYPE_SHELL, edp.JOB_TYPE_SPARK],
                         res['1.6.0'])
        self.assertEqual([edp.JOB_TYPE_SHELL, edp.JOB_TYPE_SPARK],
                         res['2.1.0'])
        self.assertEqual([edp.JOB_TYPE_SHELL, edp.JOB_TYPE_SPARK],
                         res['2.2'])
        self.assertEqual([edp.JOB_TYPE_SHELL, edp.JOB_TYPE_SPARK],
                         res['2.3'])

    def test_edp_config_hints(self):
        provider = pl.SparkProvider()

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SHELL, "1.6.0")
        self.assertEqual({'configs': {}, 'args': [], 'params': {}},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SPARK, "1.6.0")
        self.assertEqual({'args': [], 'configs': []},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SPARK, "2.1.0")
        self.assertEqual({'args': [], 'configs': []},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SHELL, "2.1.0")
        self.assertEqual({'args': [], 'configs': {}, 'params': {}},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SPARK, "2.2")
        self.assertEqual({'args': [], 'configs': []},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SHELL, "2.2")
        self.assertEqual({'args': [], 'configs': {}, 'params': {}},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SPARK, "2.3")
        self.assertEqual({'args': [], 'configs': []},
                         res['job_config'])

        res = provider.get_edp_config_hints(edp.JOB_TYPE_SHELL, "2.3")
        self.assertEqual({'args': [], 'configs': {}, 'params': {}},
                         res['job_config'])
