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

from unittest import mock

import testtools

from sahara.plugins import base as pb
from sahara.plugins import conductor
from sahara.plugins import context
from sahara.plugins import edp
from sahara.plugins import exceptions as pe
from sahara.plugins import testutils as tu
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

    def test_plugin11_edp_engine(self):
        self._test_engine('1.6.0', edp.JOB_TYPE_SPARK,
                          edp.PluginsSparkJobEngine)

    def test_plugin12_shell_engine(self):
        self._test_engine('1.6.0', edp.JOB_TYPE_SHELL,
                          edp.PluginsSparkShellJobEngine)

    def test_plugin21_edp_engine(self):
        self._test_engine('2.1.0', edp.JOB_TYPE_SPARK,
                          edp.PluginsSparkJobEngine)

    def test_plugin21_shell_engine(self):
        self._test_engine('2.1.0', edp.JOB_TYPE_SHELL,
                          edp.PluginsSparkShellJobEngine)

    def test_plugin22_edp_engine(self):
        self._test_engine('2.2', edp.JOB_TYPE_SPARK,
                          edp.PluginsSparkJobEngine)

    def test_plugin22_shell_engine(self):
        self._test_engine('2.2', edp.JOB_TYPE_SHELL,
                          edp.PluginsSparkShellJobEngine)

    def test_plugin23_edp_engine(self):
        self._test_engine('2.3', edp.JOB_TYPE_SPARK,
                          edp.PluginsSparkJobEngine)

    def test_plugin23_shell_engine(self):
        self._test_engine('2.3', edp.JOB_TYPE_SHELL,
                          edp.PluginsSparkShellJobEngine)

    def _test_engine(self, version, job_type, eng):
        cluster_dict = self._init_cluster_dict(version)

        cluster = conductor.cluster_create(context.ctx(), cluster_dict)
        plugin = pb.PLUGINS.get_plugin(cluster.plugin_name)
        self.assertIsInstance(plugin.get_edp_engine(cluster, job_type), eng)

    def test_cleanup_configs(self):
        remote = mock.Mock()
        instance = mock.Mock()

        extra_conf = {'job_cleanup': {
            'valid': True,
            'script': 'script_text',
            'cron': 'cron_text'}}
        instance.node_group.node_processes = ["master"]
        instance.node_group.id = id
        cluster_dict = self._init_cluster_dict('2.2')

        cluster = conductor.cluster_create(context.ctx(), cluster_dict)
        plugin = pb.PLUGINS.get_plugin(cluster.plugin_name)
        plugin._push_cleanup_job(remote, cluster, extra_conf, instance)
        remote.write_file_to.assert_called_with(
            '/etc/hadoop/tmp-cleanup.sh',
            'script_text')
        remote.execute_command.assert_called_with(
            'sudo sh -c \'echo "cron_text" > /etc/cron.d/spark-cleanup\'')

        remote.reset_mock()
        instance.node_group.node_processes = ["worker"]
        plugin._push_cleanup_job(remote, cluster, extra_conf, instance)
        self.assertFalse(remote.called)

        remote.reset_mock()
        instance.node_group.node_processes = ["master"]
        extra_conf['job_cleanup']['valid'] = False
        plugin._push_cleanup_job(remote, cluster, extra_conf, instance)
        remote.execute_command.assert_called_with(
            'sudo rm -f /etc/crond.d/spark-cleanup')


class SparkValidationTest(base.SaharaTestCase):
    def setUp(self):
        super(SparkValidationTest, self).setUp()
        self.override_config("plugins", ["spark"])
        pb.setup_plugins()
        self.plugin = pl.SparkProvider()

    def test_validate(self):
        self.ng = []
        self.ng.append(tu.make_ng_dict("nn", "f1", ["namenode"], 0))
        self.ng.append(tu.make_ng_dict("ma", "f1", ["master"], 0))
        self.ng.append(tu.make_ng_dict("sl", "f1", ["slave"], 0))
        self.ng.append(tu.make_ng_dict("dn", "f1", ["datanode"], 0))

        self._validate_case(1, 1, 3, 3)
        self._validate_case(1, 1, 3, 4)
        self._validate_case(1, 1, 4, 3)

        with testtools.ExpectedException(pe.InvalidComponentCountException):
            self._validate_case(2, 1, 3, 3)

        with testtools.ExpectedException(pe.InvalidComponentCountException):
            self._validate_case(1, 2, 3, 3)

        with testtools.ExpectedException(pe.InvalidComponentCountException):
            self._validate_case(0, 1, 3, 3)

        with testtools.ExpectedException(pe.RequiredServiceMissingException):
            self._validate_case(1, 0, 3, 3)

        cl = self._create_cluster(
            1, 1, 3, 3, cluster_configs={'HDFS': {'dfs.replication': 4}})

        with testtools.ExpectedException(pe.InvalidComponentCountException):
            self.plugin.validate(cl)

    def _create_cluster(self, *args, **kwargs):
        lst = []
        for i in range(0, len(args)):
            self.ng[i]['count'] = args[i]
            lst.append(self.ng[i])

        return tu.create_cluster("cluster1", "tenant1", "spark",
                                 "2.2", lst, **kwargs)

    def _validate_case(self, *args):
        cl = self._create_cluster(*args)
        self.plugin.validate(cl)


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
