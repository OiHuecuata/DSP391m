"""Validate our task graph/configuration using the real pinned Airflow runtime."""

import importlib.util
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "orchestration/airflow"))

from airflow.dag_processing.dagbag import DagBag
from smoke import EDGES


class MarketSignalDAGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["DSP_AIRFLOW_REPO_ROOT"] = str(ROOT)
        cls.bag = DagBag(dag_folder=str(ROOT / "orchestration/airflow/dags"))

    def test_imports_without_errors_and_exact_task_ids(self):
        self.assertEqual(self.bag.import_errors, {})
        self.assertEqual(set(self.bag.dags), {"dsp391m_market_signal_refresh"})
        dag = self.bag.dags["dsp391m_market_signal_refresh"]
        self.assertEqual(set(dag.task_ids), {name for edge in EDGES for name in edge})

    def test_dependencies_block_unsafe_publication(self):
        dag = self.bag.dags["dsp391m_market_signal_refresh"]
        actual = {
            (task.task_id, child)
            for task in dag.tasks
            for child in task.downstream_task_ids
        }
        self.assertEqual(actual, set(EDGES))
        for task in dag.tasks:
            self.assertEqual(task.trigger_rule, "all_success")
            self.assertFalse(task.depends_on_past)
            self.assertEqual(
                task.retries, 2 if task.task_id == "ensure_clickhouse_ready" else 0
            )
            self.assertFalse(task.do_xcom_push)

    def test_manual_schedule_and_single_writer_limits(self):
        dag = self.bag.dags["dsp391m_market_signal_refresh"]
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.max_active_tasks, 1)
        # Default is deliberately manual. Optional scheduling is explicit.
        self.assertEqual(dag.schedule, os.getenv("DSP_AIRFLOW_SCHEDULE") or None)
        spec = importlib.util.spec_from_file_location(
            "market_refresh_fixture",
            ROOT / "orchestration/airflow/dags/market_signal_refresh.py",
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        scheduled = module.build_dag("@daily")
        self.assertEqual(scheduled.schedule, "@daily")
        self.assertFalse(scheduled.catchup)

    def test_no_streaming_or_business_logic_in_task_commands(self):
        dag = self.bag.dags["dsp391m_market_signal_refresh"]
        for task in dag.tasks:
            self.assertIn("run_job.py", task.bash_command)
            self.assertNotIn("social_kafka_to_delta", task.bash_command)
            self.assertNotIn("{{", task.bash_command)
            self.assertIn("DSP_AIRFLOW_RUN_CONF", task.env)
