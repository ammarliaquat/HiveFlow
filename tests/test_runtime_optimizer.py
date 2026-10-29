import unittest
from unittest.mock import MagicMock
from src.runtime.runtime_optimizer import RuntimeOptimizer, OptimizationPlan
from src.metadata.metadata_models import MetadataRow

class TestRuntimeOptimizer(unittest.TestCase):

    def setUp(self):
        self.mock_cm = MagicMock()
        self.mock_cm.get_capabilities.return_value = {"supports_parallel_read": True}
        self.mock_cm.get_dynamic_concurrency.return_value = 5
        self.optimizer = RuntimeOptimizer(self.mock_cm)

    def test_calculate_fetch_size_with_lob(self):
        fetch_sz = self.optimizer.calculate_fetch_size("opf_oracle", "tblop_clob", has_lob=True)
        self.assertEqual(fetch_sz, 100)

    def test_calculate_fetch_size_standard(self):
        fetch_sz = self.optimizer.calculate_fetch_size("opf_oracle", "tblop_standard", has_lob=False)
        self.assertEqual(fetch_sz, 10000)

    def test_calculate_partitions_sizing(self):
        # 500 MB dataset / 128 MB target = 3 partitions
        parts = self.optimizer.calculate_partitions(estimated_size_mb=500.0)
        self.assertEqual(parts, 3)

        # 0 MB dataset fallback
        parts_zero = self.optimizer.calculate_partitions(estimated_size_mb=0.0, default_partitions=8)
        self.assertEqual(parts_zero, 8)

    def test_optimize_table_execution(self):
        self.optimizer.fetch_historical_stats = MagicMock(return_value={
            "average_row_count": 1000,
            "average_size_mb": 25.0,
            "average_duration_sec": 12.0,
            "average_extract_speed_rps": 500.0,
            "p95_duration_sec": 15.0,
        })
        meta = MagicMock()
        meta.source_tag = "opf_oracle"
        meta.source_table = "tblop"
        meta.chunk_size = 10000
        meta.num_partitions = 5
        plan = self.optimizer.optimize_table_execution(meta, has_lob=True)
        self.assertIsInstance(plan, OptimizationPlan)
        self.assertEqual(plan.fetch_size, 100)
        self.assertTrue(plan.is_lob_throttled)
        self.assertEqual(plan.parallel_concurrency, 5)

if __name__ == "__main__":
    unittest.main()
