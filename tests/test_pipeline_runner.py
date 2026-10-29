import unittest

from src.runner.pipeline_runner import PipelineRunner, PipelineRunResult

class TestPipelineRunner(unittest.TestCase):

    def setUp(self):
        self.runner = PipelineRunner()

    def test_pipeline_runner_rejects_mutable_execution(self):
        result = self.runner.run(source_tag="opf_oracle", table_name="tblop")
        self.assertIsInstance(result, PipelineRunResult)
        self.assertEqual(result.status, "FAILED")
        self.assertIsNone(result.execution_plan)
        self.assertIn("LEGACY_PIPELINE_RUNNER_RETIRED", result.errors[0])

if __name__ == "__main__":
    unittest.main()
