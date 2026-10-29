# src/runner/__init__.py
"""
Canonical Pipeline Execution Engine Package
"""
from src.runner.pipeline_runner import PipelineRunner, PipelineRunResult

__all__ = ["PipelineRunner", "PipelineRunResult"]
