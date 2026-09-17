from __future__ import annotations

import inspect

import yaml

from app.infrastructure.dag_generator.dag_generator import DagGenerator


def test_dag_generator_uses_injected_commit_hash() -> None:
    gen = DagGenerator(commit_hash="abc1234def")
    yaml_str = yaml.dump(
        {
            "pipeline": {
                "id": "pipe-test",
                "name": "test_pipe",
                "type": "ingestion",
                "owner": "owner@test.com",
                "schedule": {"mode": "cron", "cron": "0 6 * * *"},
            }
        }
    )
    dag_code = gen.generate(yaml_str)
    assert "abc1234def" in dag_code


def test_dag_generator_default_commit_hash_is_not_hardcoded_local() -> None:
    source = inspect.getsource(DagGenerator.render_pipeline_config)
    assert 'commit_hash="local"' not in source
