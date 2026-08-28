from __future__ import annotations

from pathlib import Path

import api
import application
import ingestion
import scripts

ROOT = Path(__file__).resolve().parents[2]


def test_package_roots_are_importable() -> None:
    assert application.__doc__
    assert ingestion.__doc__
    assert api.__doc__
    assert scripts.__doc__


def test_quality_workflow_has_exact_required_jobs_and_triggers() -> None:
    text = (ROOT / ".github/workflows/quality-gates.yml").read_text(encoding="utf-8")
    for trigger in ("push:", "pull_request:", "merge_group:"):
        assert trigger in text
    assert "  push:\n    branches: [main]" in text
    assert "  push:\n  pull_request:" not in text
    for job in ("  lint:\n", "  type:\n", "  unit:\n", "  integration:\n", "  coverage:\n"):
        assert job in text
    assert "needs: [unit, integration]" in text
    assert "name: coverage-unit" in text
    assert "name: coverage-integration" in text


def test_makefile_enforces_coverage_and_parallel_gate() -> None:
    text = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "$(MAKE) -j4 lint type unit integration" in text
    assert "coverage report --fail-under=90" in text


def test_pre_push_hook_blocks_dirty_tree_before_quality_gate() -> None:
    text = (ROOT / ".githooks/pre-push").read_text(encoding="utf-8")
    dirty_index = text.index("git status --short")
    gate_index = text.index("make quality-gate")
    assert dirty_index < gate_index


def test_repository_setup_contains_required_policy() -> None:
    text = (ROOT / "scripts/setup_github_repository.sh").read_text(encoding="utf-8")
    for check in ("lint", "type", "unit", "integration", "coverage"):
        assert f'"{check}"' in text
    assert "allow_auto_merge=true" in text
    assert "allow_squash_merge=true" in text
    assert "allow_merge_commit=false" in text
    assert "allow_rebase_merge=false" in text
    assert '"enforce_admins": true' in text
    assert '"allow_force_pushes": false' in text
    assert '"allow_deletions": false' in text


def test_auto_merge_helper_uses_squash_and_auto() -> None:
    text = (ROOT / "scripts/enable_auto_merge.sh").read_text(encoding="utf-8")
    assert 'gh pr merge "$1" --auto --squash' in text
