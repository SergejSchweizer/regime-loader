from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

BACKLOG = Path("BACKLOG.md")
ALLOWED_DELIVERY = {"Planned", "In Progress", "Blocked", "Ready", "Merged"}
ALLOWED_GIT = {
    "not-started (branch absent)",
    "active-clean",
    "pushed-ci-failing",
    "pushed-ci-green",
    "merged",
}
ALLOWED_TYPES = "feat|fix|docs|test|refactor|perf|build|ci|chore"
HEADER_RE = re.compile(r"^## (PR-\d{2}): .+$", re.MULTILINE)
BRANCH_RE = re.compile(r"^(pr-\d{2})/[a-z0-9]+(?:-[a-z0-9]+)*$")
COMMIT_RE = re.compile(rf"^({ALLOWED_TYPES})\((pr-\d{{2}})\): [a-z0-9].+$")
REQUIRED_FIELDS = (
    "Status",
    "Updated",
    "PR",
    "Git branch",
    "Git status",
    "Agent lane",
    "Depends on",
    "Commit",
    "Design patterns",
)


@dataclass(frozen=True, slots=True)
class BacklogPr:
    pr_id: str
    body: str


def _sections(text: str) -> list[BacklogPr]:
    matches = list(HEADER_RE.finditer(text))
    sections: list[BacklogPr] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        sections.append(BacklogPr(match.group(1), text[match.end() : end]))
    return sections


def _field(section: BacklogPr, name: str) -> str:
    matches = re.findall(rf"^{re.escape(name)}: (.+)$", section.body, re.MULTILINE)
    assert len(matches) == 1, f"{section.pr_id}: expected exactly one {name} field"
    return matches[0].strip()


def _unquote(value: str) -> str:
    if value.startswith("`") and value.endswith("`"):
        return value[1:-1]
    return value


def _validate(text: str) -> list[BacklogPr]:
    sections = _sections(text)
    assert sections, "backlog must contain at least one PR section"
    expected = [f"PR-{index:02d}" for index in range(1, len(sections) + 1)]
    assert [section.pr_id for section in sections] == expected

    for section in sections:
        values = {name: _field(section, name) for name in REQUIRED_FIELDS}
        pr_lower = section.pr_id.lower()

        assert values["Status"] in ALLOWED_DELIVERY

        git_status = _unquote(values["Git status"])
        assert git_status in ALLOWED_GIT or git_status.startswith("active-dirty: ")

        branch = _unquote(values["Git branch"])
        branch_match = BRANCH_RE.fullmatch(branch)
        assert branch_match is not None, f"{section.pr_id}: invalid Git branch"
        assert branch_match.group(1) == pr_lower

        commit = _unquote(values["Commit"])
        commit_match = COMMIT_RE.fullmatch(commit)
        assert commit_match is not None, f"{section.pr_id}: invalid Conventional Commit"
        assert commit_match.group(2) == pr_lower

        patterns = values["Design patterns"].strip()
        assert patterns, f"{section.pr_id}: Design patterns must be non-empty"

        status = values["Status"]
        if status == "Merged":
            assert git_status == "merged"
            assert values["PR"].startswith("#")
        elif status == "Planned":
            assert git_status == "not-started (branch absent)"
        elif status == "Ready":
            assert git_status == "pushed-ci-green"
        elif status == "In Progress":
            assert git_status in {
                "active-clean",
                "pushed-ci-failing",
                "pushed-ci-green",
            } or git_status.startswith("active-dirty: ")

    return sections


def test_backlog_has_contiguous_pr_metadata_contract() -> None:
    sections = _validate(BACKLOG.read_text(encoding="utf-8"))
    assert sections[0].pr_id == "PR-01"


def _minimal_section() -> str:
    return """# Backlog

## PR-01: Example

Status: Planned
Updated: 2026-08-19
PR: none
Git branch: `pr-01/example`
Git status: `not-started (branch absent)`
Agent lane: Agent A
Depends on: none
Commit: `feat(pr-01): add example`
Design patterns: Architectural baseline only.
"""


def test_validator_rejects_gap_in_pr_sequence() -> None:
    text = _minimal_section() + _minimal_section().replace("PR-01", "PR-03").replace(
        "pr-01", "pr-03"
    )
    with pytest.raises(AssertionError):
        _validate(text)


def test_validator_rejects_missing_git_branch() -> None:
    text = _minimal_section().replace("Git branch: `pr-01/example`\n", "")
    with pytest.raises(AssertionError, match="Git branch"):
        _field(_sections(text)[0], "Git branch")


def test_validator_rejects_commit_pr_mismatch() -> None:
    section = _sections(_minimal_section())[0]
    commit = _unquote(_field(section, "Commit")).replace("pr-01", "pr-02")
    match = COMMIT_RE.fullmatch(commit)
    assert match is not None
    assert match.group(2) != section.pr_id.lower()


def test_validator_rejects_missing_design_patterns() -> None:
    text = _minimal_section().replace("Design patterns: Architectural baseline only.\n", "")
    with pytest.raises(AssertionError, match="Design patterns"):
        _field(_sections(text)[0], "Design patterns")


def test_validator_rejects_unknown_git_status() -> None:
    section = _sections(
        _minimal_section().replace(
            "Git status: `not-started (branch absent)`",
            "Git status: `almost-green`",
        )
    )[0]
    assert _unquote(_field(section, "Git status")) not in ALLOWED_GIT


def test_pushed_ci_failing_is_explicitly_allowed() -> None:
    assert "pushed-ci-failing" in ALLOWED_GIT
