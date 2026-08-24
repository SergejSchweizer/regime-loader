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
BRANCH_RE = re.compile(r"^(pr-\d{2})/([a-z0-9]+(?:-[a-z0-9]+)*)$")
COMMIT_RE = re.compile(rf"^({ALLOWED_TYPES})\((pr-\d{{2}})\): (.+)$")
REQUIRED_FIELDS = (
    "PR name",
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
LEGACY_POSTGRES_FIRST = 31
LEGACY_POSTGRES_LAST = 39


@dataclass(frozen=True, slots=True)
class BacklogPr:
    pr_id: str
    body: str


def _sections(text: str) -> list[BacklogPr]:
    matches = list(HEADER_RE.finditer(text))
    result: list[BacklogPr] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        result.append(BacklogPr(match.group(1), text[match.end() : end]))
    return result


def _field(section: BacklogPr, name: str) -> str:
    matches = re.findall(rf"^{re.escape(name)}: (.+)$", section.body, re.MULTILINE)
    assert len(matches) == 1, f"{section.pr_id}: expected exactly one {name} field"
    value = matches[0].strip()
    if value.startswith("`") and value.endswith("`"):
        return value[1:-1]
    return value


def _requirement_ids(section: BacklogPr, prefix: str) -> list[int]:
    return [
        int(value)
        for value in re.findall(
            rf"^- {prefix}(\d+)(?: \(verifies R\d+\))?:", section.body, re.MULTILINE
        )
    ]


def _validate(text: str) -> list[BacklogPr]:
    sections = [
        section
        for section in _sections(text)
        if LEGACY_POSTGRES_FIRST <= int(section.pr_id[3:]) <= LEGACY_POSTGRES_LAST
    ]
    assert [section.pr_id for section in sections] == [
        f"PR-{value:02d}" for value in range(LEGACY_POSTGRES_FIRST, LEGACY_POSTGRES_LAST + 1)
    ]
    for section in sections:
        fields = {name: _field(section, name) for name in REQUIRED_FIELDS}
        pr_lower = section.pr_id.lower()
        pr_name = fields["PR name"]
        assert re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", pr_name)
        assert fields["Status"] in ALLOWED_DELIVERY
        assert fields["Git status"] in ALLOWED_GIT or fields["Git status"].startswith(
            "active-dirty: "
        )

        branch_match = BRANCH_RE.fullmatch(fields["Git branch"])
        assert branch_match is not None
        assert branch_match.group(1) == pr_lower
        assert branch_match.group(2) == pr_name

        commit_match = COMMIT_RE.fullmatch(fields["Commit"])
        assert commit_match is not None
        assert commit_match.group(2) == pr_lower
        assert pr_name in commit_match.group(3)

        requirements = _requirement_ids(section, "R")
        acceptance = _requirement_ids(section, "A")
        assert requirements == list(range(1, len(requirements) + 1))
        assert acceptance == requirements

        if fields["Status"] == "Merged":
            assert fields["Git status"] == "merged"
            assert fields["PR"].startswith("#")
        elif fields["Status"] == "Planned":
            assert fields["Git status"] == "not-started (branch absent)"
        elif fields["Status"] == "Ready":
            assert fields["Git status"] == "pushed-ci-green"
    return sections


def test_legacy_postgres_backlog_contract() -> None:
    sections = _validate(BACKLOG.read_text(encoding="utf-8"))
    assert len(sections) == LEGACY_POSTGRES_LAST - LEGACY_POSTGRES_FIRST + 1


def test_validator_rejects_missing_pr_name_in_branch() -> None:
    text = BACKLOG.read_text(encoding="utf-8").replace(
        "pr-32/postgres-gold-sync-contracts",
        "pr-32/wrong-name",
        1,
    )
    with pytest.raises(AssertionError):
        _validate(text)


def test_validator_rejects_requirement_acceptance_mismatch() -> None:
    text = BACKLOG.read_text(encoding="utf-8").replace(
        "- A4 (verifies R4): incompatible or non-current sources fail deterministically.",
        "- A5 (verifies R4): incompatible or non-current sources fail deterministically.",
        1,
    )
    with pytest.raises(AssertionError):
        _validate(text)
