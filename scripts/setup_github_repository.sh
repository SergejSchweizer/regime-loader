#!/usr/bin/env bash
set -euo pipefail

verify_only=false
if [[ "${1:-}" == "--verify" ]]; then
  verify_only=true
  shift
fi

repo="${1:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"

command -v gh >/dev/null 2>&1 || {
  echo "gh CLI is required" >&2
  exit 2
}

gh auth status >/dev/null

verify_repository() {
  local repository_settings protection_settings
  repository_settings="$(gh api "repos/${repo}" --jq '[
    .allow_squash_merge,
    .allow_merge_commit,
    .allow_rebase_merge,
    .delete_branch_on_merge,
    .allow_auto_merge
  ] | @tsv')"
  protection_settings="$(gh api "repos/${repo}/branches/main/protection" --jq '[
    .required_status_checks.strict,
    (.required_status_checks.contexts | sort | join(",")),
    .enforce_admins.enabled,
    .required_linear_history.enabled,
    .allow_force_pushes.enabled,
    .allow_deletions.enabled,
    .restrictions
  ] | @tsv')"
  [[ "$repository_settings" == $'true\tfalse\tfalse\ttrue\ttrue' ]] || {
    echo "repository merge policy differs from the source-controlled contract" >&2
    return 1
  }
  [[ "$protection_settings" == $'true\tcoverage,integration,lint,type,unit\ttrue\ttrue\tfalse\tfalse\t' ]] || {
    echo "main protection differs from the source-controlled contract" >&2
    return 1
  }
}

if [[ "$verify_only" == true ]]; then
  verify_repository
  printf 'verified protected main and auto-merge for %s\n' "$repo"
  exit 0
fi

# Repository merge policy: squash only, delete merged branches, allow PR auto-merge.
gh api --method PATCH "repos/${repo}" \
  -F allow_squash_merge=true \
  -F allow_merge_commit=false \
  -F allow_rebase_merge=false \
  -F delete_branch_on_merge=true \
  -F allow_auto_merge=true >/dev/null

protection_json="$(mktemp)"
trap 'rm -f "$protection_json"' EXIT
cat >"$protection_json" <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["lint", "type", "unit", "integration", "coverage"]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": false,
    "require_code_owner_reviews": false,
    "required_approving_review_count": 0
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": false,
  "lock_branch": false,
  "allow_fork_syncing": false
}
JSON

gh api --method PUT \
  -H "Accept: application/vnd.github+json" \
  "repos/${repo}/branches/main/protection" \
  --input "$protection_json" >/dev/null

verify_repository
printf 'configured protected main and auto-merge for %s\n' "$repo"
