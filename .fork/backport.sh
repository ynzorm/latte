#!/usr/bin/env bash
# backport.sh - Create or update a backport branch with develop-only commits.
#
# Usage: backport.sh [--pick <sha>] [--dry-run] [<backport-branch> [<base>]]
#   --pick <sha>       Only backport the given merge commit's branch, or a
#                      single commit. <sha> can be abbreviated.
#   --dry-run          Print the git commands instead of executing them.
#   <backport-branch>  Branch to create/update  (default: backport)
#   <base>             Base for new branch       (default: main)
#
# Workflow:
#   1. First run:   creates <backport-branch> from <base>, cherry-picks all
#                   develop-only commits (oldest first), or only --pick subset.
#   2. Next runs:   detects which commits are already applied (by patch-id),
#                   cherry-picks only the new ones.
#   3. On conflict: git pauses; resolve files, then:
#                     git cherry-pick --continue
#                   Re-run backport.sh afterwards to apply any remaining commits.
#   4. To abort:    git cherry-pick --abort
#                   Re-run backport.sh to retry from scratch.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DRY_RUN=0
PICK_SHA=""
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --pick) PICK_SHA="$2"; shift ;;
    *) args+=("$1") ;;
  esac
  shift
done

BACKPORT_BRANCH="${args[0]:-backport}"
BASE="${args[1]:-main}"

GIT_DIR="$(git rev-parse --git-dir)"

# ── Guards (skipped in dry-run) ──────────────────────────────────────────────
if (( ! DRY_RUN )); then
# ── Guard: any in-progress git operation ─────────────────────────────────────
if [[ -f "$GIT_DIR/CHERRY_PICK_HEAD" ]]; then
  echo "ERROR: a cherry-pick is in progress." >&2
  echo "  Resolve conflicts, then:  git cherry-pick --continue" >&2
  echo "  To abandon:               git cherry-pick --abort" >&2
  echo "  Then re-run backport.sh." >&2
  exit 1
fi
if [[ -f "$GIT_DIR/MERGE_HEAD" ]]; then
  echo "ERROR: a merge is in progress. Finish or abort it first:" >&2
  echo "  git merge --continue  |  git merge --abort" >&2
  exit 1
fi
if [[ -d "$GIT_DIR/rebase-merge" || -d "$GIT_DIR/rebase-apply" ]]; then
  echo "ERROR: a rebase is in progress. Finish or abort it first:" >&2
  echo "  git rebase --continue  |  git rebase --abort" >&2
  exit 1
fi
if [[ -f "$GIT_DIR/BISECT_LOG" ]]; then
  echo "ERROR: a bisect is in progress. Finish or abort it first:" >&2
  echo "  git bisect reset" >&2
  exit 1
fi

# ── Guard: uncommitted changes ────────────────────────────────────────────────
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "ERROR: you have uncommitted changes. Stash or commit them first:" >&2
  echo "  git stash  (then later: git stash pop)" >&2
  git status --short >&2
  exit 1
fi
fi # end guards

# ── Compute ordered develop-only SHAs (oldest → newest) ──────────────────────
echo "Computing develop-only commits..."
mapfile -t all_ordered < <("$SCRIPT_DIR/dev-commits.sh" --ordered)

if [[ ${#all_ordered[@]} -eq 0 ]]; then
  echo "No develop-only commits found. Nothing to backport."
  exit 0
fi

# ── Apply --pick filter if requested ─────────────────────────────────────────
if [[ -n "$PICK_SHA" ]]; then
  full_pick=$(git rev-parse "$PICK_SHA")
  parent_count=$(git log -1 --format="%P" "$full_pick" | wc -w)

  if (( parent_count > 1 )); then
    # Merge commit: collect all commits reachable from its non-first parents
    parent1=$(git log -1 --format="%P" "$full_pick" | awk '{print $1}')
    declare -A pick_set
    while read -r sha; do pick_set[$sha]=1; done < <(
      git log -p "$full_pick" --not "$parent1" --no-merges | git patch-id --stable | awk '{print $2}'
    )
    ordered=()
    for sha in "${all_ordered[@]}"; do
      [[ -n "${pick_set[$sha]+_}" ]] && ordered+=("$sha")
    done
    echo "(--pick: selecting commits from merge $PICK_SHA)"
  else
    # Single commit: just that one, verified it's in our good set
    declare -A good_set
    for sha in "${all_ordered[@]}"; do good_set[$sha]=1; done
    if [[ -z "${good_set[$full_pick]+_}" ]]; then
      echo "ERROR: $PICK_SHA is not a develop-only commit known to dev-commits.sh" >&2
      exit 1
    fi
    ordered=("$full_pick")
    echo "(--pick: selecting single commit $PICK_SHA)"
  fi
else
  ordered=("${all_ordered[@]}")
fi

if [[ ${#ordered[@]} -eq 0 ]]; then
  echo "No commits matched --pick $PICK_SHA."
  exit 0
fi

# ── Filter: remove commits already on the backport branch ────────────────────
declare -A already_set

if git rev-parse --verify "$BACKPORT_BRANCH" &>/dev/null; then
  echo "Checking what is already on '$BACKPORT_BRANCH'..."
  while read -r pid _; do
    already_set[$pid]=1
  done < <(git log -p "$BACKPORT_BRANCH" --not "$BASE" --no-merges | git patch-id --stable)
fi

new_shas=()
for sha in "${ordered[@]}"; do
  pid=$(git log -p --no-walk "$sha" | git patch-id --stable | awk '{print $1}')
  if [[ -z "${already_set[$pid]+_}" ]]; then
    new_shas+=("$sha")
  fi
done

if [[ ${#new_shas[@]} -eq 0 ]]; then
  echo "Backport branch '$BACKPORT_BRANCH' is already up to date."
  exit 0
fi

# ── Print plan ────────────────────────────────────────────────────────────────
already_count=$(( ${#ordered[@]} - ${#new_shas[@]} ))
echo ""
echo "Plan: cherry-pick ${#new_shas[@]} commit(s) onto '$BACKPORT_BRANCH'"
[[ $already_count -gt 0 ]] && echo "      ($already_count already applied, skipped)"
echo ""
for sha in "${new_shas[@]}"; do
  echo "  $(git log --no-walk --oneline "$sha")"
done
echo ""

# ── Dry-run: print commands and exit ─────────────────────────────────────────
if (( DRY_RUN )); then
  echo "# Commands to apply ${#new_shas[@]} commit(s) onto '$BACKPORT_BRANCH':"
  echo ""
  if ! git rev-parse --verify "$BACKPORT_BRANCH" &>/dev/null; then
    echo "git checkout -b $BACKPORT_BRANCH $BASE"
  else
    echo "git checkout $BACKPORT_BRANCH"
  fi
  echo "git cherry-pick ${new_shas[*]}"
  echo ""
  echo "# On conflict: resolve files, then:"
  echo "#   git cherry-pick --continue"
  echo "#   .fork/backport.sh $BACKPORT_BRANCH   # to apply any remaining commits"
  exit 0
fi

# ── Confirm ───────────────────────────────────────────────────────────────────
read -r -p "Proceed? [Y/n] " confirm
[[ "${confirm,,}" == "n" ]] && { echo "Aborted."; exit 0; }

# ── Create branch if needed ───────────────────────────────────────────────────
current_branch="$(git symbolic-ref --short HEAD)"

if ! git rev-parse --verify "$BACKPORT_BRANCH" &>/dev/null; then
  echo "Creating '$BACKPORT_BRANCH' from '$BASE'..."
  git checkout -b "$BACKPORT_BRANCH" "$BASE"
else
  git checkout "$BACKPORT_BRANCH"
fi

echo ""
echo "Cherry-picking onto '$BACKPORT_BRANCH'..."
echo "If a conflict occurs, git will pause. Resolve it, then:"
echo "  git cherry-pick --continue"
echo "  .fork/backport.sh   # to apply any remaining commits"
echo ""

# Cherry-pick all at once — git handles interactive conflict resolution natively
git cherry-pick "${new_shas[@]}"

echo ""
echo "Done. '$BACKPORT_BRANCH' is up to date."
echo "You were on '$current_branch'; you are now on '$BACKPORT_BRANCH'."
