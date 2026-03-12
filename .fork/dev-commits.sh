#!/usr/bin/env bash
# Show commits in develop that are not in main, accounting for main→develop merges.
# Usage: dev-commits.sh [--list|--merges|--graph]
#   --list   (default) flat list of non-merge, non-fork commits
#   --merges list of merge commits that bring in develop-only work
#   --graph  combined graph showing branch structure

set -euo pipefail

# Compute develop-only commits by patch-id (handles main→develop merge duplicates)
# Output: full SHAs, one per line. "fork:" commits are always excluded.
good_full() {
  awk 'NR==FNR{ids[$1]=1;next} !ids[$1]{print $2}' \
    <(git log -p main --no-merges | git patch-id --stable) \
    <(git log -p develop --not main --no-merges | git patch-id --stable) \
  | xargs git log --no-walk --format="%H %s" \
  | grep -v " fork:" \
  | awk '{print $1}'
}

# Shared ordered iterator used by --graph, --ordered, --merges.
# Emits tagged lines oldest-first:
#   merge <full-sha> <oneline>
#   commit <full-sha> <oneline>
#   orphan <full-sha> <oneline>
iter_ordered() {
  mapfile -t good_short < <(good_full | awk '{print substr($1,1,7)}')
  declare -A good_set
  for sha in "${good_short[@]}"; do good_set[$sha]=1; done
  declare -A under_merge

  while read -r merge_sha; do
    read -ra parents <<< "$(git log -1 --format="%P" "$merge_sha")"
    parent1="${parents[0]}"
    merge_line=$(git log --no-walk --oneline "$merge_sha")
    branch_lines=()
    while read -r bsha full; do
      if [[ -n "${good_set[$bsha]+_}" ]]; then
        branch_lines+=("$full $(git log --no-walk --format="%s" "$full")")
        under_merge[$bsha]=1
      fi
    done < <(
      for parent in "${parents[@]:1}"; do
        git log "$parent" --not "$parent1" --no-merges --reverse --format="%h %H"
      done
    )
    if (( ${#branch_lines[@]} > 0 )); then
      echo "merge $merge_sha $merge_line"
      for line in "${branch_lines[@]}"; do
        full="${line%% *}"
        echo "commit $full $(git log --no-walk --oneline "$full")"
      done
    fi
  done < <(git log develop --not main --merges --format="%H" --reverse)

  for sha in "${good_short[@]}"; do
    if [[ -z "${under_merge[$sha]+_}" ]]; then
      full=$(git log --no-walk --format="%H" "$sha")
      echo "orphan $full $(git log --no-walk --oneline "$full")"
    fi
  done
}

mode="${1:---list}"

case "$mode" in
  --list)
    good_full | xargs git log --no-walk --oneline
    ;;

  --merges)
    iter_ordered | awk '$1=="merge"{$1=$2=""; print substr($0,3)}' | grep -v " fork:"
    ;;

  --graph)
    current_merge=""
    while IFS= read -r line; do
      tag="${line%% *}"
      rest="${line#* }"       # <full-sha> <oneline>
      oneline="${rest#* }"    # <oneline>  (drop full sha)
      case "$tag" in
        merge)
          [[ -n "$current_merge" ]] && echo "  |"
          current_merge="$oneline"
          echo "* $oneline"
          ;;
        commit) echo "  | $oneline" ;;
        orphan)
          [[ -n "$current_merge" ]] && echo "  |" && current_merge=""
          echo "* (direct) $oneline"
          ;;
      esac
    done < <(iter_ordered | grep -v " fork:")
    [[ -n "$current_merge" ]] && echo "  |"
    ;;

  --ordered)
    iter_ordered | awk '$1=="commit"||$1=="orphan"{print $2}'
    ;;

  *)
    echo "Usage: $0 [--list|--merges|--graph|--ordered]" >&2
    exit 1
    ;;
esac
