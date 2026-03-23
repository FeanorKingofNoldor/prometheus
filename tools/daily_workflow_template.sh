#!/usr/bin/env bash
set -u

usage() {
  cat <<'EOF'
Usage:
  daily_workflow_template.sh [--project DIR] [--name LABEL] [--log-root DIR]
                             [--note TEXT]... [--step "COMMAND"]...

Behavior:
  - Appends all output to PROJECT/logs/daily/YYYY-MM-DD.log (or --log-root)
  - Runs each --step command in sequence via bash -lc
  - Records per-step pass/fail and exits non-zero if any step fails
EOF
}

project_dir=""
workflow_name="daily-workflow"
log_root=""
declare -a notes=()
declare -a steps=()

while (($#)); do
  case "$1" in
    --project)
      project_dir="$2"
      shift 2
      ;;
    --name)
      workflow_name="$2"
      shift 2
      ;;
    --log-root)
      log_root="$2"
      shift 2
      ;;
    --note)
      notes+=("$2")
      shift 2
      ;;
    --step)
      steps+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$project_dir" ]]; then
  project_dir="$(pwd)"
fi
if [[ ! -d "$project_dir" ]]; then
  echo "Project directory does not exist: $project_dir" >&2
  exit 2
fi

if [[ -z "$log_root" ]]; then
  log_root="$project_dir/logs/daily"
fi

mkdir -p "$log_root"
log_file="$log_root/$(date +%F).log"

if [[ ${#steps[@]} -eq 0 ]]; then
  steps+=("git --no-pager -C \"$project_dir\" status --short")
fi

append_line() {
  printf '%s\n' "$1" >> "$log_file"
}

append_line "================================================================================"
append_line "timestamp: $(date --iso-8601=seconds)"
append_line "workflow: ${workflow_name}"
append_line "project: ${project_dir}"
for note in "${notes[@]}"; do
  append_line "note: ${note}"
done
append_line "--------------------------------------------------------------------------------"

failures=0
step_index=0

for step_cmd in "${steps[@]}"; do
  step_index=$((step_index + 1))
  append_line "[$(date --iso-8601=seconds)] step ${step_index} start: ${step_cmd}"
  if bash -lc "$step_cmd" >> "$log_file" 2>&1; then
    append_line "[$(date --iso-8601=seconds)] step ${step_index} status: PASS"
  else
    append_line "[$(date --iso-8601=seconds)] step ${step_index} status: FAIL"
    failures=$((failures + 1))
  fi
  append_line "--------------------------------------------------------------------------------"
done

append_line "summary: total_steps=${step_index} failures=${failures}"
append_line "================================================================================"

if ((failures > 0)); then
  echo "Completed with ${failures} failing step(s). Log: ${log_file}" >&2
  exit 1
fi

echo "Completed successfully. Log: ${log_file}"
