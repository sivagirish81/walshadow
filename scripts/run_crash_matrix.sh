#!/usr/bin/env bash
set -Eeuo pipefail

ENV_FILE="${ENV_FILE:-.env}"
LAB_SCRIPT="${LAB_SCRIPT:-./scripts/checkpoint_lab.sh}"
MATRIX_ROOT="${MATRIX_ROOT:-artifacts/crash-matrix-$(date -u +%Y%m%dT%H%M%SZ)}"
TARGETS="${TARGETS:-64 128 256 384}"
REPEATS="${REPEATS:-1}"
INTER_RUN_SLEEP_SECS="${INTER_RUN_SLEEP_SECS:-5}"
# Empty delegates to checkpoint_lab.sh's target-based auto-sizing. Set an
# explicit positive value to override it for every target in this matrix.
MAX_BATCHES="${MAX_BATCHES:-}"
CATCHUP_TIMEOUT_SECS="${CATCHUP_TIMEOUT_SECS:-300}"
CDC_CATCHUP_TIMEOUT_SECS="${CDC_CATCHUP_TIMEOUT_SECS:-900}"
STALL_TIMEOUT_SECS="${STALL_TIMEOUT_SECS:-30}"
CLEANUP_ROWS="${CLEANUP_ROWS:-1}"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

mkdir -p "$MATRIX_ROOT"
RESULTS_CSV="$MATRIX_ROOT/results.csv"
MANIFEST="$MATRIX_ROOT/manifest.txt"

printf '%s\n' \
  'target_wal_mb,repeat,run_status,run_valid,target_reached,max_batches,batches_executed,generated_mib,pre_crash_gap_mib,pause_consumed_lsn,pre_crash_replay,pause_minus_shadow_bytes,shadow_queryable_secs,replay_after_queryable_secs,total_shadow_recovery_secs,recovery_upper_bound_secs,cdc_total_recovery_secs,first_replay_after_restart,source_checkpoint_changed,restartpoint_req_delta_without_source_cp,restartpoint_done_delta_without_source_cp,unlock_checkpoint_advanced,unlock_redo_advanced,unlock_req_delta,unlock_done_delta,run_dir,failure_phase,failure_reason' \
  > "$RESULTS_CSV"

{
    echo "matrix_root=$MATRIX_ROOT"
    echo "targets=$TARGETS"
    echo "repeats=$REPEATS"
    echo "max_batches=${MAX_BATCHES:-auto}"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$MANIFEST"

bytes_to_mib() {
    awk -v b="$1" 'BEGIN { printf "%.3f", b / 1048576 }'
}

csv_escape() {
    local value="${1//\"/\"\"}"
    printf '"%s"' "$value"
}

successful_runs=0
invalid_completed_runs=0
failed_runs=0
attempted_runs=0

for target in $TARGETS; do
    for ((rep=1; rep<=REPEATS; rep++)); do
        attempted_runs=$((attempted_runs + 1))
        echo
        echo "============================================================"
        echo "Crash benchmark: target=${target} MiB repeat=${rep}/${REPEATS} max_batches=${MAX_BATCHES:-auto}"
        echo "============================================================"

        before_file="$(mktemp)"
        after_file="$(mktemp)"
        find "$MATRIX_ROOT" -type f -name result.env -print 2>/dev/null | sort > "$before_file" || true

        lab_exit=0
        ENV_FILE=/dev/null \
        ARTIFACT_ROOT="$MATRIX_ROOT" \
        RUN_CRASH_TEST=1 \
        TARGET_WAL_MB="$target" \
        MAX_BATCHES="$MAX_BATCHES" \
        CATCHUP_TIMEOUT_SECS="$CATCHUP_TIMEOUT_SECS" \
        CDC_CATCHUP_TIMEOUT_SECS="$CDC_CATCHUP_TIMEOUT_SECS" \
        STALL_TIMEOUT_SECS="$STALL_TIMEOUT_SECS" \
        CLEANUP_ROWS="$CLEANUP_ROWS" \
        "$LAB_SCRIPT" || lab_exit=$?

        find "$MATRIX_ROOT" -type f -name result.env -print 2>/dev/null | sort > "$after_file" || true
        result_file="$(comm -13 "$before_file" "$after_file" | tail -n 1)"
        rm -f "$before_file" "$after_file"

        # Do not retain values sourced for the previous matrix row if a child
        # omitted a field or failed before writing result.env.
        unset RUN_STATUS RUN_VALID TARGET_REACHED GENERATED_BYTES GENERATED_MIB \
            PRE_CRASH_GAP_BYTES PRE_CRASH_GAP_MIB PAUSE_CONSUMED_LSN \
            PAUSE_MINUS_SHADOW_BYTES PRE_CRASH_REPLAY SHADOW_QUERYABLE_SECS \
            REPLAY_AFTER_QUERYABLE_SECS TOTAL_SHADOW_RECOVERY_SECS \
            SHADOW_RECOVERY_UPPER_BOUND_SECS CDC_TOTAL_RECOVERY_SECS \
            FIRST_REPLAY_AFTER_RESTART SOURCE_CHECKPOINT_CHANGED \
            REQ_DELTA_WITHOUT_SOURCE_CP DONE_DELTA_WITHOUT_SOURCE_CP \
            NOCP_REQ_DELTA NOCP_DONE_DELTA UNLOCK_CP_ADVANCED \
            UNLOCK_REDO_ADVANCED UNLOCK_REQ_DELTA UNLOCK_DONE_DELTA \
            EFFECTIVE_MAX_BATCHES BATCHES_EXECUTED RUN_DIR FAILURE_PHASE FAILURE_REASON || true

        if [[ -n "$result_file" && -f "$result_file" ]]; then
            # result.env is produced by checkpoint_lab.sh and contains only scalar values.
            # shellcheck disable=SC1090
            source "$result_file"
        else
            RUN_STATUS="failed"
            RUN_VALID=0
            FAILURE_PHASE="harness_result_collection"
            FAILURE_REASON="checkpoint_lab.sh exited with status $lab_exit without producing result.env"
            echo "ERROR: $FAILURE_REASON for target=$target repeat=$rep" >&2
        fi

        generated_mib="${GENERATED_MIB:-$(bytes_to_mib "${GENERATED_BYTES:-0}")}"
        pre_crash_gap_mib="${PRE_CRASH_GAP_MIB:-$(bytes_to_mib "${PRE_CRASH_GAP_BYTES:-0}")}"
        failure_reason_csv="$(csv_escape "${FAILURE_REASON:-}")"
        failure_phase_csv="$(csv_escape "${FAILURE_PHASE:-}")"
        run_dir_csv="$(csv_escape "${RUN_DIR:-}")"
        row_status="${RUN_STATUS:-failed}"
        if (( lab_exit != 0 )); then
            row_status="failed"
        fi

        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$target" "$rep" "$row_status" "${RUN_VALID:-0}" "${TARGET_REACHED:-0}" \
            "${EFFECTIVE_MAX_BATCHES:-${MAX_BATCHES:-auto}}" "${BATCHES_EXECUTED:-0}" "$generated_mib" "$pre_crash_gap_mib" \
            "${PAUSE_CONSUMED_LSN:-not-run}" "${PRE_CRASH_REPLAY:-not-run}" "${PAUSE_MINUS_SHADOW_BYTES:-0}" \
            "${SHADOW_QUERYABLE_SECS:-not-run}" "${REPLAY_AFTER_QUERYABLE_SECS:-not-run}" "${TOTAL_SHADOW_RECOVERY_SECS:-not-run}" \
            "${SHADOW_RECOVERY_UPPER_BOUND_SECS:-not-run}" \
            "${CDC_TOTAL_RECOVERY_SECS:-not-run}" \
            "${FIRST_REPLAY_AFTER_RESTART:-not-run}" "${SOURCE_CHECKPOINT_CHANGED:-not-run}" \
            "${REQ_DELTA_WITHOUT_SOURCE_CP:-${NOCP_REQ_DELTA:-0}}" "${DONE_DELTA_WITHOUT_SOURCE_CP:-${NOCP_DONE_DELTA:-0}}" \
            "${UNLOCK_CP_ADVANCED:-0}" "${UNLOCK_REDO_ADVANCED:-0}" "${UNLOCK_REQ_DELTA:-0}" "${UNLOCK_DONE_DELTA:-0}" "$run_dir_csv" \
            "$failure_phase_csv" "$failure_reason_csv" \
            >> "$RESULTS_CSV"

        if [[ "$row_status" != "success" ]]; then
            failed_runs=$((failed_runs + 1))
            echo "ERROR: run failed in ${FAILURE_PHASE:-unknown}: ${FAILURE_REASON:-unknown failure}" >&2
            echo "Failure recorded in $RESULTS_CSV; continuing with the matrix." >&2
        elif [[ "${RUN_VALID:-0}" == "1" ]]; then
            successful_runs=$((successful_runs + 1))
        else
            invalid_completed_runs=$((invalid_completed_runs + 1))
        fi

        if [[ "$row_status" == "success" ]]; then
            echo "Result:"
            echo "  actual WAL       = ${generated_mib} MiB"
            echo "  pre-crash gap    = ${pre_crash_gap_mib} MiB"
            echo "  queryable        = ${SHADOW_QUERYABLE_SECS}s"
            echo "  replay recovery  = ${REPLAY_AFTER_QUERYABLE_SECS}s"
            echo "  total recovery   = ${TOTAL_SHADOW_RECOVERY_SECS}s"
            echo "  CDC recovery     = ${CDC_TOTAL_RECOVERY_SECS}s"
            echo "  valid run        = ${RUN_VALID}"
            echo "  artifact         = ${RUN_DIR}"
        fi

        sleep "$INTER_RUN_SLEEP_SECS"
    done
done

{
    echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "results_csv=$RESULTS_CSV"
    echo "attempted_runs=$attempted_runs"
    echo "successful_runs=$successful_runs"
    echo "invalid_completed_runs=$invalid_completed_runs"
    echo "failed_runs=$failed_runs"
} >> "$MANIFEST"

echo
echo "============================================================"
echo "Matrix complete"
echo "Results: $RESULTS_CSV"
echo "Successful runs: $successful_runs"
echo "Invalid completed runs: $invalid_completed_runs"
echo "Failed runs: $failed_runs"
echo "============================================================"

if command -v column >/dev/null 2>&1; then
    column -s, -t "$RESULTS_CSV"
else
    cat "$RESULTS_CSV"
fi

(( failed_runs == 0 )) || exit 1
