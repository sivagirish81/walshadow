#!/usr/bin/env bash
set -Eeuo pipefail

# ==============================================================================
# WalShadow checkpoint / restartpoint + crash recovery lab
# ==============================================================================

ENV_FILE="${ENV_FILE:-.env}"
COMPOSE_FILE="${COMPOSE_FILE:-docker/docker-compose.yml}"
WALSHADOW_SERVICE="${WALSHADOW_SERVICE:-walshadow}"
TEST_TABLE="${TEST_TABLE:-public.checkpoint_probe}"

SHADOW_SOCKET="${SHADOW_SOCKET:-/var/run/postgresql}"
SHADOW_PORT="${SHADOW_PORT:-5432}"
SHADOW_USER="${SHADOW_USER:-postgres}"
SHADOW_DB="${SHADOW_DB:-postgres}"

BATCH_ROWS="${BATCH_ROWS:-25000}"
PAYLOAD_REPEAT="${PAYLOAD_REPEAT:-4}"
TARGET_WAL_MB="${TARGET_WAL_MB:-96}"
# Empty means auto-size after .env is loaded. At the default workload shape a
# batch normally emits more than 6 MiB, so this conservative floor plus
# headroom gives a 2 GiB run at least 352 attempts. An explicit MAX_BATCHES
# remains authoritative.
MAX_BATCHES="${MAX_BATCHES:-}"
BATCH_WAL_FLOOR_MIB="${BATCH_WAL_FLOOR_MIB:-6}"
BATCH_HEADROOM="${BATCH_HEADROOM:-10}"
CATCHUP_TIMEOUT_SECS="${CATCHUP_TIMEOUT_SECS:-300}"
CDC_CATCHUP_TIMEOUT_SECS="${CDC_CATCHUP_TIMEOUT_SECS:-900}"
STALL_TIMEOUT_SECS="${STALL_TIMEOUT_SECS:-30}"
PREFLIGHT_TIMEOUT_SECS="${PREFLIGHT_TIMEOUT_SECS:-300}"
PREFLIGHT_MAX_LAG_MB="${PREFLIGHT_MAX_LAG_MB:-32}"
PRE_CRASH_STABLE_SAMPLES="${PRE_CRASH_STABLE_SAMPLES:-2}"
UNLOCK_TIMEOUT_SECS="${UNLOCK_TIMEOUT_SECS:-30}"
UNLOCK_COUNTER_GRACE_SECS="${UNLOCK_COUNTER_GRACE_SECS:-5}"
PROGRESS_LOG_INTERVAL_SECS="${PROGRESS_LOG_INTERVAL_SECS:-10}"
SOURCE_RETRIES="${SOURCE_RETRIES:-3}"
SOURCE_RETRY_DELAY_SECS="${SOURCE_RETRY_DELAY_SECS:-2}"
WORKLOAD_STATEMENT_TIMEOUT_SECS="${WORKLOAD_STATEMENT_TIMEOUT_SECS:-120}"
ALLOW_UNSLOTTED_SOURCE="${ALLOW_UNSLOTTED_SOURCE:-0}"
RUN_CRASH_TEST="${RUN_CRASH_TEST:-0}"
CLEANUP_ROWS="${CLEANUP_ROWS:-0}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-artifacts}"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

for numeric_setting in TARGET_WAL_MB BATCH_WAL_FLOOR_MIB BATCH_HEADROOM \
    PREFLIGHT_TIMEOUT_SECS PREFLIGHT_MAX_LAG_MB PRE_CRASH_STABLE_SAMPLES \
    UNLOCK_TIMEOUT_SECS UNLOCK_COUNTER_GRACE_SECS; do
    numeric_value="${!numeric_setting}"
    [[ "$numeric_value" =~ ^[1-9][0-9]*$ ]] || {
        echo "$numeric_setting must be a positive integer (got: $numeric_value)." >&2
        exit 1
    }
done

if [[ -z "$MAX_BATCHES" ]]; then
    MAX_BATCHES=$(( (TARGET_WAL_MB + BATCH_WAL_FLOOR_MIB - 1) / BATCH_WAL_FLOOR_MIB + BATCH_HEADROOM ))
elif [[ ! "$MAX_BATCHES" =~ ^[1-9][0-9]*$ ]]; then
    echo "MAX_BATCHES must be a positive integer (got: $MAX_BATCHES)." >&2
    exit 1
fi

: "${WALSHADOW_PG_URL:?WALSHADOW_PG_URL is not set}"

if [[ "${LAB_ACK:-}" != "YES" ]]; then
    cat <<'MSG'
This experiment writes rows, executes CHECKPOINT on the PostgreSQL source,
and intentionally generates WAL. Use only a disposable/test PostgreSQL source.

Set LAB_ACK=YES in .env when ready.
MSG
    exit 1
fi

RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")-$$"
RUN_TAG="walshadow_cp_${RUN_ID}"
RUN_DIR="${ARTIFACT_ROOT}/checkpoint-${RUN_ID}-target${TARGET_WAL_MB}MiB"
mkdir -p "$RUN_DIR"

RUN_LOG="$RUN_DIR/run.log"
COMMAND_LOG="$RUN_DIR/commands.log"
ERROR_LOG="$RUN_DIR/errors.log"
SAMPLES="$RUN_DIR/samples.csv"
SUMMARY="$RUN_DIR/summary.md"
RESULT_ENV="$RUN_DIR/result.env"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

touch "$RUN_LOG" "$COMMAND_LOG" "$ERROR_LOG"
printf '%s\n' \
  'timestamp,phase,source_current_lsn,source_checkpoint_lsn,source_redo_lsn,shadow_replay_lsn,shadow_checkpoint_lsn,shadow_redo_lsn,source_to_shadow_lag_bytes,shadow_recovery_gap_bytes,restartpoints_timed,restartpoints_req,restartpoints_done' \
  > "$SAMPLES"

CURRENT_PHASE="setup"
RUN_STATUS="running"
FAILURE_REASON=""
STREAM_PAUSED_BY_HARNESS=0

# ----------------------------- helpers ----------------------------------------

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# macOS BSD date does not support %N. Python gives portable millisecond epoch time.
now_ms() {
    python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

log() { printf '[%s] %s\n' "$(timestamp)" "$*" | tee -a "$RUN_LOG"; }
record_command() { printf '[%s] %s\n' "$(timestamp)" "$*" >> "$COMMAND_LOG"; }
die() {
    FAILURE_REASON="$*"
    log "ERROR: $*"
    exit 1
}

human_mib() {
    local bytes="${1:-0}"
    awk -v b="$bytes" 'BEGIN { printf "%.2f MiB", b / 1048576 }'
}

seconds_between_ms() {
    local start="$1" end="$2"
    awk -v s="$start" -v e="$end" 'BEGIN { printf "%.3f", (e-s)/1000 }'
}

source_q() {
    local sql="$1"
    psql "$WALSHADOW_PG_URL" -X -v ON_ERROR_STOP=1 -A -t -F '|' -c "$sql" 2>>"$ERROR_LOG"
}

shadow_q() {
    local sql="$1"
    "${COMPOSE[@]}" exec -T "$WALSHADOW_SERVICE" \
        psql -X -v ON_ERROR_STOP=1 -A -t -F '|' \
        -h "$SHADOW_SOCKET" -p "$SHADOW_PORT" -U "$SHADOW_USER" -d "$SHADOW_DB" \
        -c "$sql" 2>>"$ERROR_LOG"
}

ctl_q() {
    local verb="$1"
    "${COMPOSE[@]}" exec -T "$WALSHADOW_SERVICE" \
        walshadow-stream ctl "$verb" 2>>"$ERROR_LOG"
}

ctl_field_q() {
    local field="$1"
    ctl_q status | awk -F '[[:space:]]*=[[:space:]]*' -v field="$field" '
        $1 == field {
            gsub(/^"|"$/, "", $2)
            print $2
            exit
        }
    '
}

metrics_field_q() {
    local field="$1"
    curl -fsS http://localhost:9484/metrics 2>/dev/null | awk -v field="$field" '
        $1 == field { print $2; exit }
    '
}

compose_service_running() {
    [[ -n "$("${COMPOSE[@]}" ps --status running -q "$WALSHADOW_SERVICE" 2>/dev/null)" ]]
}

record_stopped_service() {
    local context="$1"
    WAIT_FAILURE_REASON="WalShadow service exited while ${context}. See service-exit-${CURRENT_PHASE}.log."
    "${COMPOSE[@]}" ps -a > "$RUN_DIR/service-exit-${CURRENT_PHASE}.status" 2>&1 || true
    "${COMPOSE[@]}" logs --no-color --tail 200 "$WALSHADOW_SERVICE" \
        > "$RUN_DIR/service-exit-${CURRENT_PHASE}.log" 2>&1 || true
}

# Keep the crash-recovery hot path free of logging and unrelated SQL. Function
# call redirections do not override shadow_q's own stderr redirection, so this
# deliberately invokes psql directly instead of wrapping shadow_q.
shadow_replay_q() {
    "${COMPOSE[@]}" exec -T "$WALSHADOW_SERVICE" \
        psql -X -v ON_ERROR_STOP=1 -A -t \
        -h "$SHADOW_SOCKET" -p "$SHADOW_PORT" -U "$SHADOW_USER" -d "$SHADOW_DB" \
        -c "SELECT pg_last_wal_replay_lsn()::text;" 2>/dev/null
}

valid_lsn() {
    [[ "$1" =~ ^[0-9A-Fa-f]+/[0-9A-Fa-f]+$ ]]
}

replay_reached_lsn() {
    local replay="$1" target="$2"
    local replay_hi replay_lo target_hi target_lo
    valid_lsn "$replay" && valid_lsn "$target" || return 1
    IFS='/' read -r replay_hi replay_lo <<< "$replay"
    IFS='/' read -r target_hi target_lo <<< "$target"

    # Compare the two 32-bit halves independently. This is exact across the
    # full pg_lsn range and avoids issuing source SQL from recovery polling.
    (( 16#$replay_hi > 16#$target_hi )) ||
        (( 16#$replay_hi == 16#$target_hi && 16#$replay_lo >= 16#$target_lo ))
}

lsn_strictly_after() {
    local candidate="$1" baseline="$2"
    replay_reached_lsn "$candidate" "$baseline" && [[ "$candidate" != "$baseline" ]]
}

status_field_from() {
    local status="$1" field="$2"
    printf '%s\n' "$status" | awk -F '[[:space:]]*=[[:space:]]*' -v field="$field" '
        $1 == field {
            gsub(/^"|"$/, "", $2)
            print $2
            exit
        }
    '
}

wait_walshadow_preflight_ready() {
    local deadline status source_received shadow_replay source_system_id uptime paused backfills lag_bytes=""
    local max_lag_bytes=$((PREFLIGHT_MAX_LAG_MB * 1024 * 1024))
    local last_log=0 now

    deadline=$(( $(date +%s) + PREFLIGHT_TIMEOUT_SECS ))
    while true; do
        status="$(ctl_q status || true)"
        source_received="$(status_field_from "$status" source_received)"
        shadow_replay="$(status_field_from "$status" shadow_replay)"
        source_system_id="$(status_field_from "$status" source_system_id)"
        uptime="$(status_field_from "$status" uptime_secs)"
        paused="$(status_field_from "$status" paused)"
        backfills="$(status_field_from "$status" backfills_pending)"
        lag_bytes=""

        if valid_lsn "$source_received" && [[ "$source_received" != "0/0" ]] &&
           valid_lsn "$shadow_replay" && [[ "$shadow_replay" != "0/0" ]]; then
            lag_bytes="$(source_q "SELECT GREATEST(pg_wal_lsn_diff('$source_received'::pg_lsn, '$shadow_replay'::pg_lsn), 0)::bigint;" || true)"
        fi

        if [[ "$source_system_id" =~ ^[1-9][0-9]*$ && "$uptime" =~ ^[1-9][0-9]*$ &&
              "$paused" == "false" && "$backfills" == "0" &&
              "$lag_bytes" =~ ^[0-9]+$ ]] && (( lag_bytes <= max_lag_bytes )); then
            [[ "$(shadow_q "SELECT pg_is_in_recovery();" || true)" == "t" ]] || {
                WAIT_FAILURE_REASON="WalShadow control state initialized, but shadow PostgreSQL is not queryable in recovery."
                return 1
            }
            log "WalShadow is initialized and caught up for preflight (source=$source_received shadow=$shadow_replay lag=$(human_mib "$lag_bytes"))."
            return 0
        fi

        if ! compose_service_running; then
            record_stopped_service "waiting for initialized preflight state"
            return 3
        fi

        now="$(date +%s)"
        if (( now >= deadline )); then
            WAIT_FAILURE_REASON="WalShadow preflight did not become healthy within ${PREFLIGHT_TIMEOUT_SECS}s (source_received=${source_received:-missing}, shadow_replay=${shadow_replay:-missing}, source_system_id=${source_system_id:-missing}, uptime_secs=${uptime:-missing}, paused=${paused:-missing}, backfills_pending=${backfills:-missing}, lag_bytes=${lag_bytes:-unknown})."
            return 1
        fi
        if (( now - last_log >= PROGRESS_LOG_INTERVAL_SECS )); then
            log "Waiting for initialized/caught-up WalShadow state: source=${source_received:-missing} shadow=${shadow_replay:-missing} lag=${lag_bytes:-unknown} paused=${paused:-missing} backfills=${backfills:-missing}"
            last_log="$now"
        fi
        sleep 2
    done
}

source_sql() {
    local sql="$1"
    record_command "SOURCE SQL:"
    printf '%s\n\n' "$sql" >> "$COMMAND_LOG"
    log "SOURCE SQL:"
    printf '%s\n' "$sql" | tee -a "$RUN_LOG"
    source_q "$sql" | tee -a "$RUN_LOG"
}

run_workload_batch() {
    local batch="$1" sql="$2" attempt rows
    for ((attempt=1; attempt<=SOURCE_RETRIES; attempt++)); do
        if source_sql "$sql"; then
            return 0
        fi

        log "WARNING: workload batch $batch attempt $attempt/$SOURCE_RETRIES lost its source connection."
        rows="$(source_q "SELECT count(*) FROM $TEST_TABLE WHERE payload LIKE '$RUN_TAG:$batch:%';" || true)"
        if [[ "$rows" =~ ^[0-9]+$ ]] && (( rows >= BATCH_ROWS )); then
            log "Batch $batch committed before the connection failed; continuing without replaying it."
            return 0
        fi
        (( attempt < SOURCE_RETRIES )) || break
        sleep "$SOURCE_RETRY_DELAY_SECS"
    done
    die "Workload batch $batch failed after $SOURCE_RETRIES attempts. See errors.log."
}

shadow_sql() {
    local sql="$1"
    record_command "SHADOW SQL:"
    printf '%s\n\n' "$sql" >> "$COMMAND_LOG"
    log "SHADOW SQL:"
    printf '%s\n' "$sql" | tee -a "$RUN_LOG"
    shadow_q "$sql" | tee -a "$RUN_LOG"
}

SAMPLE_NUMBER=0
sample_state() {
    local phase="$1"
    local src shadow source_lag=""
    local src_current="" src_checkpoint="" src_redo=""
    local shadow_replay="" shadow_checkpoint="" shadow_redo="" recovery_gap=""
    local rp_timed="" rp_req="" rp_done=""

    src="$(source_q "
        SELECT pg_current_wal_insert_lsn()::text,
               checkpoint_lsn::text,
               redo_lsn::text
        FROM pg_control_checkpoint();
    " || true)"

    if [[ -n "$src" ]]; then
        IFS='|' read -r src_current src_checkpoint src_redo <<< "$src"
    fi

    shadow="$(shadow_q "
        SELECT pg_last_wal_replay_lsn()::text,
               c.checkpoint_lsn::text,
               c.redo_lsn::text,
               GREATEST(pg_wal_lsn_diff(pg_last_wal_replay_lsn(), c.redo_lsn), 0)::bigint,
               s.restartpoints_timed,
               s.restartpoints_req,
               s.restartpoints_done
        FROM pg_control_checkpoint() AS c
        CROSS JOIN pg_stat_checkpointer AS s;
    " || true)"

    if [[ -n "$shadow" ]]; then
        IFS='|' read -r shadow_replay shadow_checkpoint shadow_redo recovery_gap rp_timed rp_req rp_done <<< "$shadow"
    fi

    if [[ -n "$src_current" && -n "$shadow_replay" ]]; then
        source_lag="$(source_q "
            SELECT GREATEST(pg_wal_lsn_diff('$src_current'::pg_lsn, '$shadow_replay'::pg_lsn), 0)::bigint;
        " || true)"
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$(timestamp)" "$phase" "$src_current" "$src_checkpoint" "$src_redo" \
        "$shadow_replay" "$shadow_checkpoint" "$shadow_redo" "$source_lag" \
        "$recovery_gap" "$rp_timed" "$rp_req" "$rp_done" >> "$SAMPLES"

    SAMPLE_NUMBER=$((SAMPLE_NUMBER + 1))
    curl -fsS http://localhost:9484/metrics \
        > "$RUN_DIR/metrics-${SAMPLE_NUMBER}-${phase}.prom" 2>/dev/null || true

    log "Sample [$phase]"
    log "  source current     = ${src_current:-unknown}"
    log "  source checkpoint  = ${src_checkpoint:-unknown}"
    log "  shadow replay      = ${shadow_replay:-unknown}"
    log "  shadow checkpoint  = ${shadow_checkpoint:-unknown}"
    [[ -n "$source_lag" ]] && log "  CDC/shadow lag     = $(human_mib "$source_lag")"
    [[ -n "$recovery_gap" ]] && log "  recovery gap       = $(human_mib "$recovery_gap")"
    log "  restartpoints      = req=${rp_req:-?} done=${rp_done:-?}"
}

switch_source_wal() {
    record_command "SOURCE SQL: SELECT pg_switch_wal();"
    log "Requesting source WAL segment switch."
    local output
    if output="$(source_q "SELECT pg_switch_wal();")"; then
        log "pg_switch_wal() -> $output"
    else
        log "WARNING: pg_switch_wal() unavailable; continuing."
    fi
}

wait_shadow_to_lsn() {
    local target="$1" timeout="${2:-$CATCHUP_TIMEOUT_SECS}"
    local start now replay diff state
    local last_replay="" last_progress last_log
    start="$(date +%s)"
    last_progress="$start"
    last_log=0
    WAIT_FAILURE_REASON=""
    log "Waiting for shadow replay to reach $target"

    while true; do
        if ! state="$(shadow_q "
            SELECT pg_last_wal_replay_lsn()::text,
                   pg_wal_lsn_diff('$target'::pg_lsn, pg_last_wal_replay_lsn())::bigint;
        ")"; then
            state=""
            if ! compose_service_running; then
                record_stopped_service "waiting for shadow replay to reach $target"
                return 3
            fi
        fi
        replay=""
        diff=""
        [[ -n "$state" ]] && IFS='|' read -r replay diff <<< "$state"
        now="$(date +%s)"

        if valid_lsn "$replay"; then
            if [[ "$replay" != "$last_replay" ]]; then
                last_replay="$replay"
                last_progress="$now"
            fi
            if [[ -n "$diff" && "$diff" =~ ^-?[0-9]+$ ]]; then
                if (( diff <= 0 )); then
                    log "Shadow reached target: replay=$replay target=$target"
                    return 0
                fi
                if (( now - last_log >= PROGRESS_LOG_INTERVAL_SECS )); then
                    log "  remaining: $(human_mib "$diff") (shadow=$replay)"
                    last_log="$now"
                fi
            fi
        fi

        if (( now - last_progress >= STALL_TIMEOUT_SECS )); then
            WAIT_FAILURE_REASON="Shadow replay stalled at ${replay:-unknown} for ${STALL_TIMEOUT_SECS}s while waiting for $target. Check wait-stall-${CURRENT_PHASE}.status and .log."
            ctl_q status > "$RUN_DIR/wait-stall-${CURRENT_PHASE}.status" 2>&1 || true
            "${COMPOSE[@]}" logs --no-color --tail 200 "$WALSHADOW_SERVICE" \
                > "$RUN_DIR/wait-stall-${CURRENT_PHASE}.log" 2>&1 || true
            return 2
        fi
        if (( now - start >= timeout )); then
            WAIT_FAILURE_REASON="Shadow did not reach $target within ${timeout}s; last replay was ${replay:-unknown}."
            return 1
        fi
        sleep 1
    done
}

wait_ctl_lsn() {
    local field="$1" target="$2" timeout="${3:-$CDC_CATCHUP_TIMEOUT_SECS}"
    local start now value="" last_value="" last_log=0
    start="$(date +%s)"
    log "Waiting for WalShadow $field to reach $target"

    while true; do
        if ! value="$(ctl_field_q "$field")"; then
            value=""
            if ! compose_service_running; then
                record_stopped_service "waiting for $field to reach $target"
                return 3
            fi
        fi
        now="$(date +%s)"
        if valid_lsn "$value"; then
            if replay_reached_lsn "$value" "$target"; then
                log "WalShadow $field reached target: $value"
                return 0
            fi
            if [[ "$value" != "$last_value" ]]; then
                last_value="$value"
                if (( now - last_log >= PROGRESS_LOG_INTERVAL_SECS )); then
                    log "  $field progress: $value (target=$target)"
                    last_log="$now"
                fi
            fi
        fi
        if (( now - start >= timeout )); then
            WAIT_FAILURE_REASON="WalShadow $field did not reach $target within ${timeout}s; last value was ${value:-unknown}."
            return 1
        fi
        sleep 2
    done
}

capture_drained_pre_crash_state() {
    local deadline paused pause_consumed active queue_depth replay
    local last_replay="" stable_samples=0

    record_command "walshadow-stream ctl pause"
    ctl_q pause >> "$RUN_LOG"
    STREAM_PAUSED_BY_HARNESS=1
    deadline=$(( $(date +%s) + STALL_TIMEOUT_SECS ))
    while true; do
        paused="$(ctl_field_q "paused" || true)"
        pause_consumed="$(ctl_field_q "pause_consumed_lsn" || true)"
        if [[ "$paused" == "true" && "$pause_consumed" != "0/0" ]] && valid_lsn "$pause_consumed"; then
            break
        fi
        if ! compose_service_running; then
            record_stopped_service "waiting for the crash-boundary pause"
            return 3
        fi
        if (( $(date +%s) >= deadline )); then
            WAIT_FAILURE_REASON="WalShadow did not enter paused state within ${STALL_TIMEOUT_SECS}s."
            return 1
        fi
        sleep 1
    done

    PAUSE_CONSUMED_LSN="$pause_consumed"

    # Re-check this after the pause is active. pause_consumed_lsn may include
    # filtered/non-record trailing bytes, but every committed benchmark row
    # must already be durably acknowledged downstream.
    wait_ctl_lsn "emitter_ack" "$WORKLOAD_END_LSN" || return $?

    # Do not wait for PostgreSQL to reach pause_consumed_lsn: that cursor can be
    # a few filtered bytes beyond the last shadow-replayable record. Instead,
    # prove every accepted transaction and queued record has drained. Boundary
    # descriptors are fsynced before they are published to this pipeline, so a
    # stable empty pipeline is the durable crash boundary. Require the actual
    # PostgreSQL replay LSN to stay unchanged for multiple samples too.
    deadline=$(( $(date +%s) + STALL_TIMEOUT_SECS ))
    while true; do
        paused="$(ctl_field_q "paused" || true)"
        active="$(metrics_field_q "walshadow_xact_active" || true)"
        queue_depth="$(metrics_field_q "walshadow_pump_queue_depth" || true)"
        replay="$(shadow_replay_q || true)"
        if [[ "$paused" == "true" && "$active" == "0" && "$queue_depth" == "0" ]] && valid_lsn "$replay"; then
            if [[ "$replay" == "$last_replay" ]]; then
                stable_samples=$((stable_samples + 1))
            else
                last_replay="$replay"
                stable_samples=1
            fi
            (( stable_samples >= PRE_CRASH_STABLE_SAMPLES )) && break
        else
            stable_samples=0
            last_replay=""
        fi
        if ! compose_service_running; then
            record_stopped_service "draining the paused crash boundary"
            return 3
        fi
        if (( $(date +%s) >= deadline )); then
            WAIT_FAILURE_REASON="Paused crash boundary did not become drained and replay-stable within ${STALL_TIMEOUT_SECS}s (paused=${paused:-unknown}, xact_active=${active:-unknown}, pump_queue_depth=${queue_depth:-unknown}, shadow_replay=${replay:-unknown}, stable_samples=$stable_samples)."
            return 1
        fi
        sleep 1
    done

    PRE_CRASH_STATE="$(shadow_q "
        SELECT c.checkpoint_lsn::text,
               c.redo_lsn::text,
               pg_last_wal_replay_lsn()::text,
               pg_wal_lsn_diff(pg_last_wal_replay_lsn(), c.redo_lsn)::bigint,
               pg_wal_lsn_diff('$PAUSE_CONSUMED_LSN'::pg_lsn, pg_last_wal_replay_lsn())::bigint
        FROM pg_control_checkpoint() AS c;
    ")"
    IFS='|' read -r PRE_CRASH_CP PRE_CRASH_REDO PRE_CRASH_REPLAY PRE_CRASH_GAP PAUSE_MINUS_SHADOW_BYTES <<< "$PRE_CRASH_STATE"
    valid_lsn "$PRE_CRASH_REPLAY" || return 1
    [[ "$PAUSE_MINUS_SHADOW_BYTES" =~ ^-?[0-9]+$ ]] || return 1
    if [[ "$PRE_CRASH_REPLAY" != "$last_replay" ]]; then
        WAIT_FAILURE_REASON="Shadow replay changed after the drained stability window ($last_replay -> $PRE_CRASH_REPLAY)."
        return 1
    fi
    log "Drained crash boundary: pause_consumed=$PAUSE_CONSUMED_LSN shadow_replay=$PRE_CRASH_REPLAY pause_minus_shadow_bytes=$PAUSE_MINUS_SHADOW_BYTES"
}

collect_final_artifacts() {
    log "Collecting final WalShadow artifacts."
    "${COMPOSE[@]}" logs --no-color "$WALSHADOW_SERVICE" > "$RUN_DIR/walshadow.log" 2>&1 || true
    "${COMPOSE[@]}" exec -T "$WALSHADOW_SERVICE" walshadow-stream ctl status \
        > "$RUN_DIR/walshadow-status-final.txt" 2>&1 || true
    "${COMPOSE[@]}" exec -T "$WALSHADOW_SERVICE" \
        sh -c 'test ! -f /var/lib/walshadow/shadow-data/startup.log || cat /var/lib/walshadow/shadow-data/startup.log' \
        > "$RUN_DIR/shadow-startup.log" 2>&1 || true
}

write_failure_result() {
    local failure_target_reached=0
    if (( ${GENERATED_BYTES:-0} >= TARGET_WAL_MB * 1024 * 1024 )); then
        failure_target_reached=1
    fi
    {
        printf 'RUN_ID=%q\n' "$RUN_ID"
        printf 'RUN_DIR=%q\n' "$RUN_DIR"
        printf 'RUN_STATUS=failed\n'
        printf 'RUN_VALID=0\n'
        printf 'FAILURE_PHASE=%q\n' "$CURRENT_PHASE"
        printf 'FAILURE_REASON=%q\n' "${FAILURE_REASON:-unknown failure}"
        printf 'TARGET_WAL_MB=%q\n' "$TARGET_WAL_MB"
        printf 'TARGET_REACHED=%q\n' "$failure_target_reached"
        printf 'EFFECTIVE_MAX_BATCHES=%q\n' "$MAX_BATCHES"
        printf 'BATCHES_EXECUTED=%q\n' "${BATCHES_EXECUTED:-0}"
        printf 'GENERATED_BYTES=%q\n' "${GENERATED_BYTES:-0}"
        printf 'GENERATED_MIB=%q\n' "$(awk -v b="${GENERATED_BYTES:-0}" 'BEGIN { printf "%.3f", b / 1048576 }')"
        printf 'PRE_CRASH_GAP_BYTES=%q\n' "${PRE_CRASH_GAP:-0}"
        printf 'PRE_CRASH_GAP_MIB=%q\n' "$(awk -v b="${PRE_CRASH_GAP:-0}" 'BEGIN { printf "%.3f", b / 1048576 }')"
        printf 'PAUSE_CONSUMED_LSN=%q\n' "${PAUSE_CONSUMED_LSN:-not-run}"
        printf 'PAUSE_MINUS_SHADOW_BYTES=%q\n' "${PAUSE_MINUS_SHADOW_BYTES:-0}"
        printf 'PRE_CRASH_REPLAY=%q\n' "${PRE_CRASH_REPLAY:-not-run}"
        printf 'FIRST_REPLAY_AFTER_RESTART=%q\n' "${FIRST_REPLAY_AFTER_RESTART:-not-run}"
        printf 'SHADOW_QUERYABLE_SECS=%q\n' "${SHADOW_QUERYABLE_SECS:-not-run}"
        printf 'REPLAY_AFTER_QUERYABLE_SECS=%q\n' "${REPLAY_AFTER_QUERYABLE_SECS:-not-run}"
        printf 'TOTAL_SHADOW_RECOVERY_SECS=%q\n' "${TOTAL_SHADOW_RECOVERY_SECS:-not-run}"
        printf 'SHADOW_RECOVERY_UPPER_BOUND_SECS=%q\n' "${SHADOW_RECOVERY_UPPER_BOUND_SECS:-not-run}"
        printf 'CDC_TOTAL_RECOVERY_SECS=%q\n' "${CDC_TOTAL_RECOVERY_SECS:-not-run}"
        printf 'SOURCE_CHECKPOINT_CHANGED=%q\n' "${SOURCE_CHECKPOINT_CHANGED:-not-run}"
        printf 'NOCP_REQ_DELTA=%q\n' "${REQ_DELTA:-0}"
        printf 'NOCP_DONE_DELTA=%q\n' "${DONE_DELTA:-0}"
        printf 'REQ_DELTA_WITHOUT_SOURCE_CP=%q\n' "${REQ_DELTA:-0}"
        printf 'DONE_DELTA_WITHOUT_SOURCE_CP=%q\n' "${DONE_DELTA:-0}"
        printf 'UNLOCK_REQ_DELTA=%q\n' "${UNLOCK_REQ_DELTA:-0}"
        printf 'UNLOCK_DONE_DELTA=%q\n' "${UNLOCK_DONE_DELTA:-0}"
        printf 'UNLOCK_CP_ADVANCED=%q\n' "${UNLOCK_CP_ADVANCED:-0}"
        printf 'UNLOCK_REDO_ADVANCED=%q\n' "${UNLOCK_REDO_ADVANCED:-0}"
    } > "$RESULT_ENV"
}

on_error() {
    local rc="$1" line="$2"
    [[ -n "$FAILURE_REASON" ]] || FAILURE_REASON="Command failed in $CURRENT_PHASE at line $line (exit $rc)."
    return "$rc"
}

on_exit() {
    local rc="$?"
    trap - ERR EXIT
    set +e
    if (( STREAM_PAUSED_BY_HARNESS == 1 )) && compose_service_running; then
        ctl_q resume >> "$RUN_LOG" 2>> "$ERROR_LOG"
        STREAM_PAUSED_BY_HARNESS=0
    fi
    if (( rc != 0 )); then
        RUN_STATUS="failed"
        write_failure_result
        log "Run failed in $CURRENT_PHASE: ${FAILURE_REASON:-unknown failure}"
        log "Machine-readable failure: $RESULT_ENV"
    fi
    collect_final_artifacts
    exit "$rc"
}

trap 'on_error "$?" "$LINENO"' ERR
trap on_exit EXIT

# Defaults so summary generation is safe even if crash phase is disabled.
SHADOW_QUERYABLE_SECS="not-run"
REPLAY_AFTER_QUERYABLE_SECS="not-run"
TOTAL_SHADOW_RECOVERY_SECS="not-run"
SHADOW_RECOVERY_UPPER_BOUND_SECS="not-run"
CDC_TOTAL_RECOVERY_SECS="not-run"
SHADOW_RECOVERY_INTERPRETATION="Crash phase was not run."
FIRST_REPLAY_AFTER_RESTART="not-run"
POST_RECOVERY_CP="not-run"
POST_RECOVERY_REDO="not-run"
PRE_CRASH_REPLAY="not-run"
PRE_CRASH_CP="not-run"
PRE_CRASH_REDO="not-run"
PRE_CRASH_GAP="0"
PAUSE_CONSUMED_LSN="not-run"
PAUSE_MINUS_SHADOW_BYTES="0"
POST_RECOVERY_REPLAY="not-run"
UNLOCK_CP_ADVANCED=0
UNLOCK_REDO_ADVANCED=0

# ----------------------------- phase 0 ----------------------------------------

log "============================================================"
log "WalShadow checkpoint/crash lab"
log "Run ID: $RUN_ID"
log "Target WAL: ${TARGET_WAL_MB} MiB"
log "Maximum workload batches: $MAX_BATCHES"
log "Crash test: $RUN_CRASH_TEST"
log "Artifacts: $RUN_DIR"
log "============================================================"

record_command "git rev-parse HEAD"
git rev-parse HEAD > "$RUN_DIR/walshadow-git-commit.txt" 2>>"$ERROR_LOG" || true
record_command "git status --short"
git status --short > "$RUN_DIR/walshadow-git-status.txt" 2>>"$ERROR_LOG" || true
record_command "docker compose -f $COMPOSE_FILE ps"
"${COMPOSE[@]}" ps | tee "$RUN_DIR/docker-compose-ps.txt" >> "$RUN_LOG"
record_command "walshadow-stream ctl status"
"${COMPOSE[@]}" exec -T "$WALSHADOW_SERVICE" walshadow-stream ctl status \
    | tee "$RUN_DIR/walshadow-status-start.txt" >> "$RUN_LOG"

# ----------------------------- phase 1 ----------------------------------------

CURRENT_PHASE="preflight"
log "PHASE 1: preflight"
[[ "$(source_q "SELECT pg_is_in_recovery();")" == "f" ]] || die "Source PostgreSQL is unexpectedly in recovery."
[[ "$(source_q "SELECT to_regclass('$TEST_TABLE') IS NOT NULL;")" == "t" ]] || die "Test table $TEST_TABLE does not exist."
command -v python3 >/dev/null 2>&1 || die "python3 is required for portable millisecond timing on macOS."

WALSHADOW_SHOW="$(ctl_q show || true)"
SOURCE_SLOT="$(printf '%s\n' "$WALSHADOW_SHOW" | awk '
    /^\[source\]$/ { source = 1; next }
    /^\[/ { source = 0 }
    source && /^slot[[:space:]]*=/ {
        sub(/^[^=]*=[[:space:]]*/, "")
        gsub(/^"|"$/, "")
        print
        exit
    }
')"
if [[ -z "$SOURCE_SLOT" ]]; then
    if [[ "$ALLOW_UNSLOTTED_SOURCE" != "1" ]]; then
        die "No physical replication slot is configured for WalShadow. A crash matrix can outrun source WAL retention and permanently wedge the stream. Configure [source] slot and re-bootstrap before running, or set ALLOW_UNSLOTTED_SOURCE=1 only when a tested WAL archive covers the full matrix."
    fi
    log "WARNING: running without a physical source slot; relying on operator-confirmed WAL archive retention."
else
    [[ "$SOURCE_SLOT" =~ ^[A-Za-z0-9_-]+$ ]] || die "Source slot name contains characters this benchmark cannot validate safely: $SOURCE_SLOT"
    SLOT_STATE="$(source_q "
        SELECT slot_type, COALESCE(restart_lsn::text, ''), wal_status
        FROM pg_replication_slots
        WHERE slot_name = '$SOURCE_SLOT';
    ")"
    [[ -n "$SLOT_STATE" ]] || die "Configured source slot $SOURCE_SLOT does not exist. Re-bootstrap WalShadow so it creates the slot before generating benchmark WAL."
    IFS='|' read -r SLOT_TYPE SLOT_RESTART_LSN SLOT_WAL_STATUS <<< "$SLOT_STATE"
    [[ "$SLOT_TYPE" == "physical" ]] || die "Configured source slot $SOURCE_SLOT is $SLOT_TYPE, not physical."
    valid_lsn "$SLOT_RESTART_LSN" || die "Physical slot $SOURCE_SLOT has no restart_lsn and is not protecting WAL."
    [[ "$SLOT_WAL_STATUS" == "reserved" || "$SLOT_WAL_STATUS" == "extended" ]] || die "Physical slot $SOURCE_SLOT wal_status is $SLOT_WAL_STATUS; required WAL may already be unavailable."
    log "Physical source slot $SOURCE_SLOT protects WAL from $SLOT_RESTART_LSN (status=$SLOT_WAL_STATUS)."
fi
wait_walshadow_preflight_ready \
    || die "${WAIT_FAILURE_REASON:-WalShadow control state did not become initialized and caught up.}"
log "Preflight passed."

# ----------------------------- phase 2 ----------------------------------------

CURRENT_PHASE="record_settings"
log "PHASE 2: record settings"
source_q "
SELECT version();
SELECT name, setting, unit, source
FROM pg_settings
WHERE name IN ('checkpoint_timeout','max_wal_size','min_wal_size','checkpoint_completion_target','wal_level','max_wal_senders')
ORDER BY name;
" > "$RUN_DIR/source-settings.txt"

shadow_q "
SELECT version();
SELECT name, setting, unit, source
FROM pg_settings
WHERE name IN ('checkpoint_timeout','max_wal_size','min_wal_size','checkpoint_completion_target','wal_level')
ORDER BY name;
SELECT * FROM pg_stat_checkpointer;
SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), checkpoint_lsn, redo_lsn, checkpoint_time
FROM pg_control_checkpoint();
" > "$RUN_DIR/shadow-settings.txt"

sample_state "initial"

# ----------------------------- phase 3 ----------------------------------------

CURRENT_PHASE="baseline_source_checkpoint"
log "PHASE 3: establish source checkpoint baseline"
source_sql "CHECKPOINT;"
BASE_SOURCE_CP="$(source_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
BASE_SOURCE_REDO="$(source_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
log "Baseline source checkpoint = $BASE_SOURCE_CP"
log "Baseline source redo       = $BASE_SOURCE_REDO"
switch_source_wal
wait_shadow_to_lsn "$BASE_SOURCE_CP" || die "${WAIT_FAILURE_REASON:-Shadow failed to replay baseline source checkpoint.}"
sample_state "source_checkpoint_replayed"

# ----------------------------- phase 4 ----------------------------------------

CURRENT_PHASE="baseline_shadow_restartpoint"
log "PHASE 4: establish shadow restartpoint baseline"
BASE_RP_REQ="$(shadow_q "SELECT restartpoints_req FROM pg_stat_checkpointer;")"
BASE_RP_DONE="$(shadow_q "SELECT restartpoints_done FROM pg_stat_checkpointer;")"
shadow_sql "CHECKPOINT;"
sleep 1
BASE_SHADOW_CP="$(shadow_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
BASE_SHADOW_REDO="$(shadow_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
log "Baseline shadow checkpoint = $BASE_SHADOW_CP"
log "Baseline shadow redo       = $BASE_SHADOW_REDO"
sample_state "baseline_restartpoint"

# ----------------------------- phase 5 ----------------------------------------

CURRENT_PHASE="generate_workload"
log "PHASE 5: generate CDC workload without source checkpoint"
WAL_START="$(source_q "SELECT pg_current_wal_insert_lsn()::text;")"
TARGET_WAL_BYTES=$((TARGET_WAL_MB * 1024 * 1024))
GENERATED_BYTES=0
SOURCE_CHECKPOINT_CHANGED=0
BATCHES_EXECUTED=0

for ((batch=1; batch<=MAX_BATCHES; batch++)); do
    BATCHES_EXECUTED="$batch"
    log "Generating CDC batch $batch / $MAX_BATCHES"
    SQL="
SET statement_timeout = '${WORKLOAD_STATEMENT_TIMEOUT_SECS}s';
INSERT INTO $TEST_TABLE(payload)
SELECT '$RUN_TAG:$batch:' || g::text || ':' ||
       repeat(md5(random()::text || g::text || clock_timestamp()::text), $PAYLOAD_REPEAT)
FROM generate_series(1, $BATCH_ROWS) AS g;
"
    run_workload_batch "$batch" "$SQL"

    GENERATED_BYTES="$(source_q "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '$WAL_START'::pg_lsn)::bigint;")"
    CURRENT_SOURCE_CP="$(source_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
    log "Generated WAL: $(human_mib "$GENERATED_BYTES")"
    log "Source checkpoint: $CURRENT_SOURCE_CP"
    sample_state "workload_batch_${batch}"

    if [[ "$CURRENT_SOURCE_CP" != "$BASE_SOURCE_CP" ]]; then
        SOURCE_CHECKPOINT_CHANGED=1
        log "WARNING: source checkpoint changed during workload; run is not valid for the no-source-checkpoint comparison."
        break
    fi

    (( GENERATED_BYTES < TARGET_WAL_BYTES )) || break
done

WORKLOAD_END_LSN="$(source_q "SELECT pg_current_wal_insert_lsn()::text;")"
log "Workload end LSN = $WORKLOAD_END_LSN"
log "Total WAL generated = $(human_mib "$GENERATED_BYTES")"
switch_source_wal
wait_shadow_to_lsn "$WORKLOAD_END_LSN" || die "${WAIT_FAILURE_REASON:-Shadow did not catch up to workload end LSN.}"
sample_state "workload_shadow_caught_up"

# ----------------------------- phase 6 ----------------------------------------

CURRENT_PHASE="shadow_restartpoint_without_source_checkpoint"
log "PHASE 6: shadow-only restartpoint request"
NOCP_CP_BEFORE="$(shadow_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
NOCP_REDO_BEFORE="$(shadow_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
NOCP_GAP_BEFORE="$(shadow_q "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), redo_lsn)::bigint FROM pg_control_checkpoint();")"
NOCP_REQ_BEFORE="$(shadow_q "SELECT restartpoints_req FROM pg_stat_checkpointer;")"
NOCP_DONE_BEFORE="$(shadow_q "SELECT restartpoints_done FROM pg_stat_checkpointer;")"

log "Before shadow CHECKPOINT: checkpoint=$NOCP_CP_BEFORE redo=$NOCP_REDO_BEFORE gap=$(human_mib "$NOCP_GAP_BEFORE") req=$NOCP_REQ_BEFORE done=$NOCP_DONE_BEFORE"
shadow_sql "CHECKPOINT;"
sleep 2

NOCP_CP_AFTER="$(shadow_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
NOCP_REDO_AFTER="$(shadow_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
NOCP_GAP_AFTER="$(shadow_q "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), redo_lsn)::bigint FROM pg_control_checkpoint();")"
NOCP_REQ_AFTER="$(shadow_q "SELECT restartpoints_req FROM pg_stat_checkpointer;")"
NOCP_DONE_AFTER="$(shadow_q "SELECT restartpoints_done FROM pg_stat_checkpointer;")"
REQ_DELTA=$((NOCP_REQ_AFTER - NOCP_REQ_BEFORE))
DONE_DELTA=$((NOCP_DONE_AFTER - NOCP_DONE_BEFORE))
log "After shadow CHECKPOINT: checkpoint=$NOCP_CP_AFTER redo=$NOCP_REDO_AFTER gap=$(human_mib "$NOCP_GAP_AFTER") req_delta=$REQ_DELTA done_delta=$DONE_DELTA"
sample_state "shadow_checkpoint_without_source_checkpoint"

# ----------------------------- phase 7 ----------------------------------------

if [[ "$RUN_CRASH_TEST" == "1" ]]; then
    CURRENT_PHASE="crash_recovery"
    log "PHASE 7: hard WalShadow crash/recovery"

    sample_state "immediately_before_hard_crash"

    # Start from a clean downstream baseline. The workload-end LSN follows the
    # last committed benchmark batch, so emitter_ack reaching it proves that
    # ClickHouse had acknowledged the test workload before the crash.
    wait_ctl_lsn "emitter_ack" "$WORKLOAD_END_LSN" \
        || die "${WAIT_FAILURE_REASON:-WalShadow emitter did not drain before crash.}"

    # Ensure the durable descriptor/emitter frontier also covers the exact
    # shadow replay point used as the crash target. This avoids manufacturing
    # a descriptor-log hole when a segment switch or other non-transaction WAL
    # has replayed just beyond the last workload commit.
    capture_drained_pre_crash_state \
        || die "${WAIT_FAILURE_REASON:-Could not capture a drained pre-crash state.}"
    valid_lsn "$PRE_CRASH_REPLAY" || die "Could not capture a valid pre-crash replay LSN."

    record_command "docker compose -f $COMPOSE_FILE kill -s KILL $WALSHADOW_SERVICE"
    CRASH_T0="$(now_ms)"
    "${COMPOSE[@]}" kill -s KILL "$WALSHADOW_SERVICE" | tee -a "$RUN_LOG"

    # Start the exact container that was killed. `up -d` may recreate it when
    # Compose detects config drift, measuring image/container setup instead of
    # PostgreSQL recovery (43s in the original reproduction).
    record_command "docker compose -f $COMPOSE_FILE start $WALSHADOW_SERVICE"
    "${COMPOSE[@]}" start "$WALSHADOW_SERVICE" | tee -a "$RUN_LOG"

    # The first successful query is itself the recovery observation. Do not put
    # SELECT 1, metrics, checkpoint state, or sample_state ahead of it.
    deadline_ms=$(( CRASH_T0 + CATCHUP_TIMEOUT_SECS * 1000 ))
    while true; do
        FIRST_REPLAY_AFTER_RESTART="$(shadow_replay_q || true)"
        if valid_lsn "$FIRST_REPLAY_AFTER_RESTART"; then
            CRASH_T1="$(now_ms)"
            break
        fi
        if ! compose_service_running; then
            record_stopped_service "waiting for the first post-crash replay query"
            die "$WAIT_FAILURE_REASON"
        fi
        (( $(now_ms) < deadline_ms )) || die "Shadow PostgreSQL did not return a replay LSN after crash."
        sleep 0.25
    done

    SHADOW_QUERYABLE_SECS="$(seconds_between_ms "$CRASH_T0" "$CRASH_T1")"
    POST_RECOVERY_REPLAY="$FIRST_REPLAY_AFTER_RESTART"

    if replay_reached_lsn "$FIRST_REPLAY_AFTER_RESTART" "$PRE_CRASH_REPLAY"; then
        # Recovery reached the target before PostgreSQL could answer our first
        # query. T1 is therefore an upper bound, and there is no separately
        # observable post-query replay interval.
        CRASH_T2="$CRASH_T1"
        REPLAY_AFTER_QUERYABLE_SECS="0.000"
        SHADOW_RECOVERY_UPPER_BOUND_SECS="$SHADOW_QUERYABLE_SECS"
        SHADOW_RECOVERY_INTERPRETATION="The shadow had already replayed to or past the pre-crash target by the first successful query, so queryable time is an upper bound on WAL recovery time."
    else
        while true; do
            replay="$(shadow_replay_q || true)"
            if valid_lsn "$replay"; then
                replay_observed_ms="$(now_ms)"
                if replay_reached_lsn "$replay" "$PRE_CRASH_REPLAY"; then
                    CRASH_T2="$replay_observed_ms"
                    POST_RECOVERY_REPLAY="$replay"
                    break
                fi
            fi
            if ! compose_service_running; then
                record_stopped_service "waiting for replay to regain the pre-crash LSN"
                die "$WAIT_FAILURE_REASON"
            fi
            (( $(now_ms) < deadline_ms )) || die "Shadow did not recover to pre-crash replay LSN."
            sleep 0.25
        done
        REPLAY_AFTER_QUERYABLE_SECS="$(seconds_between_ms "$CRASH_T1" "$CRASH_T2")"
        SHADOW_RECOVERY_UPPER_BOUND_SECS="not-applicable"
        SHADOW_RECOVERY_INTERPRETATION="PostgreSQL became queryable before replay regained the pre-crash LSN; total time is directly observed at polling resolution."
    fi

    TOTAL_SHADOW_RECOVERY_SECS="$(seconds_between_ms "$CRASH_T0" "$CRASH_T2")"

    # Pause is persisted, so explicitly reopen source consumption after the
    # shadow timing target has been observed. Include this in end-to-end CDC
    # recovery, but never in PostgreSQL recovery time.
    record_command "walshadow-stream ctl resume"
    ctl_q resume >> "$RUN_LOG"
    STREAM_PAUSED_BY_HARNESS=0

    # This is intentionally outside the shadow recovery hot path. A restarted
    # daemon may need to re-read from its durable floor after PostgreSQL is
    # already queryable. `emitter_ack` is the contiguous ClickHouse-done
    # watermark, so reaching PRE_CRASH_REPLAY is an unambiguous downstream
    # recovery target for this segment-switched, fully committed workload.
    wait_ctl_lsn "emitter_ack" "$PRE_CRASH_REPLAY" \
        || die "${WAIT_FAILURE_REASON:-WalShadow CDC pipeline did not regain the pre-crash target.}"
    CDC_RECOVERY_T1="$(now_ms)"
    CDC_TOTAL_RECOVERY_SECS="$(seconds_between_ms "$CRASH_T0" "$CDC_RECOVERY_T1")"

    # Recovery timing is complete. Additional state and metrics can no longer
    # contaminate it.
    POST_RECOVERY_CP="$(shadow_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
    POST_RECOVERY_REDO="$(shadow_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
    log "Pre-crash state: checkpoint=$PRE_CRASH_CP redo=$PRE_CRASH_REDO replay=$PRE_CRASH_REPLAY gap=$(human_mib "$PRE_CRASH_GAP")"
    log "First successful replay query: replay=$FIRST_REPLAY_AFTER_RESTART queryable=${SHADOW_QUERYABLE_SECS}s"
    log "Post-recovery state: checkpoint=$POST_RECOVERY_CP redo=$POST_RECOVERY_REDO replay=$POST_RECOVERY_REPLAY"
    log "Crash recovery complete: queryable=${SHADOW_QUERYABLE_SECS}s replay_after_queryable=${REPLAY_AFTER_QUERYABLE_SECS}s total=${TOTAL_SHADOW_RECOVERY_SECS}s"
    log "Full CDC recovery to pre-crash replay: ${CDC_TOTAL_RECOVERY_SECS}s"
    sample_state "after_hard_crash_recovery"
else
    log "PHASE 7 skipped: RUN_CRASH_TEST=$RUN_CRASH_TEST"
fi

# ----------------------------- phase 8 ----------------------------------------

CURRENT_PHASE="unlock_source_checkpoint"
log "PHASE 8: create new source checkpoint"
source_sql "CHECKPOINT;"
NEW_SOURCE_CP="$(source_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
NEW_SOURCE_REDO="$(source_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
log "New source checkpoint=$NEW_SOURCE_CP redo=$NEW_SOURCE_REDO"
switch_source_wal
wait_shadow_to_lsn "$NEW_SOURCE_CP" || die "${WAIT_FAILURE_REASON:-Shadow did not replay the new source checkpoint.}"
sample_state "new_source_checkpoint_replayed"

# ----------------------------- phase 9 ----------------------------------------

CURRENT_PHASE="unlock_shadow_restartpoint"
log "PHASE 9: request shadow restartpoint after new source checkpoint"
UNLOCK_REQ_BEFORE="$(shadow_q "SELECT restartpoints_req FROM pg_stat_checkpointer;")"
UNLOCK_DONE_BEFORE="$(shadow_q "SELECT restartpoints_done FROM pg_stat_checkpointer;")"
UNLOCK_CP_BEFORE="$(shadow_q "SELECT checkpoint_lsn::text FROM pg_control_checkpoint();")"
UNLOCK_REDO_BEFORE="$(shadow_q "SELECT redo_lsn::text FROM pg_control_checkpoint();")"
shadow_sql "CHECKPOINT;"
unlock_deadline=$(( $(date +%s) + UNLOCK_TIMEOUT_SECS ))
unlock_counter_grace_deadline=0
while true; do
    UNLOCK_STATE="$(shadow_q "
        SELECT c.checkpoint_lsn::text,
               c.redo_lsn::text,
               pg_wal_lsn_diff(pg_last_wal_replay_lsn(), c.redo_lsn)::bigint,
               s.restartpoints_req,
               s.restartpoints_done
        FROM pg_control_checkpoint() AS c
        CROSS JOIN pg_stat_checkpointer AS s;
    ")"
    IFS='|' read -r UNLOCK_CP_AFTER UNLOCK_REDO_AFTER UNLOCK_GAP_AFTER UNLOCK_REQ_AFTER UNLOCK_DONE_AFTER <<< "$UNLOCK_STATE"
    UNLOCK_REQ_DELTA=$((UNLOCK_REQ_AFTER - UNLOCK_REQ_BEFORE))
    UNLOCK_DONE_DELTA=$((UNLOCK_DONE_AFTER - UNLOCK_DONE_BEFORE))
    lsn_strictly_after "$UNLOCK_CP_AFTER" "$UNLOCK_CP_BEFORE" && UNLOCK_CP_ADVANCED=1
    lsn_strictly_after "$UNLOCK_REDO_AFTER" "$UNLOCK_REDO_BEFORE" && UNLOCK_REDO_ADVANCED=1

    now="$(date +%s)"
    if (( UNLOCK_CP_ADVANCED == 1 && UNLOCK_REDO_ADVANCED == 1 )); then
        (( UNLOCK_DONE_DELTA > 0 )) && break
        if (( unlock_counter_grace_deadline == 0 )); then
            unlock_counter_grace_deadline=$(( now + UNLOCK_COUNTER_GRACE_SECS ))
        fi
        (( now >= unlock_counter_grace_deadline )) && break
    fi
    (( now >= unlock_deadline )) && break
    sleep 1
done
log "After new source CP: shadow_cp=$UNLOCK_CP_AFTER redo=$UNLOCK_REDO_AFTER gap=$(human_mib "$UNLOCK_GAP_AFTER") checkpoint_advanced=$UNLOCK_CP_ADVANCED redo_advanced=$UNLOCK_REDO_ADVANCED req_delta=$UNLOCK_REQ_DELTA done_delta=$UNLOCK_DONE_DELTA"
sample_state "shadow_checkpoint_after_source_checkpoint"

# ----------------------------- phase 10 ---------------------------------------

CURRENT_PHASE="validate_workload"
log "PHASE 10: validate workload"
SOURCE_TEST_ROWS="$(source_q "SELECT count(*) FROM $TEST_TABLE WHERE payload LIKE '$RUN_TAG:%';")"
log "Source rows for run = $SOURCE_TEST_ROWS"
CH_RESULT="not-configured"

if [[ -n "${CH_HTTP_URL:-}" && -n "${CH_HTTP_USER:-}" && -n "${CH_HTTP_PASSWORD:-}" && -n "${CH_TABLE:-}" ]]; then
    log "ClickHouse row validation enabled for $CH_TABLE"
    deadline=$(( $(date +%s) + 120 ))
    while true; do
        CH_RESULT="$(curl -fsS --user "${CH_HTTP_USER}:${CH_HTTP_PASSWORD}" \
            --data-binary "SELECT count() FROM $CH_TABLE FINAL WHERE startsWith(payload, '$RUN_TAG:')" \
            "$CH_HTTP_URL" 2>>"$ERROR_LOG" || true)"
        if [[ "$CH_RESULT" =~ ^[0-9]+$ ]] && (( CH_RESULT == SOURCE_TEST_ROWS )); then
            break
        fi
        (( $(date +%s) < deadline )) || break
        sleep 2
    done
    log "ClickHouse rows for run = ${CH_RESULT:-unknown}"
else
    log "ClickHouse row validation skipped."
fi

# ----------------------------- phase 11 ---------------------------------------

CURRENT_PHASE="write_results"

if (( GENERATED_BYTES >= TARGET_WAL_BYTES )); then
    TARGET_REACHED=1
else
    TARGET_REACHED=0
fi

if [[ "$SOURCE_CHECKPOINT_CHANGED" == "0" ]]; then
    SOURCE_CP_RESULT="UNCHANGED during CDC workload"
else
    SOURCE_CP_RESULT="CHANGED during CDC workload"
fi

if (( SOURCE_CHECKPOINT_CHANGED == 0 && TARGET_REACHED == 1 )); then
    RUN_VALID="1"
else
    RUN_VALID="0"
fi

if (( SOURCE_CHECKPOINT_CHANGED == 0 && REQ_DELTA > 0 && DONE_DELTA == 0 )); then
    NOCP_RESULT="STRONG: restartpoint requested but not completed while source checkpoint remained unchanged."
elif (( SOURCE_CHECKPOINT_CHANGED == 0 && DONE_DELTA > 0 )); then
    NOCP_RESULT="A restartpoint completed without an observed source control-file checkpoint change; inspect timing before concluding."
else
    NOCP_RESULT="INCONCLUSIVE."
fi

if (( UNLOCK_CP_ADVANCED == 1 && UNLOCK_REDO_ADVANCED == 1 && UNLOCK_DONE_DELTA > 0 )); then
    UNLOCK_RESULT="STRONG: shadow checkpoint and redo LSNs advanced; restartpoints_done also increased."
elif (( UNLOCK_CP_ADVANCED == 1 && UNLOCK_REDO_ADVANCED == 1 )); then
    UNLOCK_RESULT="Shadow checkpoint and redo LSNs advanced after the new source checkpoint; restartpoints_done did not change during the corroboration window."
else
    UNLOCK_RESULT="No complete checkpoint+redo advancement was observed after the explicit source checkpoint."
fi

cat > "$SUMMARY" <<EOF
# WalShadow checkpoint + crash experiment

Run ID: \`$RUN_ID\`

WalShadow commit: \`$(cat "$RUN_DIR/walshadow-git-commit.txt" 2>/dev/null || echo unknown)\`

## Run validity

- valid no-source-checkpoint workload: **$RUN_VALID**
- source checkpoint during workload: **$SOURCE_CP_RESULT**
- requested WAL target reached: **$TARGET_REACHED**

## Workload

- target WAL: ${TARGET_WAL_MB} MiB
- maximum workload batches: $MAX_BATCHES
- actual WAL generated: $(human_mib "$GENERATED_BYTES")
- batches executed: $BATCHES_EXECUTED
- test rows: $SOURCE_TEST_ROWS
- initial source checkpoint: \`$BASE_SOURCE_CP\`
- workload end LSN: \`$WORKLOAD_END_LSN\`

## Shadow-only restartpoint request

- checkpoint before: \`$NOCP_CP_BEFORE\`
- redo before: \`$NOCP_REDO_BEFORE\`
- recovery gap before: $(human_mib "$NOCP_GAP_BEFORE")
- checkpoint after: \`$NOCP_CP_AFTER\`
- redo after: \`$NOCP_REDO_AFTER\`
- recovery gap after: $(human_mib "$NOCP_GAP_AFTER")
- restartpoint request delta: +$REQ_DELTA
- restartpoint completion delta: +$DONE_DELTA

**Interpretation:** $NOCP_RESULT

## Crash test

- enabled: $([[ "$RUN_CRASH_TEST" == "1" ]] && echo true || echo false)
- pre-crash checkpoint: \`$PRE_CRASH_CP\`
- pre-crash redo: \`$PRE_CRASH_REDO\`
- pause consumed LSN: \`$PAUSE_CONSUMED_LSN\`
- pre-crash replay: \`$PRE_CRASH_REPLAY\`
- pause minus shadow replay: $PAUSE_MINUS_SHADOW_BYTES bytes
- pre-crash recovery gap: $(human_mib "$PRE_CRASH_GAP")
- checkpoint observed after timed recovery: \`$POST_RECOVERY_CP\`
- redo observed after timed recovery: \`$POST_RECOVERY_REDO\`
- first replay observed after restart: \`$FIRST_REPLAY_AFTER_RESTART\`
- time until first successful replay query: ${SHADOW_QUERYABLE_SECS}s
- additional replay time after queryable: ${REPLAY_AFTER_QUERYABLE_SECS}s
- total shadow recovery time: ${TOTAL_SHADOW_RECOVERY_SECS}s
- shadow recovery upper bound: ${SHADOW_RECOVERY_UPPER_BOUND_SECS}s
- total CDC recovery time: ${CDC_TOTAL_RECOVERY_SECS}s
- final replay: \`$POST_RECOVERY_REPLAY\`

**Interpretation:** $SHADOW_RECOVERY_INTERPRETATION

## After explicit new source checkpoint

- source checkpoint: \`$NEW_SOURCE_CP\`
- shadow checkpoint before request: \`$UNLOCK_CP_BEFORE\`
- shadow redo before request: \`$UNLOCK_REDO_BEFORE\`
- shadow checkpoint after request: \`$UNLOCK_CP_AFTER\`
- shadow redo after request: \`$UNLOCK_REDO_AFTER\`
- shadow checkpoint advanced: **$UNLOCK_CP_ADVANCED**
- shadow redo advanced: **$UNLOCK_REDO_ADVANCED**
- recovery gap after request: $(human_mib "$UNLOCK_GAP_AFTER")
- restartpoint request delta: +$UNLOCK_REQ_DELTA
- restartpoint completion delta: +$UNLOCK_DONE_DELTA

**Interpretation:** $UNLOCK_RESULT

## CDC verification

- source rows: $SOURCE_TEST_ROWS
- ClickHouse rows: $CH_RESULT

## Artifact files

- \`samples.csv\`
- \`commands.log\`
- \`run.log\`
- \`walshadow.log\`
- \`source-settings.txt\`
- \`shadow-settings.txt\`
- \`metrics-*.prom\`
- \`result.env\`
EOF

cat > "$RESULT_ENV" <<EOF
RUN_ID=$RUN_ID
RUN_DIR=$RUN_DIR
RUN_STATUS=success
FAILURE_PHASE=
FAILURE_REASON=
RUN_VALID=$RUN_VALID
TARGET_WAL_MB=$TARGET_WAL_MB
TARGET_REACHED=$TARGET_REACHED
EFFECTIVE_MAX_BATCHES=$MAX_BATCHES
BATCHES_EXECUTED=$BATCHES_EXECUTED
GENERATED_BYTES=$GENERATED_BYTES
GENERATED_MIB=$(awk -v b="$GENERATED_BYTES" 'BEGIN { printf "%.3f", b / 1048576 }')
SOURCE_TEST_ROWS=$SOURCE_TEST_ROWS
SOURCE_CHECKPOINT_CHANGED=$SOURCE_CHECKPOINT_CHANGED
NOCP_GAP_BEFORE_BYTES=$NOCP_GAP_BEFORE
NOCP_GAP_AFTER_BYTES=$NOCP_GAP_AFTER
NOCP_REQ_DELTA=$REQ_DELTA
NOCP_DONE_DELTA=$DONE_DELTA
REQ_DELTA_WITHOUT_SOURCE_CP=$REQ_DELTA
DONE_DELTA_WITHOUT_SOURCE_CP=$DONE_DELTA
RUN_CRASH_TEST=$RUN_CRASH_TEST
PRE_CRASH_CP=$PRE_CRASH_CP
PRE_CRASH_REDO=$PRE_CRASH_REDO
PAUSE_CONSUMED_LSN=$PAUSE_CONSUMED_LSN
PAUSE_MINUS_SHADOW_BYTES=$PAUSE_MINUS_SHADOW_BYTES
PRE_CRASH_REPLAY=$PRE_CRASH_REPLAY
PRE_CRASH_GAP_BYTES=$PRE_CRASH_GAP
PRE_CRASH_GAP_MIB=$(awk -v b="$PRE_CRASH_GAP" 'BEGIN { printf "%.3f", b / 1048576 }')
FIRST_REPLAY_AFTER_RESTART=$FIRST_REPLAY_AFTER_RESTART
SHADOW_QUERYABLE_SECS=$SHADOW_QUERYABLE_SECS
REPLAY_AFTER_QUERYABLE_SECS=$REPLAY_AFTER_QUERYABLE_SECS
TOTAL_SHADOW_RECOVERY_SECS=$TOTAL_SHADOW_RECOVERY_SECS
SHADOW_RECOVERY_UPPER_BOUND_SECS=$SHADOW_RECOVERY_UPPER_BOUND_SECS
CDC_TOTAL_RECOVERY_SECS=$CDC_TOTAL_RECOVERY_SECS
UNLOCK_REQ_DELTA=$UNLOCK_REQ_DELTA
UNLOCK_DONE_DELTA=$UNLOCK_DONE_DELTA
UNLOCK_CP_ADVANCED=$UNLOCK_CP_ADVANCED
UNLOCK_REDO_ADVANCED=$UNLOCK_REDO_ADVANCED
CH_RESULT=$CH_RESULT
EOF

RUN_STATUS="success"

log "============================================================"
log "Experiment complete"
log "Summary: $SUMMARY"
log "Machine result: $RESULT_ENV"
log "============================================================"
cat "$SUMMARY"

if [[ "$CLEANUP_ROWS" == "1" ]]; then
    CURRENT_PHASE="cleanup"
    log "Cleaning up test rows."
    source_sql "DELETE FROM $TEST_TABLE WHERE payload LIKE '$RUN_TAG:%';"
    CLEANUP_END_LSN="$(source_q "SELECT pg_current_wal_insert_lsn()::text;")"
    wait_shadow_to_lsn "$CLEANUP_END_LSN" \
        || die "${WAIT_FAILURE_REASON:-Shadow did not drain cleanup WAL.}"
    wait_ctl_lsn "emitter_ack" "$CLEANUP_END_LSN" \
        || die "${WAIT_FAILURE_REASON:-ClickHouse emitter did not drain cleanup WAL.}"
else
    log "Test rows retained. Set CLEANUP_ROWS=1 to delete this run's rows."
fi
