#!/usr/bin/env bash
set -u

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <run_id> [interval_seconds]" >&2
  exit 1
fi

RUN_ID="$1"
INTERVAL="${2:-30}"
TS="$(date +%Y%m%d_%H%M%S)"
LOG="backend/run_logs/live_watch_${RUN_ID}_${TS}.log"

mkdir -p backend/run_logs

echo "[START] $(date '+%F %T %Z') run_id=$RUN_ID interval=${INTERVAL}s" >> "$LOG"

STALL_COUNT=0
LAST_EVENT_AT=""

while true; do
  NOW="$(date '+%F %T %Z')"
  JSON="$(curl -s --max-time 8 "http://127.0.0.1:8000/api/v1/scrape-runs/$RUN_ID")"
  STATUS="$(echo "$JSON" | jq -r '.status // "unknown"')"
  LAST_STAGE="$(echo "$JSON" | jq -r '.progress_state.last_stage // "n/a"')"
  LAST_UPDATE="$(echo "$JSON" | jq -r '.progress_state.last_update_at // "n/a"')"
  MET="$(echo "$JSON" | jq -c '.metrics // {}')"

  EVENTS_JSON="$(curl -s --max-time 8 "http://127.0.0.1:8000/api/v1/scrape-runs/$RUN_ID/events?cursor=0")"
  EVENT_COUNT="$(echo "$EVENTS_JSON" | jq -r '(.events // []) | length')"
  LAST_EVENT="$(echo "$EVENTS_JSON" | jq -r '(.events // [])[-1].created_at // "n/a"')"
  LAST_EVENT_STAGE="$(echo "$EVENTS_JSON" | jq -r '(.events // [])[-1].stage // "n/a"')"

  if [ "$LAST_EVENT" = "$LAST_EVENT_AT" ]; then
    STALL_COUNT=$((STALL_COUNT + 1))
  else
    STALL_COUNT=0
    LAST_EVENT_AT="$LAST_EVENT"
  fi

  echo "[$NOW] status=$STATUS stage=$LAST_STAGE last_update=$LAST_UPDATE events=$EVENT_COUNT last_event=$LAST_EVENT last_event_stage=$LAST_EVENT_STAGE stall_ticks=$STALL_COUNT metrics=$MET" >> "$LOG"

  if [ "$STALL_COUNT" -ge 6 ] && [ "$STATUS" = "running" ]; then
    echo "[$NOW] STALL_DETECTED no new events for >= $((INTERVAL*6)) seconds" >> "$LOG"
  fi

  case "$STATUS" in
    complete|partial|error|blocked|cancelled)
      echo "[END] $NOW terminal_status=$STATUS" >> "$LOG"
      break
      ;;
  esac

  sleep "$INTERVAL"
done

echo "$LOG"
