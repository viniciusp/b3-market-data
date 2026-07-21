#!/usr/bin/env sh
# Builds and starts the whole stack, then follows the initial trade backfill
# until the data is ready.

set -e

if [ -n "$(docker compose ps -q 2>/dev/null)" ]; then
  echo "the stack is already running; nothing to do."
  echo "  check it:                  docker compose ps"
  echo "  open it:                   http://localhost:8000"
  echo "  restart (keep data):       docker compose down && ./start.sh"
  echo "  fresh start (wipe data):   docker compose down -v && ./start.sh"
  exit 1
fi

docker compose up -d --build

echo ""
echo "stack is up; following the backfill (Ctrl+C detaches, the stack keeps running)"
echo ""

docker compose logs -f --no-log-prefix poller 2>/dev/null | awk '
  /backfill started/ {
    match($0, /sessions=[0-9]+/)
    total = substr($0, RSTART + 9, RLENGTH - 9)
    printf "backfill started: %s sessions to sweep\n", total
    started = 1
    next
  }
  started && /published ticker=/ {
    match($0, /file=[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]/)
    day = substr($0, RSTART + 5, 10)
    match($0, /new=[0-9]+/)
    events += substr($0, RSTART + 4, RLENGTH - 4)
    if (day != current) {
      current = day
      done++
      printf "  session %d/%s (%s)\n", done, total, day
    }
    next
  }
  /backfill finished/ {
    printf "backfill finished: %d sessions swept, %d trades published\n", done, events
    printf "data is ready: http://localhost:8000\n"
    exit 0
  }
'
