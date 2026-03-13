#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
OUTDIR="$PROJECT_ROOT/fio_script"

: "${NRFILES:=8000}"
: "${FILESIZE:=4k}"
: "${MOUNT_ROOT:=/mnt/fuse/fuse-create}"

CPU_ORDER=(
  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
  16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
  32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47
  48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
)

cpuset_for_cores() {
  local n="$1"
  local -a slice

  if ((n < 1 || n > ${#CPU_ORDER[@]})); then
    echo "unsupported core count: $n" >&2
    return 1
  fi

  slice=("${CPU_ORDER[@]:0:n}")

  local IFS=,
  printf '%s\n' "${slice[*]}"
}

if [ ! -d "$OUTDIR" ]; then
  mkdir -p "$OUTDIR"
fi

cores=1
while ((cores <= 64)); do
  cpuset="$(cpuset_for_cores "$cores")"
  printf -v file_no '%02d' "$cores"
  jobfile="$OUTDIR/create_${file_no}.fio"

  cat >"$jobfile" <<EOF
[global]
ioengine=filecreate
thread=1
group_reporting=1

create_on_open=1
create_serialize=0
create_fsync=0

openfiles=1
filesize=$FILESIZE

numjobs=$cores
nrfiles=$NRFILES

directory=$MOUNT_ROOT/case${file_no}-run
filename_format=file.\$jobnum.\$filenum
unique_filename=0

cpus_allowed=$cpuset
cpus_allowed_policy=split

[create]
EOF

  cores=$((cores + 1))
done

echo "Generated fio job files in: $OUTDIR"
