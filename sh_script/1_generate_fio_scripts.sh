#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
OUTROOT="$PROJECT_ROOT/fio_script"

CPU_ORDER=(
  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
  16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
  32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47
  48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
)

usage() {
  cat <<'EOF2'
사용법:
  ./1_generate_fio_scripts.sh <case|fio_name>

case 또는 fio_name:
  1  / filecreate_buffered
  2  / filecreate_dio
  3  / randwrite_4k_buffered
  4  / randwrite_4k_dio
  5  / randwrite_8k_buffered
  6  / randwrite_8k_dio
  7  / randwrite_16k_buffered
  8  / randwrite_16k_dio
  9  / randwrite_32k_buffered
  10 / randwrite_32k_dio
  11 / randwrite_64k_buffered
  12 / randwrite_64k_dio
  13 / randwrite_128k_buffered
  14 / randwrite_128k_dio
  15 / randwrite_256k_buffered
  16 / randwrite_256k_dio
  17 / randwrite_512k_buffered
  18 / randwrite_512k_dio
  19 / randwrite_1024k_buffered
  20 / randwrite_1024k_dio
  21 / randwrite_2048k_buffered
  22 / randwrite_2048k_dio

설명:
  - 생성 위치는 fio_script/<fio_name>/create_01.fio ~ create_64.fio 다.
  - 같은 fio_name으로 다시 생성하면 해당 디렉토리의 create_*.fio 를 덮어쓴다.
  - 각 case의 fio 값은 case별 heredoc에 직접 하드코딩되어 있다.

예시:
  ./1_generate_fio_scripts.sh 1
  ./1_generate_fio_scripts.sh filecreate_buffered
  ./1_generate_fio_scripts.sh randwrite_4k_dio
EOF2
}

die() {
  echo "$*" >&2
  exit 1
}

cpuset_for_cores() {
  local n="$1"
  local -a slice

  if ((n < 1 || n > ${#CPU_ORDER[@]})); then
    die "unsupported core count: $n"
  fi

  slice=("${CPU_ORDER[@]:0:n}")

  local IFS=,
  printf '%s\n' "${slice[*]}"
}

resolve_case() {
  local input="$1"

  case "$input" in
  1 | filecreate_buffered)
    CASE_NO=1
    FIO_NAME="filecreate_buffered"
    CASE_LABEL="filecreate buffered"
    ;;
  2 | filecreate_dio)
    CASE_NO=2
    FIO_NAME="filecreate_dio"
    CASE_LABEL="filecreate DIO"
    ;;
  3 | randwrite_4k_buffered)
    CASE_NO=3
    FIO_NAME="randwrite_4k_buffered"
    CASE_LABEL="4KB randwrite buffered"
    ;;
  4 | randwrite_4k_dio)
    CASE_NO=4
    FIO_NAME="randwrite_4k_dio"
    CASE_LABEL="4KB randwrite DIO"
    ;;
  5 | randwrite_8k_buffered)
    CASE_NO=5
    FIO_NAME="randwrite_8k_buffered"
    CASE_LABEL="8KB randwrite buffered"
    ;;
  6 | randwrite_8k_dio)
    CASE_NO=6
    FIO_NAME="randwrite_8k_dio"
    CASE_LABEL="8KB randwrite DIO"
    ;;
  7 | randwrite_16k_buffered)
    CASE_NO=7
    FIO_NAME="randwrite_16k_buffered"
    CASE_LABEL="16KB randwrite buffered"
    ;;
  8 | randwrite_16k_dio)
    CASE_NO=8
    FIO_NAME="randwrite_16k_dio"
    CASE_LABEL="16KB randwrite DIO"
    ;;
  9 | randwrite_32k_buffered)
    CASE_NO=9
    FIO_NAME="randwrite_32k_buffered"
    CASE_LABEL="32KB randwrite buffered"
    ;;
  10 | randwrite_32k_dio)
    CASE_NO=10
    FIO_NAME="randwrite_32k_dio"
    CASE_LABEL="32KB randwrite DIO"
    ;;
  11 | randwrite_64k_buffered)
    CASE_NO=11
    FIO_NAME="randwrite_64k_buffered"
    CASE_LABEL="64KB randwrite buffered"
    ;;
  12 | randwrite_64k_dio)
    CASE_NO=12
    FIO_NAME="randwrite_64k_dio"
    CASE_LABEL="64KB randwrite DIO"
    ;;
  13 | randwrite_128k_buffered)
    CASE_NO=13
    FIO_NAME="randwrite_128k_buffered"
    CASE_LABEL="128KB randwrite buffered"
    ;;
  14 | randwrite_128k_dio)
    CASE_NO=14
    FIO_NAME="randwrite_128k_dio"
    CASE_LABEL="128KB randwrite DIO"
    ;;
  15 | randwrite_256k_buffered)
    CASE_NO=15
    FIO_NAME="randwrite_256k_buffered"
    CASE_LABEL="256KB randwrite buffered"
    ;;
  16 | randwrite_256k_dio)
    CASE_NO=16
    FIO_NAME="randwrite_256k_dio"
    CASE_LABEL="256KB randwrite DIO"
    ;;
  17 | randwrite_512k_buffered)
    CASE_NO=17
    FIO_NAME="randwrite_512k_buffered"
    CASE_LABEL="512KB randwrite buffered"
    ;;
  18 | randwrite_512k_dio)
    CASE_NO=18
    FIO_NAME="randwrite_512k_dio"
    CASE_LABEL="512KB randwrite DIO"
    ;;
  19 | randwrite_1024k_buffered)
    CASE_NO=19
    FIO_NAME="randwrite_1024k_buffered"
    CASE_LABEL="1024KB randwrite buffered"
    ;;
  20 | randwrite_1024k_dio)
    CASE_NO=20
    FIO_NAME="randwrite_1024k_dio"
    CASE_LABEL="1024KB randwrite DIO"
    ;;
  21 | randwrite_2048k_buffered)
    CASE_NO=21
    FIO_NAME="randwrite_2048k_buffered"
    CASE_LABEL="2048KB randwrite buffered"
    ;;
  22 | randwrite_2048k_dio)
    CASE_NO=22
    FIO_NAME="randwrite_2048k_dio"
    CASE_LABEL="2048KB randwrite DIO"
    ;;
  *)
    die "지원하지 않는 case 또는 fio_name: $input"
    ;;
  esac
}

prepare_outdir() {
  OUTDIR="$OUTROOT/$FIO_NAME"
  mkdir -p "$OUTDIR"
  rm -f "$OUTDIR"/create_*.fio
}

write_metadata() {
  cat >"$OUTDIR/.fio_workload_meta" <<EOF2
fio_name=$FIO_NAME
case_no=$CASE_NO
label=$CASE_LABEL
generated_at=$(date '+%Y-%m-%d %H:%M:%S')
EOF2
}

write_job_for_case() {
  local case_no="$1"
  local jobfile="$2"
  local cores="$3"
  local cpuset="$4"
  local file_no="$5"

  case "$case_no" in
  1)
    cat >"$jobfile" <<EOF2
[global]
ioengine=filecreate
thread=1
group_reporting=1
direct=0

create_on_open=1
create_serialize=0
create_fsync=0

openfiles=1
filesize=4k

numjobs=$cores
nrfiles=8000

directory=/mnt/fuse/filecreate_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum
unique_filename=0

cpus_allowed=$cpuset
cpus_allowed_policy=split

[create-buffered]
EOF2
    ;;
  2)
    cat >"$jobfile" <<EOF2
[global]
ioengine=filecreate
thread=1
group_reporting=1
direct=1

create_on_open=1
create_serialize=0
create_fsync=0

openfiles=1
filesize=4k

numjobs=$cores
nrfiles=8000

directory=/mnt/fuse/filecreate_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum
unique_filename=0

cpus_allowed=$cpuset
cpus_allowed_policy=split

[create-dio]
EOF2
    ;;
  3)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_4k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-4kb-buffered]
bs=4k
rw=randwrite
size=256M
EOF2
    ;;
  4)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_4k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-4kb-dio]
bs=4k
rw=randwrite
size=256M
EOF2
    ;;
  5)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_8k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-8kb-buffered]
bs=8k
rw=randwrite
size=256M
EOF2
    ;;
  6)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_8k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-8kb-dio]
bs=8k
rw=randwrite
size=256M
EOF2
    ;;
  7)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_16k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-16kb-buffered]
bs=16k
rw=randwrite
size=256M
EOF2
    ;;
  8)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_16k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-16kb-dio]
bs=16k
rw=randwrite
size=256M
EOF2
    ;;
  9)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_32k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-32kb-buffered]
bs=32k
rw=randwrite
size=256M
EOF2
    ;;
  10)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_32k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-32kb-dio]
bs=32k
rw=randwrite
size=256M
EOF2
    ;;
  11)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_64k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-64kb-buffered]
bs=64k
rw=randwrite
size=256M
EOF2
    ;;
  12)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_64k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-64kb-dio]
bs=64k
rw=randwrite
size=256M
EOF2
    ;;
  13)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_128k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-128kb-buffered]
bs=128k
rw=randwrite
size=256M
EOF2
    ;;
  14)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_128k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-128kb-dio]
bs=128k
rw=randwrite
size=256M
EOF2
    ;;
  15)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_256k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-256kb-buffered]
bs=256k
rw=randwrite
size=256M
EOF2
    ;;
  16)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_256k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-256kb-dio]
bs=256k
rw=randwrite
size=256M
EOF2
    ;;
  17)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_512k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-512kb-buffered]
bs=512k
rw=randwrite
size=1G
EOF2
    ;;
  18)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_512k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-512kb-dio]
bs=512k
rw=randwrite
size=1G
EOF2
    ;;
  19)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_1024k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-1024kb-buffered]
bs=1024k
rw=randwrite
size=1G
EOF2
    ;;
  20)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_1024k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-1024kb-dio]
bs=1024k
rw=randwrite
size=1G
EOF2
    ;;
  21)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=0
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_2048k_buffered/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-2048kb-buffered]
bs=2048k
rw=randwrite
size=1G
EOF2
    ;;
  22)
    cat >"$jobfile" <<EOF2
[global]
numjobs=$cores
ioengine=psync
iodepth=1
direct=1
time_based=1
runtime=30
norandommap=1
randrepeat=1
group_reporting=1
end_fsync=0
allow_file_create=0
thread=1
ramp_time=5

nrfiles=1
directory=/mnt/fuse/randwrite_2048k_dio/case${file_no}-run
filename_format=file.\$jobnum.\$filenum

cpus_allowed=$cpuset
cpus_allowed_policy=split

[rand-write-2048kb-dio]
bs=2048k
rw=randwrite
size=1G
EOF2
    ;;
  *)
    die "지원하지 않는 case: $case_no"
    ;;
  esac
}

generate_case_files() {
  local cores
  local cpuset
  local file_no
  local jobfile

  prepare_outdir
  write_metadata

  for ((cores = 1; cores <= 64; cores++)); do
    cpuset="$(cpuset_for_cores "$cores")"
    printf -v file_no '%02d' "$cores"
    jobfile="$OUTDIR/create_${file_no}.fio"
    write_job_for_case "$CASE_NO" "$jobfile" "$cores" "$cpuset" "$file_no"
  done

  echo "Generated: $OUTDIR/create_*.fio ($CASE_LABEL)"
}

main() {
  local input="${1:-}"

  [[ $# -eq 1 ]] || {
    usage
    exit 1
  }

  case "$input" in
  -h | --help | help)
    usage
    exit 0
    ;;
  esac

  resolve_case "$input"
  generate_case_files
}

main "$@"
