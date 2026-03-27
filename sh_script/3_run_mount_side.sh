#!/usr/bin/env bash
set -euo pipefail

[[ $EUID -eq 0 ]] || {
  echo "root로 실행하세요."
  exit 1
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"

###############################################################################
# 사용자 설정 구간
###############################################################################

FUSE_BIN="${FUSE_BIN:-/home/leedaeeun/Documents/github/libfuse/build/example/passthrough_hp}"
FUSE_LABEL="${FUSE_LABEL:-libfuse example passthrough_hp}"

SRC_BLOCK_DEV_DEFAULT="${SRC_BLOCK_DEV_DEFAULT:-/dev/nvme0n1p1}"
SRC_BLOCK_MNT_DEFAULT="${SRC_BLOCK_MNT_DEFAULT:-/mnt/nvme0n1p1}"
SRC_BLOCK_FSTYPE_DEFAULT="${SRC_BLOCK_FSTYPE_DEFAULT:-ext4}"

SRC_TMPFS_MNT_DEFAULT="${SRC_TMPFS_MNT_DEFAULT:-/mnt/tmpfs-src}"
TMPFS_SIZE="${TMPFS_SIZE:-32G}"

MNT="${MNT:-/mnt/fuse}"

FUSE_DEV="${FUSE_DEV:-/dev/nvme0n1p2}"
FUSE_MNT_FSTYPE="${FUSE_MNT_FSTYPE:-ext4}"

USE_MNT_BACKING_FS="${USE_MNT_BACKING_FS:-1}"
IO_URING_Q_DEPTH="${IO_URING_Q_DEPTH:-2048}"
DEFAULT_SELECTOR="${DEFAULT_SELECTOR:-both}"

RUNS=(
  # 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64
  1 2 3 4 5 6 7 8
  9 10 11 12 13 14 15 16
  17 18 19 20 21 22 23 24
  25 26 27 28 29 30 31 32
  33 34 35 36 37 38 39 40
  41 42 43 44 45 46 47 48
  49 50 51 52 53 54 55 56
  57 58 59 60 61 62 63 64
  # 36 40 44 48 52 56 60 64
)

###############################################################################

STATE_DIR="${STATE_DIR:-/tmp/fuse-exp-automation}"
STATE_FILE="$STATE_DIR/current.env"
LOG_ROOT_DIR="${LOG_ROOT_DIR:-$PROJECT_ROOT/logs}"
SESSION_TAG="$(date +%m%d_%H%M%S)"
SESSION_LOG_DIR="$LOG_ROOT_DIR/log_${SESSION_TAG}"
FUSE_MOUNT_TIMEOUT_SEC="${FUSE_MOUNT_TIMEOUT_SEC:-30}"
FIO_SIDE_PID_FILE="$STATE_DIR/fio_side.pid"
FIO_SIDE_INFO_FILE="$STATE_DIR/fio_side.info"
FIO_SIDE_HEARTBEAT_FILE="$STATE_DIR/fio_side.heartbeat"
FIO_SIDE_WAIT_TIMEOUT_SEC="${FIO_SIDE_WAIT_TIMEOUT_SEC:-60}"
FUSE_URING_PARAM="/sys/module/fuse/parameters/enable_uring"

CURRENT_MNT_FS=""

DIO_MODE=""
SOURCE_PROFILE=""
DIO_DIRECT_VALUE=0

SRC_KIND=""
SRC_DEV=""
SRC_MNT=""
SRC_FSTYPE=""

CURRENT_RUN_ID=""
CURRENT_MODE=""
CURRENT_CORES=""
CURRENT_REP=""
CURRENT_STEP_IDX=""
CURRENT_TOTAL_STEPS=""
CURRENT_BACKEND_KIND=""
CURRENT_TARGET_ROOT=""
CURRENT_PHASE=""
ALL_RUNS_DONE=0
ABORT_REASON=""
INTERRUPTED=0
ACTIVE_FUSE_PID=""

usage() {
  cat <<'EOF'
사용법:
  ./3_run_mount_side.sh <dio_mode> <source_mode> <repeat_count> [selector ...]

dio_mode:
  dio            : direct I/O 모드
  buf            : buffered I/O 모드

source_mode:
  nvm            : SRC_BLOCK_DEV_DEFAULT 를 SRC_BLOCK_MNT_DEFAULT 에 mount해서 FUSE source로 사용
  nvme           : nvm과 동일
  tmpfs          : SRC_TMPFS_MNT_DEFAULT 를 tmpfs로 mount해서 FUSE source로 사용

주의:
  - source_mode 는 FUSE backend의 source 쪽에만 적용된다.
  - plain fs(ext4/xfs/btrfs/f2fs) 실험은 항상 FUSE_DEV 를 MNT 에 mount해서 사용한다.
  - selector 에 plain fs만 있으면 source_mode 는 로그 표기 외에는 직접 사용되지 않는다.

selector:
  both           : 일반 FUSE + uring FUSE
  base           : 일반 FUSE만
  uring          : uring FUSE만
  ext4           : ext4만
  ext4nojournal  : ext4 저널링 비활성화
  xfs            : xfs만
  btrfs          : btrfs만
  f2fs           : f2fs만
  all            : 일반 FUSE + uring FUSE + ext4
  allfs          : 일반 FUSE + uring FUSE + ext4 + ext4nojournal + xfs + btrfs + f2fs

예시:
  ./3_run_mount_side.sh dio nvm 5
  ./3_run_mount_side.sh buf tmpfs 5
  ./3_run_mount_side.sh dio nvm 5 both
  ./3_run_mount_side.sh dio nvm 5 base
  ./3_run_mount_side.sh buf tmpfs 5 uring
  ./3_run_mount_side.sh buf tmpfs 5 ext4
  ./3_run_mount_side.sh buf tmpfs 5 ext4 ext4nojournal
  ./3_run_mount_side.sh dio nvm 5 allfs
EOF
}

is_valid_dio_mode() {
  case "$1" in
  dio | buf | dio_on | dio_off)
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

is_valid_source_mode() {
  case "$1" in
  nvm | nvme | tmpfs)
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

is_valid_selector() {
  case "$1" in
  both | base | uring | ext4 | ext4nojournal | xfs | btrfs | f2fs | all | allfs)
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

configure_dio_mode() {
  case "$1" in
  dio | dio_on)
    DIO_MODE="dio"
    DIO_DIRECT_VALUE=1
    ;;
  buf | dio_off)
    DIO_MODE="buf"
    DIO_DIRECT_VALUE=0
    ;;
  *)
    echo "지원하지 않는 dio_mode: $1"
    exit 1
    ;;
  esac
}

configure_source_profile() {
  case "$1" in
  nvm | nvme)
    SOURCE_PROFILE="nvm"
    SRC_KIND="block"
    SRC_DEV="$SRC_BLOCK_DEV_DEFAULT"
    SRC_MNT="$SRC_BLOCK_MNT_DEFAULT"
    SRC_FSTYPE="$SRC_BLOCK_FSTYPE_DEFAULT"
    ;;
  tmpfs)
    SOURCE_PROFILE="tmpfs"
    SRC_KIND="tmpfs"
    SRC_DEV=""
    SRC_MNT="$SRC_TMPFS_MNT_DEFAULT"
    SRC_FSTYPE=""
    ;;
  *)
    echo "지원하지 않는 source_mode: $1"
    exit 1
    ;;
  esac
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  usage
  exit 0
fi

if (($# < 3)); then
  usage
  exit 1
fi

if ! is_valid_dio_mode "$1"; then
  echo "지원하지 않는 dio_mode: $1"
  usage
  exit 1
fi

if ! is_valid_source_mode "$2"; then
  echo "지원하지 않는 source_mode: $2"
  usage
  exit 1
fi

configure_dio_mode "$1"
configure_source_profile "$2"
REPEAT_COUNT="$3"
shift 3

if ! [[ "$REPEAT_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "repeat_count는 1 이상의 정수여야 합니다."
  exit 1
fi

declare -a SELECTORS=()

if (($# == 0)); then
  SELECTORS=("$DEFAULT_SELECTOR")
else
  SELECTORS=("$@")
fi

for sel in "${SELECTORS[@]}"; do
  if ! is_valid_selector "$sel"; then
    echo "지원하지 않는 selector: $sel"
    usage
    exit 1
  fi
done

mkdir -p "$STATE_DIR" "$MNT" "$SRC_MNT"

if [[ ! -d "$LOG_ROOT_DIR" ]]; then
  mkdir -p "$LOG_ROOT_DIR"
fi
if [[ ! -d "$SESSION_LOG_DIR" ]]; then
  mkdir -p "$SESSION_LOG_DIR"
fi

top_fstype() {
  local mp="$1"

  awk -v mp="$mp" '
    $5 == mp {
      for (i = 1; i <= NF; i++) {
        if ($i == "-") {
          fstype = $(i + 1)
          break
        }
      }
      if (fstype != "")
        last = fstype
      fstype = ""
    }
    END {
      if (last != "")
        print last
    }
  ' /proc/self/mountinfo
}

is_fuse_top_mount() {
  local fstype

  fstype="$(top_fstype "$MNT")"
  [[ "$fstype" == fuse* ]]
}

record_current_context() {
  CURRENT_RUN_ID="$1"
  CURRENT_MODE="$2"
  CURRENT_CORES="$3"
  CURRENT_REP="$4"
  CURRENT_STEP_IDX="$5"
  CURRENT_TOTAL_STEPS="$6"
  CURRENT_BACKEND_KIND="$7"
  CURRENT_TARGET_ROOT="$8"
}

ensure_state_dir() {
  mkdir -p "$STATE_DIR"
}

fio_side_pid() {
  if [[ -r "$FIO_SIDE_PID_FILE" ]]; then
    awk 'NR == 1 { print $1 }' "$FIO_SIDE_PID_FILE"
  fi
}

fio_side_alive() {
  local pid

  pid="$(fio_side_pid)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

read_state_var() {
  local key="$1"

  [[ -r "$STATE_FILE" ]] || return 1

  bash -c '
    key="$1"
    file="$2"
    # shellcheck disable=SC1090
    . "$file" 2>/dev/null || exit 1
    eval "printf \"%s\n\" \"\${$key:-}\""
  ' _ "$key" "$STATE_FILE" 2>/dev/null
}

other_mount_side_alive() {
  local pid

  pid="$(read_state_var MOUNT_SIDE_PID || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ "$pid" == "$$" ]] && return 1
  kill -0 "$pid" 2>/dev/null
}

cleanup_state_files() {
  rm -f "$STATE_FILE" "$STATE_FILE.tmp"

  if ! fio_side_alive; then
    rm -f "$FIO_SIDE_PID_FILE" "$FIO_SIDE_INFO_FILE" "$FIO_SIDE_HEARTBEAT_FILE"
  fi
}

kill_stale_fuse_processes() {
  local pid
  local cmdline

  if ! command -v pgrep >/dev/null 2>&1; then
    return 0
  fi

  while read -r pid; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    [[ "$pid" != "$$" ]] || continue
    if [[ -n "${ACTIVE_FUSE_PID:-}" && "$pid" == "$ACTIVE_FUSE_PID" ]]; then
      continue
    fi
    [[ -r "/proc/$pid/cmdline" ]] || continue

    cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline")"
    case "$cmdline" in
    *"$FUSE_BIN"*"$MNT"*)
      echo "[mount] stale FUSE process 정리: pid=$pid"
      kill "$pid" 2>/dev/null || true
      sleep 0.2
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
      ;;
    esac
  done < <(pgrep -f -- "$FUSE_BIN" || true)
}

reset_after_interrupted_session() {
  echo
  echo "============================================================"
  echo "[mount] 재실행 전 초기화 시작"
  echo "============================================================"

  if other_mount_side_alive; then
    echo "[mount] 다른 3_run_mount_side.sh 가 이미 실행 중입니다: pid=$(read_state_var MOUNT_SIDE_PID)" >&2
    exit 1
  fi

  cleanup_state_files
  kill_stale_fuse_processes || true
  cleanup_mount
  if [[ -n "${SRC_MNT:-}" ]]; then
    unmount_nonfuse_mount "$SRC_MNT" || true
  fi

  rm -rf "$MNT"
  mkdir -p "$MNT" "$SRC_MNT"

  echo "[mount] 재실행 전 초기화 완료"
}

interrupt_handler() {
  INTERRUPTED=1
  ABORT_REASON="${ABORT_REASON:-mount side interrupted by signal}"

  echo
  echo "[mount] Ctrl+C 감지: 현재 세션 정리 후 종료"

  if [[ -n "${ACTIVE_FUSE_PID:-}" ]]; then
    kill "$ACTIVE_FUSE_PID" 2>/dev/null || true
  fi

  exit 130
}

wait_for_fio_side_ready() {
  local start_epoch now tick
  local timeout="${1:-$FIO_SIDE_WAIT_TIMEOUT_SEC}"

  start_epoch="$(date +%s)"
  tick=0

  while true; do
    if fio_side_alive; then
      if [[ -r "$FIO_SIDE_INFO_FILE" ]]; then
        echo "[mount] fio side 연결 확인: $(tr '\n' ' ' <"$FIO_SIDE_INFO_FILE" | sed 's/[[:space:]]\+/ /g')"
      else
        echo "[mount] fio side 연결 확인: pid=$(fio_side_pid)"
      fi
      return 0
    fi

    now="$(date +%s)"
    if ((timeout > 0 && now - start_epoch >= timeout)); then
      echo "[mount] fio side 를 찾지 못했습니다. 먼저 2_run_fio_side.sh 를 실행하세요." >&2
      return 1
    fi

    sleep 0.2
    tick=$((tick + 1))
    if ((tick % 25 == 0)); then
      echo "[mount] fio side 대기 중: $FIO_SIDE_PID_FILE"
    fi
  done
}

print_cmd() {
  printf '  '
  printf '%q ' "$@"
  printf '\n'
}

disable_aslr() {
  echo "[mount] ASLR 비활성화"
  sysctl -w kernel.randomize_va_space=0 >/dev/null
  cat /proc/sys/kernel/randomize_va_space
}

unmount_fuse_if_active() {
  local fstype

  fstype="$(top_fstype "$MNT")"
  if [[ "$fstype" == fuse* ]]; then
    fusermount3 -u "$MNT" || umount "$MNT" || true
  fi
}

unmount_nonfuse_mount() {
  local mp="$1"
  local fstype

  if mountpoint -q "$mp"; then
    fstype="$(top_fstype "$mp")"
    if [[ "$fstype" != fuse* ]]; then
      umount "$mp" || true
    fi
  fi
}

unmount_any_on_mnt() {
  unmount_fuse_if_active || true
  unmount_nonfuse_mount "$MNT" || true
}

dd_zero_device() {
  local dev="$1"
  sync
}

mkfs_on_device() {
  local dev="$1"
  local fstype="$2"

  case "$fstype" in
  ext4)
    mkfs.ext4 -F "$dev"
    ;;
  ext4nojournal)
    mkfs.ext4 -F "$dev"
    tune2fs -O ^has_journal "$dev"
    e2fsck -fy "$dev" || true
    ;;
  xfs)
    mkfs.xfs -f "$dev"
    ;;
  btrfs)
    mkfs.btrfs -f "$dev"
    ;;
  f2fs)
    mkfs.f2fs -f "$dev"
    ;;
  *)
    echo "지원하지 않는 mkfs 타입: $fstype"
    exit 1
    ;;
  esac
}

mount_type_for() {
  case "$1" in
  ext4 | ext4nojournal)
    printf '%s\n' "ext4"
    ;;
  xfs | btrfs | f2fs)
    printf '%s\n' "$1"
    ;;
  *)
    echo "지원하지 않는 mount 타입: $1"
    exit 1
    ;;
  esac
}

reformat_and_mount() {
  local dev="$1"
  local mp="$2"
  local fstype="$3"
  local mount_type

  unmount_nonfuse_mount "$mp"

  mkfs_on_device "$dev" "$fstype"

  mount_type="$(mount_type_for "$fstype")"
  mount -t "$mount_type" "$dev" "$mp"
}

mount_tmpfs_source() {
  unmount_nonfuse_mount "$SRC_MNT" || true
  rm -rf "$SRC_MNT"
  mkdir -p "$SRC_MNT"
  mount -t tmpfs -o "size=$TMPFS_SIZE" tmpfs "$SRC_MNT"
}

prepare_source_fs_for_fuse() {
  case "$SRC_KIND" in
  tmpfs)
    mount_tmpfs_source
    ;;
  block)
    reformat_and_mount "$SRC_DEV" "$SRC_MNT" "$SRC_FSTYPE"
    ;;
  *)
    echo "지원하지 않는 SRC_KIND: $SRC_KIND"
    exit 1
    ;;
  esac
}

prepare_fuse_mount_root_for_fuse() {
  if [[ "$USE_MNT_BACKING_FS" != "1" ]]; then
    unmount_any_on_mnt
    mkdir -p "$MNT"
    return 0
  fi

  unmount_fuse_if_active || true

  if mountpoint -q "$MNT"; then
    local fstype
    fstype="$(top_fstype "$MNT")"
    if [[ "$fstype" != fuse* ]]; then
      CURRENT_MNT_FS="$fstype"
    fi
  fi

  if [[ "$CURRENT_MNT_FS" == "$FUSE_MNT_FSTYPE" ]]; then
    if ! mountpoint -q "$MNT"; then
      mount -t "$(mount_type_for "$FUSE_MNT_FSTYPE")" "$FUSE_DEV" "$MNT"
    fi
    echo "[mount] FUSE backing 재사용: $MNT ($CURRENT_MNT_FS)"
    return 0
  fi

  echo "[mount] FUSE 복귀를 위해 /mnt/fuse 를 $FUSE_MNT_FSTYPE 로 복구합니다"
  reformat_and_mount "$FUSE_DEV" "$MNT" "$FUSE_MNT_FSTYPE"
  CURRENT_MNT_FS="$FUSE_MNT_FSTYPE"
}

prepare_plainfs_mount_root() {
  local fstype="$1"

  unmount_any_on_mnt
  reformat_and_mount "$FUSE_DEV" "$MNT" "$fstype"
  CURRENT_MNT_FS="$fstype"
}

selection_to_experiments() {
  local sel="$1"
  local __outvar="$2"
  local -n out="$__outvar"

  case "$sel" in
  both)
    out=(
      "fuse:base"
      "fuse:uring"
    )
    ;;
  base)
    out=(
      "fuse:base"
    )
    ;;
  uring)
    out=(
      "fuse:uring"
    )
    ;;
  ext4)
    out=(
      "fs:ext4"
    )
    ;;
  ext4nojournal)
    out=(
      "fs:ext4nojournal"
    )
    ;;
  xfs)
    out=(
      "fs:xfs"
    )
    ;;
  btrfs)
    out=(
      "fs:btrfs"
    )
    ;;
  f2fs)
    out=(
      "fs:f2fs"
    )
    ;;
  all)
    out=(
      "fuse:base"
      "fuse:uring"
      "fs:ext4"
    )
    ;;
  allfs)
    out=(
      "fuse:base"
      "fuse:uring"
      "fs:ext4"
      "fs:ext4nojournal"
      "fs:xfs"
      "fs:btrfs"
      "fs:f2fs"
    )
    ;;
  *)
    echo "지원하지 않는 selector: $sel"
    exit 1
    ;;
  esac
}

declare -a EXPERIMENTS=()

build_experiments() {
  local __outvar="$1"
  local -n out="$__outvar"
  local item
  local exp
  local -a tmp=()
  local -A seen=()

  out=()

  for item in "${SELECTORS[@]}"; do
    selection_to_experiments "$item" tmp
    for exp in "${tmp[@]}"; do
      if [[ -n "${seen[$exp]:-}" ]]; then
        continue
      fi
      seen["$exp"]=1
      out+=("$exp")
    done
  done
}

experiments_need_fuse_source() {
  local exp
  local backend

  for exp in "${EXPERIMENTS[@]}"; do
    backend="${exp%%:*}"
    if [[ "$backend" == "fuse" ]]; then
      return 0
    fi
  done

  return 1
}

experiments_need_plainfs() {
  local exp
  local backend

  for exp in "${EXPERIMENTS[@]}"; do
    backend="${exp%%:*}"
    if [[ "$backend" == "fs" ]]; then
      return 0
    fi
  done

  return 1
}

validate_runtime_paths() {
  if [[ -z "$MNT" || "$MNT" == "/" ]]; then
    echo "[mount] MNT 값이 비어 있거나 위험합니다: $MNT" >&2
    exit 1
  fi

  if [[ -z "$SRC_MNT" || "$SRC_MNT" == "/" ]]; then
    echo "[mount] SRC_MNT 값이 비어 있거나 위험합니다: $SRC_MNT" >&2
    exit 1
  fi

  if [[ "$MNT" == "$SRC_MNT" ]]; then
    echo "[mount] MNT 와 SRC_MNT 는 서로 달라야 합니다: $MNT" >&2
    exit 1
  fi
}

validate_runtime_devices() {
  validate_runtime_paths

  if experiments_need_fuse_source; then
    case "$SRC_KIND" in
    block)
      if [[ ! -b "$SRC_DEV" ]]; then
        echo "[mount] source_mode=nvm 이지만 block device 를 찾지 못했습니다: $SRC_DEV" >&2
        exit 1
      fi

      if [[ "$SRC_DEV" == "$FUSE_DEV" ]]; then
        echo "[mount] source block device 와 FUSE_DEV 가 같습니다: $SRC_DEV" >&2
        echo "[mount] nvm source 와 /mnt/fuse 용 device 는 반드시 분리해야 합니다." >&2
        exit 1
      fi
      ;;
    tmpfs)
      if [[ -z "$TMPFS_SIZE" ]]; then
        echo "[mount] TMPFS_SIZE 값이 비어 있습니다." >&2
        exit 1
      fi
      ;;
    *)
      echo "[mount] 지원하지 않는 SRC_KIND: $SRC_KIND" >&2
      exit 1
      ;;
    esac
  fi

  if [[ "$USE_MNT_BACKING_FS" == "1" ]] || experiments_need_plainfs; then
    if [[ ! -b "$FUSE_DEV" ]]; then
      echo "[mount] FUSE_DEV block device 를 찾지 못했습니다: $FUSE_DEV" >&2
      exit 1
    fi
  fi
}

print_source_mode_notes() {
  if experiments_need_fuse_source && experiments_need_plainfs; then
    echo "[mount] 주의: source_mode=$SOURCE_PROFILE 는 FUSE source 에만 적용됩니다"
    echo "[mount] 주의: plain fs 실험은 항상 $FUSE_DEV 를 $MNT 에 mount 해서 사용합니다"
    return 0
  fi

  if ! experiments_need_fuse_source; then
    echo "[mount] 참고: 이번 selector 에는 FUSE 실험이 없어 source_mode=$SOURCE_PROFILE 는 직접 사용되지 않습니다"
    return 0
  fi

  if [[ "$SOURCE_PROFILE" == "tmpfs" ]]; then
    echo "[mount] 참고: tmpfs mode 는 FUSE source 를 매 run 새 tmpfs 로 준비합니다"
  else
    echo "[mount] 참고: nvm mode 는 FUSE source block device 를 매 run 재포맷 후 mount 합니다"
  fi
}

initial_prepare_mounts() {
  echo
  echo "============================================================"
  echo "[mount] 초기 준비 시작"
  echo "[mount] dio mode      : $DIO_MODE"
  echo "[mount] source mode   : $SOURCE_PROFILE"
  echo "[mount] source kind   : $SRC_KIND"
  echo "[mount] source root   : $SRC_MNT"
  if [[ "$SRC_KIND" == "tmpfs" ]]; then
    echo "[mount] tmpfs size    : $TMPFS_SIZE"
  else
    echo "[mount] source dev    : $SRC_DEV"
    echo "[mount] source fstype : $SRC_FSTYPE"
  fi
  echo "============================================================"

  validate_runtime_devices
  print_source_mode_notes

  unmount_any_on_mnt || true
  unmount_nonfuse_mount "$SRC_MNT" || true

  rm -rf "$MNT"
  mkdir -p "$MNT"
  mkdir -p "$SRC_MNT"

  if [[ "$USE_MNT_BACKING_FS" == "1" ]]; then
    reformat_and_mount "$FUSE_DEV" "$MNT" "$FUSE_MNT_FSTYPE"
    CURRENT_MNT_FS="$FUSE_MNT_FSTYPE"
  else
    unmount_any_on_mnt || true
    mkdir -p "$MNT"
    CURRENT_MNT_FS=""
  fi

  if experiments_need_fuse_source; then
    prepare_source_fs_for_fuse
  else
    echo "[mount] 이번 selector에는 FUSE 실험이 없어 source 준비를 건너뜁니다"
  fi

  echo "[mount] 초기 준비 완료"
  findmnt -T "$MNT" || true
  if mountpoint -q "$SRC_MNT"; then
    findmnt -T "$SRC_MNT" || true
  fi
}

wait_for_plainfs_unmount() {
  local fs_type="$1"
  local cores="$2"
  local rep="$3"
  local tick=0

  while mountpoint -q "$MNT"; do
    if ! fio_side_alive; then
      echo "[mount] fio side 가 종료되어 plain fs unmount 를 기다릴 수 없습니다: mode=$fs_type cores=$cores rep=$rep" >&2
      return 1
    fi

    sleep 0.2
    tick=$((tick + 1))

    if ((tick % 25 == 0)); then
      echo "[mount] fio 측 unmount 대기 중: $MNT"
      findmnt -T "$MNT" || true
    fi
  done

  return 0
}

set_fuse_uring_mode() {
  local mode="$1"

  if [[ ! -e "$FUSE_URING_PARAM" ]]; then
    echo "[mount] fuse enable_uring 파라미터를 찾지 못했습니다: $FUSE_URING_PARAM" >&2
    exit 1
  fi

  if [[ ! -w "$FUSE_URING_PARAM" ]]; then
    echo "[mount] fuse enable_uring 파라미터에 쓸 수 없습니다: $FUSE_URING_PARAM" >&2
    exit 1
  fi

  case "$mode" in
  base)
    echo 0 >"$FUSE_URING_PARAM"
    ;;
  uring)
    echo 1 >"$FUSE_URING_PARAM"
    ;;
  *)
    echo "지원하지 않는 FUSE mode: $mode"
    exit 1
    ;;
  esac

  echo "[mount] $FUSE_URING_PARAM = $(cat "$FUSE_URING_PARAM")"
}

build_fuse_cmd() {
  local __outvar="$1"
  local mode="$2"
  local cores="$3"
  local -n out="$__outvar"

  case "$mode" in
  base)
    out=(
      prlimit --nofile=524288:524288 --
      "$FUSE_BIN"
      --foreground
      # --debug-fuse
      --nopassthrough
      --clone-fd
      --num-threads="$cores"
      -o allow_other
      "$SRC_MNT"
      "$MNT"
    )
    ;;
  uring)
    out=(
      prlimit --nofile=524288:524288 --
      "$FUSE_BIN"
      --foreground
      # --debug-fuse
      --nopassthrough
      -o allow_other
      -o io_uring
      -o io_uring_q_depth="$IO_URING_Q_DEPTH"
      "$SRC_MNT"
      "$MNT"
    )
    ;;
  *)
    echo "지원하지 않는 FUSE mode: $mode"
    exit 1
    ;;
  esac
}

write_state_phase() {
  local phase="$1"
  local tmp="$STATE_FILE.tmp"

  ensure_state_dir
  CURRENT_PHASE="$phase"

  {
    printf 'RUN_ID=%q\n' "$CURRENT_RUN_ID"
    printf 'MODE=%q\n' "$CURRENT_MODE"
    printf 'CORES=%q\n' "$CURRENT_CORES"
    printf 'REP=%q\n' "$CURRENT_REP"
    printf 'STEP_IDX=%q\n' "$CURRENT_STEP_IDX"
    printf 'TOTAL_STEPS=%q\n' "$CURRENT_TOTAL_STEPS"
    printf 'BACKEND_KIND=%q\n' "$CURRENT_BACKEND_KIND"
    printf 'TARGET_ROOT=%q\n' "$CURRENT_TARGET_ROOT"
    printf 'SESSION_TAG=%q\n' "$SESSION_TAG"
    printf 'DIO_MODE=%q\n' "$DIO_MODE"
    printf 'SOURCE_PROFILE=%q\n' "$SOURCE_PROFILE"
    printf 'MOUNT_SIDE_PID=%q\n' "$$"
    printf 'PHASE=%q\n' "$phase"
    printf 'DONE=%q\n' "0"
    printf 'ABORTED=%q\n' "0"
    printf 'ABORT_REASON=%q\n' ""
  } >"$tmp"

  mv "$tmp" "$STATE_FILE"
}

write_done() {
  local tmp="$STATE_FILE.tmp"

  ensure_state_dir
  CURRENT_PHASE="done"

  {
    printf 'RUN_ID=%q\n' "$CURRENT_RUN_ID"
    printf 'MODE=%q\n' "$CURRENT_MODE"
    printf 'CORES=%q\n' "$CURRENT_CORES"
    printf 'REP=%q\n' "$CURRENT_REP"
    printf 'STEP_IDX=%q\n' "$CURRENT_STEP_IDX"
    printf 'TOTAL_STEPS=%q\n' "$CURRENT_TOTAL_STEPS"
    printf 'BACKEND_KIND=%q\n' "$CURRENT_BACKEND_KIND"
    printf 'TARGET_ROOT=%q\n' "$CURRENT_TARGET_ROOT"
    printf 'SESSION_TAG=%q\n' "$SESSION_TAG"
    printf 'DIO_MODE=%q\n' "$DIO_MODE"
    printf 'SOURCE_PROFILE=%q\n' "$SOURCE_PROFILE"
    printf 'MOUNT_SIDE_PID=%q\n' "$$"
    printf 'PHASE=%q\n' "done"
    printf 'DONE=%q\n' "1"
    printf 'ABORTED=%q\n' "0"
    printf 'ABORT_REASON=%q\n' ""
  } >"$tmp"

  mv "$tmp" "$STATE_FILE"
}

write_abort() {
  local reason="$1"
  local tmp="$STATE_FILE.tmp"

  [[ -n "${CURRENT_RUN_ID:-}" ]] || return 0

  ensure_state_dir
  CURRENT_PHASE="aborted"
  ABORT_REASON="$reason"

  {
    printf 'RUN_ID=%q\n' "$CURRENT_RUN_ID"
    printf 'MODE=%q\n' "$CURRENT_MODE"
    printf 'CORES=%q\n' "$CURRENT_CORES"
    printf 'REP=%q\n' "$CURRENT_REP"
    printf 'STEP_IDX=%q\n' "$CURRENT_STEP_IDX"
    printf 'TOTAL_STEPS=%q\n' "$CURRENT_TOTAL_STEPS"
    printf 'BACKEND_KIND=%q\n' "$CURRENT_BACKEND_KIND"
    printf 'TARGET_ROOT=%q\n' "$CURRENT_TARGET_ROOT"
    printf 'SESSION_TAG=%q\n' "$SESSION_TAG"
    printf 'DIO_MODE=%q\n' "$DIO_MODE"
    printf 'SOURCE_PROFILE=%q\n' "$SOURCE_PROFILE"
    printf 'MOUNT_SIDE_PID=%q\n' "$$"
    printf 'PHASE=%q\n' "aborted"
    printf 'DONE=%q\n' "1"
    printf 'ABORTED=%q\n' "1"
    printf 'ABORT_REASON=%q\n' "$reason"
  } >"$tmp"

  mv "$tmp" "$STATE_FILE"
}

wait_for_fuse_mount_ready() {
  local fuse_pid="$1"
  local mode="$2"
  local cores="$3"
  local rep="$4"
  local start_epoch now tick

  start_epoch="$(date +%s)"
  tick=0

  while true; do
    if is_fuse_top_mount; then
      return 0
    fi

    if ! kill -0 "$fuse_pid" 2>/dev/null; then
      echo "[mount] FUSE 프로세스가 mount 완료 전에 종료되었습니다: pid=$fuse_pid mode=$mode cores=$cores rep=$rep" >&2
      return 1
    fi

    now="$(date +%s)"
    if ((FUSE_MOUNT_TIMEOUT_SEC > 0 && now - start_epoch >= FUSE_MOUNT_TIMEOUT_SEC)); then
      echo "[mount] FUSE mount 준비 시간 초과: ${FUSE_MOUNT_TIMEOUT_SEC}s mode=$mode cores=$cores rep=$rep" >&2
      return 1
    fi

    sleep 0.2
    tick=$((tick + 1))

    if ((tick % 25 == 0)); then
      echo "[mount] FUSE mount 대기 중: mode=$mode cores=$cores rep=$rep"
      echo "[mount] top fstype: $(top_fstype "$MNT" || true)"
      findmnt -T "$MNT" || true
    fi
  done
}

wait_for_fuse_backend_end() {
  local fuse_pid="$1"
  local mode="$2"
  local cores="$3"
  local rep="$4"
  local tick=0
  local rc

  while true; do
    if ! kill -0 "$fuse_pid" 2>/dev/null; then
      set +e
      wait "$fuse_pid"
      rc=$?
      set -e
      return "$rc"
    fi

    if ! fio_side_alive; then
      echo "[mount] fio side 가 종료되어 FUSE unmount 를 기다릴 수 없습니다: mode=$mode cores=$cores rep=$rep" >&2
      fusermount3 -u "$MNT" || umount "$MNT" || true
      kill "$fuse_pid" 2>/dev/null || true
      wait "$fuse_pid" || true
      return 125
    fi

    sleep 0.2
    tick=$((tick + 1))
    if ((tick % 25 == 0)); then
      echo "[mount] FUSE 종료 대기 중: mode=$mode cores=$cores rep=$rep"
      findmnt -T "$MNT" || true
    fi
  done
}

cleanup_mount() {
  unmount_any_on_mnt || true
}

drop_caches() {
  sync
  echo 3 >/proc/sys/vm/drop_caches
}

run_fuse_backend() {
  local run_id="$1"
  local mode="$2"
  local cores="$3"
  local rep="$4"
  local step_idx="$5"
  local total_steps="$6"
  local -a fuse_cmd
  local fuse_pid
  local fuse_rc

  if [[ ! -x "$FUSE_BIN" ]]; then
    echo "FUSE 실행 파일이 없거나 실행 권한이 없습니다: $FUSE_BIN"
    exit 1
  fi

  if ! wait_for_fio_side_ready; then
    ABORT_REASON="fio side not ready for fuse backend: mode=$mode cores=$cores rep=$rep"
    exit 1
  fi

  prepare_source_fs_for_fuse
  prepare_fuse_mount_root_for_fuse
  set_fuse_uring_mode "$mode"
  build_fuse_cmd fuse_cmd "$mode" "$cores"
  record_current_context "$run_id" "$mode" "$cores" "$rep" \
    "$step_idx" "$total_steps" "fuse" "$MNT"

  echo
  echo "============================================================"
  echo "[mount] backend      : fuse"
  echo "[mount] selectors    : ${SELECTORS[*]}"
  echo "[mount] session tag  : $SESSION_TAG"
  echo "[mount] log dir      : $SESSION_LOG_DIR"
  echo "[mount] progress     : ${step_idx}/${total_steps}"
  echo "[mount] mode         : $mode"
  echo "[mount] dio mode     : $DIO_MODE"
  echo "[mount] cores        : $cores"
  echo "[mount] repeat       : $rep"
  echo "[mount] source mode  : $SOURCE_PROFILE"
  echo "[mount] source kind  : $SRC_KIND"
  echo "[mount] source root  : $SRC_MNT"
  if [[ "$SRC_KIND" == "tmpfs" ]]; then
    echo "[mount] tmpfs size   : $TMPFS_SIZE"
    echo "[mount] source note  : tmpfs source 는 이번 run 시작 시 새로 mount 됩니다"
  else
    echo "[mount] source dev   : $SRC_DEV"
    echo "[mount] source note  : nvm source 는 이번 run 시작 시 재포맷 후 mount 됩니다"
  fi
  echo "[mount] mountpoint   : $MNT"
  echo "[mount] fuse dev     : $FUSE_DEV"
  echo "[mount] fuse label   : $FUSE_LABEL"
  echo "[mount] fuse bin     : $FUSE_BIN"
  echo "[mount] 실행 명령:"
  print_cmd "${fuse_cmd[@]}"
  echo "============================================================"

  write_state_phase "starting"

  "${fuse_cmd[@]}" &
  fuse_pid=$!
  ACTIVE_FUSE_PID="$fuse_pid"

  if ! wait_for_fuse_mount_ready "$fuse_pid" "$mode" "$cores" "$rep"; then
    ABORT_REASON="fuse mount failed before ready: mode=$mode cores=$cores rep=$rep"
    if kill -0 "$fuse_pid" 2>/dev/null; then
      fusermount3 -u "$MNT" || umount "$MNT" || true
      kill "$fuse_pid" 2>/dev/null || true
      wait "$fuse_pid" || true
    fi
    exit 1
  fi

  write_state_phase "ready"

  set +e
  wait_for_fuse_backend_end "$fuse_pid" "$mode" "$cores" "$rep"
  fuse_rc=$?
  set -e

  if ((fuse_rc != 0)); then
    ABORT_REASON="fuse backend ended with rc=$fuse_rc: mode=$mode cores=$cores rep=$rep"
    exit "$fuse_rc"
  fi

  ACTIVE_FUSE_PID=""
  echo "[mount] FUSE 종료: mode=$mode cores=$cores rep=$rep"
}

run_fs_backend() {
  local run_id="$1"
  local fs_type="$2"
  local cores="$3"
  local rep="$4"
  local step_idx="$5"
  local total_steps="$6"

  if ! wait_for_fio_side_ready; then
    ABORT_REASON="fio side not ready for fs backend: mode=$fs_type cores=$cores rep=$rep"
    exit 1
  fi

  prepare_plainfs_mount_root "$fs_type"
  record_current_context "$run_id" "$fs_type" "$cores" "$rep" \
    "$step_idx" "$total_steps" "fs" "$MNT"

  echo
  echo "============================================================"
  echo "[mount] backend      : fs"
  echo "[mount] selectors    : ${SELECTORS[*]}"
  echo "[mount] session tag  : $SESSION_TAG"
  echo "[mount] log dir      : $SESSION_LOG_DIR"
  echo "[mount] progress     : ${step_idx}/${total_steps}"
  echo "[mount] mode         : $fs_type"
  echo "[mount] dio mode     : $DIO_MODE"
  echo "[mount] cores        : $cores"
  echo "[mount] repeat       : $rep"
  echo "[mount] source mode  : $SOURCE_PROFILE (plain fs에서는 직접 사용되지 않음)"
  echo "[mount] source note  : plain fs 실험은 source_mode 와 무관하게 $FUSE_DEV 를 사용합니다"
  echo "[mount] plainfs dev  : $FUSE_DEV"
  echo "[mount] mountpoint   : $MNT"
  echo "============================================================"

  write_state_phase "ready"

  if ! wait_for_plainfs_unmount "$fs_type" "$cores" "$rep"; then
    ABORT_REASON="plain fs unmount wait failed: mode=$fs_type cores=$cores rep=$rep"
    exit 1
  fi

  echo "[mount] plain filesystem 종료: mode=$fs_type cores=$cores rep=$rep"
}

cleanup_all() {
  if ((ALL_RUNS_DONE == 0)) && [[ "${CURRENT_PHASE:-}" != "done" ]]; then
    write_abort "${ABORT_REASON:-mount side terminated before completion}"
  fi

  if [[ -n "${ACTIVE_FUSE_PID:-}" ]]; then
    kill "$ACTIVE_FUSE_PID" 2>/dev/null || true
    wait "$ACTIVE_FUSE_PID" 2>/dev/null || true
  fi

  kill_stale_fuse_processes || true
  cleanup_mount
  if [[ -n "${SRC_MNT:-}" ]]; then
    unmount_nonfuse_mount "$SRC_MNT" || true
  fi

  if ((INTERRUPTED != 0)); then
    cleanup_state_files
  fi
}

trap cleanup_all EXIT
trap interrupt_handler INT TERM

build_experiments EXPERIMENTS
reset_after_interrupted_session
initial_prepare_mounts
disable_aslr

total_steps=$((REPEAT_COUNT * ${#RUNS[@]} * ${#EXPERIMENTS[@]}))
run_id=0
step_idx=0

echo "[mount] session tag   : $SESSION_TAG"
echo "[mount] log root      : $LOG_ROOT_DIR"
echo "[mount] log dir       : $SESSION_LOG_DIR"
echo "[mount] dio mode      : $DIO_MODE"
echo "[mount] source mode   : $SOURCE_PROFILE"
echo "[mount] selectors     : ${SELECTORS[*]}"
if experiments_need_fuse_source && experiments_need_plainfs; then
  echo "[mount] source scope  : FUSE source에만 적용 / plain fs는 $FUSE_DEV 사용"
elif experiments_need_fuse_source; then
  echo "[mount] source scope  : FUSE source에 적용"
else
  echo "[mount] source scope  : 이번 selector에서는 직접 사용되지 않음"
fi
echo "[mount] repeats       : $REPEAT_COUNT"
echo "[mount] cores         : ${RUNS[*]}"

for rep in $(seq 1 "$REPEAT_COUNT"); do
  for cores in "${RUNS[@]}"; do
    for exp in "${EXPERIMENTS[@]}"; do
      run_id=$((run_id + 1))
      step_idx=$((step_idx + 1))

      backend="${exp%%:*}"
      value="${exp##*:}"

      echo
      echo "############################################################"
      echo "# [mount] 준비: backend=$backend value=$value dio=$DIO_MODE source=$SOURCE_PROFILE cores=$cores rep=$rep"
      echo "############################################################"

      case "$backend" in
      fuse)
        run_fuse_backend "$run_id" "$value" "$cores" "$rep" \
          "$step_idx" "$total_steps"
        ;;
      fs)
        run_fs_backend "$run_id" "$value" "$cores" "$rep" \
          "$step_idx" "$total_steps"
        ;;
      *)
        echo "지원하지 않는 실험 종류: $backend"
        exit 1
        ;;
      esac

      drop_caches
    done
  done
done

write_done
ALL_RUNS_DONE=1

echo
echo "[mount] 모든 실험 완료"
