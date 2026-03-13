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

# passthrough_hp 실행 파일만 절대경로 유지
FUSE_BIN="/home/leedaeeun/Documents/github/libfuse/build/example/passthrough_hp"
FUSE_LABEL="libfuse example passthrough_hp"

# FUSE source 쪽 장치/마운트 정보
SRC_DEV="/dev/nvme0n1p1"
SRC_MNT="/mnt/nvme0n1p1"
SRC_FSTYPE="ext4"

# 모든 실험의 실행 mountpoint
MNT="/mnt/fuse"

# FUSE backing fs 또는 plain filesystem 실험에 사용하는 장치
FUSE_DEV="/dev/nvme0n1p2"
FUSE_MNT_FSTYPE="ext4"

# FUSE mountpoint 밑에 backing fs를 둘지 여부
# 1 : 필요 시 FUSE_DEV를 mount하고 그 위에 FUSE mount
# 0 : 단순 mountpoint 디렉토리만 사용
USE_MNT_BACKING_FS=1

# io_uring qdepth
IO_URING_Q_DEPTH=8

# 기본 선택자
DEFAULT_SELECTOR="both"

# 코어 목록
RUNS=(
  1 4
  # 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 64
  # 1 2 3 4 5 6 7 8
  # 9 10 11 12 13 14 15 16
  # 17 18 19 20 21 22 23 24
  # 25 26 27 28 29 30 31 32
  # # 33 34 35 36 37 38 39 40
  # # 41 42 43 44 45 46 47 48
  # # 49 50 51 52 53 54 55 56
  # # 57 58 59 60 61 62 63 64
  # #
  # 36 40 44 48 52 56 60 64
)

###############################################################################

STATE_DIR="/tmp/fuse-exp-automation"
LOG_ROOT_DIR="$PROJECT_ROOT/logs"
SESSION_TAG="$(date +%m%d_%H%M%S)"
SESSION_LOG_DIR="$LOG_ROOT_DIR/log_${SESSION_TAG}"

# /mnt/fuse 바닥 파일시스템 상태 추적
CURRENT_MNT_FS=""

usage() {
  cat <<'EOF'
사용법:
  ./3_run_mount_side.sh <repeat_count> [selector ...]

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
  ./3_run_mount_side.sh 5
  ./3_run_mount_side.sh 5 both
  ./3_run_mount_side.sh 5 base
  ./3_run_mount_side.sh 5 uring
  ./3_run_mount_side.sh 5 ext4
  ./3_run_mount_side.sh 5 ext4 ext4nojournal
  ./3_run_mount_side.sh 5 ext4 btrfs f2fs
  ./3_run_mount_side.sh 5 btrfs xfs
  ./3_run_mount_side.sh 5 base ext4
  ./3_run_mount_side.sh 5 uring ext4
  ./3_run_mount_side.sh 5 both ext4
  ./3_run_mount_side.sh 5 all
  ./3_run_mount_side.sh 5 allfs
EOF
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

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  usage
  exit 0
fi

REPEAT_COUNT="${1:-}"
shift || true

if [[ -z "$REPEAT_COUNT" ]]; then
  usage
  exit 1
fi

if ! [[ "$REPEAT_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "repeat_count는 1 이상의 정수여야 합니다." >&2
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
    echo "지원하지 않는 selector: $sel" >&2
    usage
    exit 1
  fi
done

mkdir -p "$STATE_DIR" "$SRC_MNT" "$MNT"

if [[ ! -d "$LOG_ROOT_DIR" ]]; then
  mkdir -p "$LOG_ROOT_DIR"
fi
if [[ ! -d "$SESSION_LOG_DIR" ]]; then
  mkdir -p "$SESSION_LOG_DIR"
fi

top_fstype() {
  findmnt -T "$1" -n -o FSTYPE 2>/dev/null || true
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

# dd 방식 포맷은 너무 오래 걸려서 사용하지 않음.
# 필요하면 아래 주석을 해제해서 다시 사용할 수 있음.
dd_zero_device() {
  local dev="$1"

  # dd if=/dev/zero of="$dev" bs=4M status=progress conv=fsync || true
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
    echo "지원하지 않는 mkfs 타입: $fstype" >&2
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
    echo "지원하지 않는 mount 타입: $1" >&2
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

  # dd 기반 전체 zero-fill 포맷은 비활성화.
  # dd_zero_device "$dev"

  mkfs_on_device "$dev" "$fstype"

  mount_type="$(mount_type_for "$fstype")"
  mount -t "$mount_type" "$dev" "$mp"
}

# FUSE source는 FUSE 실험 때마다 다시 포맷
prepare_source_fs_for_fuse() {
  reformat_and_mount "$SRC_DEV" "$SRC_MNT" "$SRC_FSTYPE"
}

# FUSE backing은 ext4가 아니면 ext4로 복구, ext4면 재사용
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

# plain filesystem 실험은 /mnt/fuse 를 해당 fs로 매번 포맷
prepare_plainfs_mount_root() {
  local fstype="$1"

  unmount_any_on_mnt
  reformat_and_mount "$FUSE_DEV" "$MNT" "$fstype"
  CURRENT_MNT_FS="$fstype"
}

initial_prepare_mounts() {
  echo
  echo "============================================================"
  echo "[mount] 초기 준비 시작"
  echo "[mount] /mnt/fuse 와 /mnt/nvme0n1p1 를 먼저 ext4로 재포맷합니다"
  echo "============================================================"

  unmount_any_on_mnt || true
  unmount_nonfuse_mount "$SRC_MNT" || true

  rm -rf "$MNT"
  mkdir -p "$MNT"
  mkdir -p "$SRC_MNT"

  reformat_and_mount "$FUSE_DEV" "$MNT" "$FUSE_MNT_FSTYPE"
  reformat_and_mount "$SRC_DEV" "$SRC_MNT" "$SRC_FSTYPE"

  CURRENT_MNT_FS="$FUSE_MNT_FSTYPE"

  echo "[mount] 초기 준비 완료"
  findmnt -T "$MNT" || true
  findmnt -T "$SRC_MNT" || true
}

wait_for_plainfs_unmount() {
  local tick=0

  while mountpoint -q "$MNT"; do
    sleep 0.2
    tick=$((tick + 1))

    if ((tick % 25 == 0)); then
      echo "[mount] fio 측 unmount 대기 중: $MNT"
      findmnt -T "$MNT" || true
    fi
  done
}

set_fuse_uring_mode() {
  local mode="$1"

  case "$mode" in
  base)
    echo 0 >/sys/module/fuse/parameters/enable_uring
    ;;
  uring)
    echo 1 >/sys/module/fuse/parameters/enable_uring
    ;;
  *)
    echo "지원하지 않는 FUSE mode: $mode" >&2
    exit 1
    ;;
  esac

  echo "[mount] /sys/module/fuse/parameters/enable_uring = $(cat /sys/module/fuse/parameters/enable_uring)"
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
      --debug-fuse
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
      --debug-fuse
      --nopassthrough
      -o allow_other
      -o io_uring
      -o io_uring_q_depth="$IO_URING_Q_DEPTH"
      "$SRC_MNT"
      "$MNT"
    )
    ;;
  *)
    echo "지원하지 않는 FUSE mode: $mode" >&2
    exit 1
    ;;
  esac
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
    echo "지원하지 않는 selector: $sel" >&2
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

write_state() {
  local run_id="$1"
  local mode="$2"
  local cores="$3"
  local rep="$4"
  local step_idx="$5"
  local total_steps="$6"
  local backend_kind="$7"
  local target_root="$8"
  local tmp="$STATE_DIR/current.env.tmp"

  {
    printf 'RUN_ID=%q\n' "$run_id"
    printf 'MODE=%q\n' "$mode"
    printf 'CORES=%q\n' "$cores"
    printf 'REP=%q\n' "$rep"
    printf 'STEP_IDX=%q\n' "$step_idx"
    printf 'TOTAL_STEPS=%q\n' "$total_steps"
    printf 'BACKEND_KIND=%q\n' "$backend_kind"
    printf 'TARGET_ROOT=%q\n' "$target_root"
    printf 'SESSION_TAG=%q\n' "$SESSION_TAG"
    printf 'DONE=0\n'
  } >"$tmp"

  mv "$tmp" "$STATE_DIR/current.env"
}

write_done() {
  local tmp="$STATE_DIR/current.env.tmp"

  {
    printf 'DONE=1\n'
  } >"$tmp"

  mv "$tmp" "$STATE_DIR/current.env"
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

  if [[ ! -x "$FUSE_BIN" ]]; then
    echo "FUSE 실행 파일이 없거나 실행 권한이 없습니다: $FUSE_BIN" >&2
    exit 1
  fi

  prepare_source_fs_for_fuse
  prepare_fuse_mount_root_for_fuse
  set_fuse_uring_mode "$mode"
  build_fuse_cmd fuse_cmd "$mode" "$cores"

  echo
  echo "============================================================"
  echo "[mount] backend      : fuse"
  echo "[mount] selectors    : ${SELECTORS[*]}"
  echo "[mount] session tag  : $SESSION_TAG"
  echo "[mount] log dir      : $SESSION_LOG_DIR"
  echo "[mount] progress     : ${step_idx}/${total_steps}"
  echo "[mount] mode         : $mode"
  echo "[mount] cores        : $cores"
  echo "[mount] repeat       : $rep"
  echo "[mount] source root  : $SRC_MNT"
  echo "[mount] mountpoint   : $MNT"
  echo "[mount] fuse dev     : $FUSE_DEV"
  echo "[mount] fuse label   : $FUSE_LABEL"
  echo "[mount] fuse bin     : $FUSE_BIN"
  echo "[mount] 실행 명령:"
  print_cmd "${fuse_cmd[@]}"
  echo "============================================================"

  write_state "$run_id" "$mode" "$cores" "$rep" \
    "$step_idx" "$total_steps" "fuse" "$MNT"

  "${fuse_cmd[@]}"

  echo "[mount] FUSE 종료: mode=$mode cores=$cores rep=$rep"
}

run_fs_backend() {
  local run_id="$1"
  local fs_type="$2"
  local cores="$3"
  local rep="$4"
  local step_idx="$5"
  local total_steps="$6"

  prepare_plainfs_mount_root "$fs_type"

  echo
  echo "============================================================"
  echo "[mount] backend      : fs"
  echo "[mount] selectors    : ${SELECTORS[*]}"
  echo "[mount] session tag  : $SESSION_TAG"
  echo "[mount] log dir      : $SESSION_LOG_DIR"
  echo "[mount] progress     : ${step_idx}/${total_steps}"
  echo "[mount] mode         : $fs_type"
  echo "[mount] cores        : $cores"
  echo "[mount] repeat       : $rep"
  echo "[mount] plainfs dev  : $FUSE_DEV"
  echo "[mount] mountpoint   : $MNT"
  echo "============================================================"

  write_state "$run_id" "$fs_type" "$cores" "$rep" \
    "$step_idx" "$total_steps" "fs" "$MNT"

  wait_for_plainfs_unmount

  echo "[mount] plain filesystem 종료: mode=$fs_type cores=$cores rep=$rep"
}

cleanup_all() {
  cleanup_mount
}

trap cleanup_all EXIT INT TERM

cleanup_mount
rm -f "$STATE_DIR/current.env"

initial_prepare_mounts

disable_aslr
build_experiments EXPERIMENTS

total_steps=$((REPEAT_COUNT * ${#RUNS[@]} * ${#EXPERIMENTS[@]}))
run_id=0
step_idx=0

echo "[mount] session tag   : $SESSION_TAG"
echo "[mount] log root      : $LOG_ROOT_DIR"
echo "[mount] log dir       : $SESSION_LOG_DIR"
echo "[mount] selectors     : ${SELECTORS[*]}"
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
      echo "# [mount] 준비: backend=$backend value=$value cores=$cores rep=$rep"
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
        echo "지원하지 않는 실험 종류: $backend" >&2
        exit 1
        ;;
      esac

      drop_caches
    done
  done
done

write_done

echo
echo "[mount] 모든 실험 완료"
