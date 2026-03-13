#!/usr/bin/env bash
set -euo pipefail

[[ $EUID -eq 0 ]] || {
  echo "root로 실행하세요."
  exit 1
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"

MNT="/mnt/fuse"
STATE_DIR="/tmp/fuse-exp-automation"
FIO_DIR="$PROJECT_ROOT/fio_script"
LOG_ROOT_DIR="$PROJECT_ROOT/logs"
TMP_JOB_DIR="/tmp/fio-auto-jobs"

CPU_ORDER=(
  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
  16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
  32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47
  48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
)

detect_restore_owner() {
  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    printf '%s:%s\n' "$SUDO_USER" "$(id -gn "$SUDO_USER")"
    return 0
  fi

  stat -c '%U:%G' "$PROJECT_ROOT"
}

RESTORE_OWNER="$(detect_restore_owner)"

cleanup_stale_tmp() {
  if [[ -e "$STATE_DIR" ]]; then
    rm -rf "$STATE_DIR"
  fi

  if [[ -e "$TMP_JOB_DIR" ]]; then
    rm -rf "$TMP_JOB_DIR"
  fi
}

cleanup_stale_tmp

if [[ ! -d "$LOG_ROOT_DIR" ]]; then
  mkdir -p "$LOG_ROOT_DIR"
fi
mkdir -p "$TMP_JOB_DIR"

is_fuse_active() {
  findmnt -T "$MNT" -n -o FSTYPE 2>/dev/null | grep -q '^fuse'
}

is_mount_active() {
  mountpoint -q "$MNT"
}

is_backend_ready() {
  local backend="$1"

  case "$backend" in
  fuse)
    is_fuse_active
    ;;
  fs)
    is_mount_active
    ;;
  *)
    return 1
    ;;
  esac
}

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

numa_args_for_cores() {
  local n="$1"

  if ((n <= 16)); then
    printf '%s\n' '--membind=0'
  else
    printf '%s\n' '--interleave=0,1'
  fi
}

make_tmp_jobfile() {
  local src_job="$1"
  local dst_job="$2"
  local target_dir="$3"

  sed "s|^directory=.*|directory=$target_dir|" "$src_job" >"$dst_job"
}

format_elapsed_filename() {
  local sec="$1"
  local h m s

  h=$((sec / 3600))
  m=$(((sec % 3600) / 60))
  s=$((sec % 60))

  if ((h > 0)); then
    printf '%02dh_%02dm_%02ds\n' "$h" "$m" "$s"
  else
    printf '%02dm_%02ds\n' "$m" "$s"
  fi
}

format_elapsed_human() {
  local sec="$1"
  local h m s

  h=$((sec / 3600))
  m=$(((sec % 3600) / 60))
  s=$((sec % 60))

  if ((h > 0)); then
    printf '%02dh %02dm %02ds\n' "$h" "$m" "$s"
  else
    printf '%02dm %02ds\n' "$m" "$s"
  fi
}

padded_mode_field() {
  local mode="$1"
  local padded

  printf -v padded '%-18s' "$mode"
  padded="${padded// /_}"
  printf '%s\n' "$padded"
}

build_final_log_path() {
  local session_tag="$1"
  local mode="$2"
  local cores="$3"
  local rep="$4"
  local elapsed_tag="$5"
  local session_dir="$6"

  local mode_field
  local core_field

  mode_field="$(padded_mode_field "$mode")"
  printf -v core_field '%02dcore' "$cores"

  printf '%s/log_%s___%s___%s___r%s___%s.log\n' \
    "$session_dir" "$session_tag" "$mode_field" "$core_field" "$rep" "$elapsed_tag"
}

cleanup_all() {
  rm -rf "$TMP_JOB_DIR"

  if [[ -f "$STATE_DIR/current.env" ]]; then
    rm -f "$STATE_DIR/current.env"
  fi

  rmdir "$STATE_DIR" 2>/dev/null || true

  if [[ -d "$LOG_ROOT_DIR" ]]; then
    chown -R "$RESTORE_OWNER" "$LOG_ROOT_DIR" || true
  fi
}

trap cleanup_all EXIT

last_run=""
wait_msg_tick=0

echo "[fio] current.env 대기 중"

while true; do
  if [[ ! -f "$STATE_DIR/current.env" ]]; then
    sleep 0.2
    wait_msg_tick=$((wait_msg_tick + 1))
    if ((wait_msg_tick % 25 == 0)); then
      echo "[fio] state 파일 대기 중"
    fi
    continue
  fi

  unset RUN_ID MODE CORES REP DONE STEP_IDX TOTAL_STEPS BACKEND_KIND TARGET_ROOT SESSION_TAG
  # shellcheck disable=SC1090
  . "$STATE_DIR/current.env"

  if [[ "${DONE:-0}" == "1" ]]; then
    if ! is_mount_active; then
      echo -e "\n\n\n\n\n\n\n\n\n\n"
      echo "============================================================"
      echo "[fio] 모든 실험 완료"
      echo "[fio] tmp 정리 및 logs 소유권 복구 진행"
      echo "============================================================"
      exit 0
    fi
  fi

  if [[ -z "${RUN_ID:-}" || -z "${CORES:-}" || -z "${REP:-}" || -z "${MODE:-}" || -z "${BACKEND_KIND:-}" ]]; then
    sleep 0.2
    continue
  fi

  if [[ "$RUN_ID" == "$last_run" ]]; then
    sleep 0.2
    continue
  fi

  if ! is_backend_ready "$BACKEND_KIND"; then
    sleep 0.2
    wait_msg_tick=$((wait_msg_tick + 1))
    if ((wait_msg_tick % 25 == 0)); then
      case "$BACKEND_KIND" in
      fuse)
        echo "[fio] FUSE mount 대기 중: $MNT"
        ;;
      fs)
        echo "[fio] plain filesystem mount 대기 중: $MNT"
        ;;
      *)
        echo "[fio] backend 대기 중: $BACKEND_KIND"
        ;;
      esac
      findmnt -T "$MNT" || true
    fi
    continue
  fi

  wait_msg_tick=0

  printf -v file_no '%02d' "$CORES"

  jobfile="$FIO_DIR/create_${file_no}.fio"
  cpuset="$(cpuset_for_cores "$CORES")"
  numa_args="$(numa_args_for_cores "$CORES")"
  target_dir="$MNT/create_${file_no}-run"
  tmpjob="$TMP_JOB_DIR/${MODE}.create_${file_no}.r${REP}.fio"

  if [[ ! -f "$jobfile" ]]; then
    echo "[fio] jobfile 없음: create_${file_no}.fio" >&2
    exit 1
  fi

  session_tag="${SESSION_TAG:-$(date +%m%d_%H%M%S)}"
  session_dir="$LOG_ROOT_DIR/log_${session_tag}"
  if [[ ! -d "$session_dir" ]]; then
    mkdir -p "$session_dir"
  fi

  tmp_log="$session_dir/log_${session_tag}___running____________${file_no}core___r${REP}.log"

  echo -e "\n\n\n\n\n\n\n\n\n\n"
  echo "############################################################"
  if [[ -n "${STEP_IDX:-}" && -n "${TOTAL_STEPS:-}" ]]; then
    echo "# [fio] progress : ${STEP_IDX}/${TOTAL_STEPS}"
  fi
  echo "# [fio] backend  : $BACKEND_KIND"
  echo "# [fio] mode     : $MODE"
  echo "# [fio] cores    : $CORES"
  echo "# [fio] repeat   : $REP"
  echo "# [fio] jobfile  : create_${file_no}.fio"
  echo "# [fio] target   : $target_dir"
  echo "# [fio] cpuset   : $cpuset"
  echo "# [fio] numa     : $numa_args"
  echo "# [fio] log dir  : $session_dir"
  echo "############################################################"

  mkdir -p "$target_dir"
  rm -rf "$target_dir"/*

  make_tmp_jobfile "$jobfile" "$tmpjob" "$target_dir"

  start_epoch="$(date +%s)"
  set +e
  prlimit --nofile=524288:524288 -- \
    numactl --physcpubind="$cpuset" "$numa_args" \
    fio --eta=never "$tmpjob" 2>&1 | tee "$tmp_log"
  fio_rc=${PIPESTATUS[0]}
  set -e
  end_epoch="$(date +%s)"

  elapsed_sec=$((end_epoch - start_epoch))
  elapsed_tag="$(format_elapsed_filename "$elapsed_sec")"
  elapsed_human="$(format_elapsed_human "$elapsed_sec")"
  final_log="$(build_final_log_path "$session_tag" "$MODE" "$CORES" "$REP" "$elapsed_tag" "$session_dir")"

  mv "$tmp_log" "$final_log"

  echo "[fio] 완료: backend=$BACKEND_KIND mode=$MODE cores=$CORES rep=$REP"
  echo "[fio] elapsed: $elapsed_human"
  echo "[fio] saved  : $final_log"

  if ((fio_rc != 0)); then
    echo "[fio] fio 실패: rc=$fio_rc" >&2
    exit "$fio_rc"
  fi

  echo "[fio] mount 해제 수행: $MNT"

  case "$BACKEND_KIND" in
  fuse)
    fusermount3 -u "$MNT" || umount "$MNT" || true
    ;;
  fs)
    umount "$MNT" || fusermount3 -u "$MNT" || true
    ;;
  *)
    echo "[fio] 지원하지 않는 BACKEND_KIND: $BACKEND_KIND" >&2
    exit 1
    ;;
  esac

  last_run="$RUN_ID"
done
