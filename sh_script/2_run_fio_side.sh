#!/usr/bin/env bash
set -euo pipefail

[[ $EUID -eq 0 ]] || {
  echo "root로 실행하세요."
  exit 1
}

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"

MNT="${MNT:-/mnt/fuse}"
STATE_DIR="${STATE_DIR:-/tmp/fuse-exp-automation}"
FIO_ROOT_DIR="${FIO_ROOT_DIR:-$PROJECT_ROOT/fio_script}"
LOG_ROOT_DIR="${LOG_ROOT_DIR:-$PROJECT_ROOT/logs}"
TMP_JOB_DIR="${TMP_JOB_DIR:-/tmp/fio-auto-jobs}"
FIO_SIDE_PID_FILE="$STATE_DIR/fio_side.pid"
FIO_SIDE_INFO_FILE="$STATE_DIR/fio_side.info"
FIO_SIDE_HEARTBEAT_FILE="$STATE_DIR/fio_side.heartbeat"
SPACE_HEADROOM_MIN_BYTES=$((512 * 1024 * 1024))
SPACE_HEADROOM_PERCENT=10
INTERRUPTED=0

CPU_ORDER=(
  0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
  16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
  32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47
  48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63
)

usage() {
  cat <<'EOF2'
사용법:
  ./2_run_fio_side.sh <fio_name>

예시:
  ./2_run_fio_side.sh filecreate_buffered
  ./2_run_fio_side.sh filecreate_dio
  ./2_run_fio_side.sh randwrite_4k_buffered
  ./2_run_fio_side.sh randwrite_4k_dio

설명:
  - 1번 스크립트가 생성한 fio_script/<fio_name>/create_01.fio ~ create_64.fio 를 실행한다.
  - 3번 스크립트가 current.env 를 갱신하면 해당 cores/repeat/backend 조합에 맞춰 실행한다.
  - allow_file_create=0 인 workload 는 실행 전에 입력 파일을 먼저 생성한다.
EOF2
}

die() {
  echo "$*" >&2
  exit 1
}

sanitize_name() {
  local s="$1"
  s="${s// /_}"
  s="${s//\//_}"
  printf '%s\n' "$s"
}

detect_restore_owner() {
  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    printf '%s:%s\n' "$SUDO_USER" "$(id -gn "$SUDO_USER")"
    return 0
  fi

  stat -c '%U:%G' "$PROJECT_ROOT"
}

resolve_fio_dir() {
  local input="$1"

  if [[ -d "$FIO_ROOT_DIR/$input" ]]; then
    FIO_NAME="$input"
    FIO_DIR="$FIO_ROOT_DIR/$input"
    return 0
  fi

  if [[ -d "$input" ]]; then
    FIO_NAME="$(basename "$input")"
    FIO_DIR="$input"
    return 0
  fi

  die "fio 디렉토리를 찾지 못했습니다: $input"
}

RESTORE_OWNER="$(detect_restore_owner)"

cleanup_stale_tmp() {
  if [[ -e "$TMP_JOB_DIR" ]]; then
    rm -rf "$TMP_JOB_DIR"
  fi
}

ensure_state_dir() {
  mkdir -p "$STATE_DIR"
}

write_fio_side_presence() {
  ensure_state_dir

  printf '%s\n' "$$" >"$FIO_SIDE_PID_FILE"

  {
    printf 'pid=%q\n' "$$"
    printf 'fio_name=%q\n' "${FIO_NAME:-}"
  } >"$FIO_SIDE_INFO_FILE"

  date +%s >"$FIO_SIDE_HEARTBEAT_FILE"
}

touch_fio_side_heartbeat() {
  ensure_state_dir
  date +%s >"$FIO_SIDE_HEARTBEAT_FILE"
}

remove_fio_side_presence() {
  rm -f "$FIO_SIDE_PID_FILE" "$FIO_SIDE_INFO_FILE" "$FIO_SIDE_HEARTBEAT_FILE"
}

existing_fio_side_pid() {
  if [[ -r "$FIO_SIDE_PID_FILE" ]]; then
    awk 'NR == 1 { print $1 }' "$FIO_SIDE_PID_FILE"
  fi
}

other_fio_side_alive() {
  local pid

  pid="$(existing_fio_side_pid || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ "$pid" == "$$" ]] && return 1
  kill -0 "$pid" 2>/dev/null
}

read_state_var() {
  local key="$1"
  local state_file="$STATE_DIR/current.env"

  [[ -r "$state_file" ]] || return 1

  bash -c '
    key="$1"
    file="$2"
    # shellcheck disable=SC1090
    . "$file" 2>/dev/null || exit 1
    eval "printf \"%s\n\" \"\${$key:-}\""
  ' _ "$key" "$state_file" 2>/dev/null
}

mount_side_pid_from_state() {
  read_state_var MOUNT_SIDE_PID
}

mount_side_alive_from_state() {
  local pid

  pid="$(mount_side_pid_from_state || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

best_effort_unmount_mnt() {
  if ! mountpoint -q "$MNT"; then
    return 0
  fi

  case "${BACKEND_KIND:-}" in
  fuse)
    fusermount3 -u "$MNT" || umount "$MNT" || umount -l "$MNT" || true
    ;;
  fs)
    umount "$MNT" || fusermount3 -u "$MNT" || umount -l "$MNT" || true
    ;;
  *)
    fusermount3 -u "$MNT" || umount "$MNT" || umount -l "$MNT" || true
    ;;
  esac
}

reset_after_interrupted_session() {
  echo "[fio] 재실행 전 초기화 시작"

  cleanup_stale_tmp
  ensure_state_dir

  if other_fio_side_alive; then
    die "다른 2_run_fio_side.sh 가 이미 실행 중입니다: pid=$(existing_fio_side_pid)"
  fi

  remove_fio_side_presence || true
  rm -f "$STATE_DIR/current.env.tmp"

  if [[ -f "$STATE_DIR/current.env" ]]; then
    if mount_side_alive_from_state; then
      echo "[fio] live mount side session 감지: current.env 유지"
    else
      echo "[fio] stale current.env 제거"
      rm -f "$STATE_DIR/current.env"
    fi
  fi

  if ! mount_side_alive_from_state && mountpoint -q "$MNT"; then
    echo "[fio] stale mount 정리 시도: $MNT"
    best_effort_unmount_mnt
  fi

  mkdir -p "$TMP_JOB_DIR"
  echo "[fio] 재실행 전 초기화 완료"
}

interrupt_handler() {
  INTERRUPTED=1
  echo
  echo "[fio] Ctrl+C 감지: 현재 세션 정리 후 종료"

  if command -v pkill >/dev/null 2>&1; then
    pkill -INT -P "$$" fio 2>/dev/null || true
    pkill -TERM -P "$$" tee 2>/dev/null || true
  fi

  exit 130
}

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

first_value_of_key() {
  local jobfile="$1"
  local key="$2"

  awk -F= -v key="$key" '
    /^[[:space:]]*;/ { next }
    /^[[:space:]]*#/ { next }
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" {
      val = $2
      sub(/^[[:space:]]+/, "", val)
      sub(/[[:space:]]+$/, "", val)
      print val
      exit
    }
  ' "$jobfile"
}

job_needs_precreate() {
  local jobfile="$1"

  grep -Eq '^[[:space:]]*allow_file_create[[:space:]]*=[[:space:]]*0([[:space:]]*($|[#;]))' "$jobfile"
}

expand_filename_format() {
  local fmt="$1"
  local jobnum="$2"
  local filenum="$3"
  local name

  name="${fmt//\$jobnum/$jobnum}"
  name="${name//\$filenum/$filenum}"
  printf '%s\n' "$name"
}

preallocate_one_file() {
  local path="$1"
  local size="$2"

  mkdir -p "$(dirname "$path")"

  if ! fallocate -l "$size" "$path" 2>/dev/null; then
    truncate -s "$size" "$path"
  fi
}

prepare_input_files_if_needed() {
  local jobfile="$1"
  local target_dir="$2"
  local numjobs
  local nrfiles
  local filename_format
  local filename
  local size
  local j
  local f
  local created=0
  local path

  if ! job_needs_precreate "$jobfile"; then
    return 0
  fi

  numjobs="$(first_value_of_key "$jobfile" numjobs)"
  nrfiles="$(first_value_of_key "$jobfile" nrfiles)"
  filename_format="$(first_value_of_key "$jobfile" filename_format)"
  filename="$(first_value_of_key "$jobfile" filename)"
  size="$(first_value_of_key "$jobfile" size)"

  if [[ -z "$size" ]]; then
    size="$(first_value_of_key "$jobfile" filesize)"
  fi

  [[ -n "$numjobs" ]] || numjobs=1
  [[ -n "$nrfiles" ]] || nrfiles=1
  [[ -n "$size" ]] || die "size/filesize 값을 찾지 못했습니다: $jobfile"

  if [[ -z "$filename_format" && -z "$filename" ]]; then
    filename_format='file.$jobnum.$filenum'
  fi

  echo "[fio] allow_file_create=0 감지: 입력 파일 사전 생성 시작"
  echo "[fio] precreate numjobs=$numjobs nrfiles=$nrfiles size=$size"

  if [[ -n "$filename" ]]; then
    path="$target_dir/$filename"
    preallocate_one_file "$path" "$size"
    created=1
  else
    for ((j = 0; j < numjobs; j++)); do
      for ((f = 0; f < nrfiles; f++)); do
        path="$target_dir/$(expand_filename_format "$filename_format" "$j" "$f")"
        preallocate_one_file "$path" "$size"
        created=$((created + 1))
      done
    done
  fi

  echo "[fio] 입력 파일 사전 생성 완료: ${created}개"
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

padded_field() {
  local text="$1"
  local width="$2"
  local padded

  printf -v padded "%-${width}s" "$text"
  padded="${padded// /_}"
  printf '%s\n' "$padded"
}

build_final_log_path() {
  local session_tag="$1"
  local fio_name="$2"
  local mode="$3"
  local cores="$4"
  local rep="$5"
  local elapsed_tag="$6"
  local session_dir="$7"
  local fio_field
  local mode_field
  local core_field

  fio_field="$(padded_field "$fio_name" 24)"
  mode_field="$(padded_field "$mode" 18)"
  printf -v core_field '%02dcore' "$cores"

  printf '%s/log_%s___%s___%s___%s___r%s___%s.log\n' \
    "$session_dir" "$session_tag" "$fio_field" "$mode_field" "$core_field" "$rep" "$elapsed_tag"
}

parse_size_to_bytes() {
  local raw="$1"
  local num suffix

  raw="${raw//[[:space:]]/}"
  [[ -n "$raw" ]] || return 1

  case "$raw" in
  *[KkMmGgTtPp])
    num="${raw%[KkMmGgTtPp]}"
    suffix="${raw:${#raw}-1}"
    ;;
  *)
    if [[ "$raw" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$raw"
      return 0
    fi
    return 1
    ;;
  esac

  [[ "$num" =~ ^[0-9]+$ ]] || return 1

  case "$suffix" in
  k|K) printf '%s\n' "$((num * 1024))" ;;
  m|M) printf '%s\n' "$((num * 1024 * 1024))" ;;
  g|G) printf '%s\n' "$((num * 1024 * 1024 * 1024))" ;;
  t|T) printf '%s\n' "$((num * 1024 * 1024 * 1024 * 1024))" ;;
  p|P) printf '%s\n' "$((num * 1024 * 1024 * 1024 * 1024 * 1024))" ;;
  *) return 1 ;;
  esac
}

required_bytes_for_jobfile() {
  local jobfile="$1"
  local numjobs
  local nrfiles
  local unit_size
  local unit_bytes

  numjobs="$(first_value_of_key "$jobfile" numjobs)"
  nrfiles="$(first_value_of_key "$jobfile" nrfiles)"
  unit_size="$(first_value_of_key "$jobfile" size)"

  if [[ -z "$unit_size" ]]; then
    unit_size="$(first_value_of_key "$jobfile" filesize)"
  fi

  [[ -n "$numjobs" ]] || numjobs=1
  [[ -n "$nrfiles" ]] || nrfiles=1
  [[ -n "$unit_size" ]] || return 1

  unit_bytes="$(parse_size_to_bytes "$unit_size")" || return 1
  printf '%s\n' "$((unit_bytes * numjobs * nrfiles))"
}

available_bytes_on_path() {
  local path="$1"

  df -PB1 --output=avail "$path" 2>/dev/null | awk 'NR==2 { print $1 }'
}

humanize_bytes() {
  local value="$1"

  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec-i --suffix=B "$value"
    return 0
  fi

  printf '%sB\n' "$value"
}

check_space_budget() {
  local jobfile="$1"
  local path="$2"
  local required_bytes
  local available_bytes
  local headroom_bytes
  local needed_bytes
  local source_profile
  local backend_kind

  required_bytes="$(required_bytes_for_jobfile "$jobfile" 2>/dev/null || true)"
  [[ "$required_bytes" =~ ^[0-9]+$ ]] || return 0

  available_bytes="$(available_bytes_on_path "$path" 2>/dev/null || true)"
  [[ "$available_bytes" =~ ^[0-9]+$ ]] || return 0

  headroom_bytes=$((required_bytes * SPACE_HEADROOM_PERCENT / 100))
  if ((headroom_bytes < SPACE_HEADROOM_MIN_BYTES)); then
    headroom_bytes=$SPACE_HEADROOM_MIN_BYTES
  fi
  needed_bytes=$((required_bytes + headroom_bytes))

  if ((needed_bytes <= available_bytes)); then
    return 0
  fi

  source_profile="${SOURCE_PROFILE:-unknown}"
  backend_kind="${BACKEND_KIND:-unknown}"

  echo "[fio] 공간 부족 예상: workload=$FIO_NAME backend=$backend_kind mode=${MODE:-unknown}" >&2
  echo "[fio] required(raw) : $(humanize_bytes "$required_bytes")" >&2
  echo "[fio] required+margin: $(humanize_bytes "$needed_bytes")" >&2
  echo "[fio] available     : $(humanize_bytes "$available_bytes")" >&2

  if [[ "$backend_kind" == "fuse" && "$source_profile" == "tmpfs" ]]; then
    echo "[fio] tmpfs source 에서는 현재 workload/cores 조합이 TMPFS_SIZE 를 초과할 가능성이 큽니다." >&2
    echo "[fio] 특히 64코어 기준 512k/1024k/2048k randwrite 는 size=1G x numjobs 이라 64GiB 이상이 필요합니다." >&2
  fi

  exit 1
}

cleanup_all() {
  if ((INTERRUPTED != 0)); then
    echo "[fio] 중단 감지: mount/tmp/state 정리"
  fi

  best_effort_unmount_mnt || true
  rm -rf "$TMP_JOB_DIR"
  remove_fio_side_presence || true

  if ! mount_side_alive_from_state; then
    rm -f "$STATE_DIR/current.env" "$STATE_DIR/current.env.tmp"
  fi

  rmdir "$STATE_DIR" 2>/dev/null || true

  if [[ -d "$LOG_ROOT_DIR" ]]; then
    chown -R "$RESTORE_OWNER" "$LOG_ROOT_DIR" || true
  fi
}

main() {
  local input_fio_name="${1:-}"
  local last_run=""
  local wait_msg_tick=0
  local session_tag
  local session_dir
  local sanitized_fio_name
  local file_no
  local jobfile
  local cpuset
  local numa_args
  local target_dir
  local tmpjob
  local tmp_log
  local start_epoch
  local end_epoch
  local elapsed_sec
  local elapsed_tag
  local elapsed_human
  local final_log
  local fio_rc

  if [[ $# -ne 1 ]]; then
    usage
    exit 1
  fi

  case "$input_fio_name" in
  -h | --help | help)
    usage
    exit 0
    ;;
  esac

  resolve_fio_dir "$input_fio_name"
  sanitized_fio_name="$(sanitize_name "$FIO_NAME")"

  reset_after_interrupted_session

  if [[ ! -d "$LOG_ROOT_DIR" ]]; then
    mkdir -p "$LOG_ROOT_DIR"
  fi
  mkdir -p "$TMP_JOB_DIR"

  trap cleanup_all EXIT
  trap interrupt_handler INT TERM
  write_fio_side_presence

  echo "[fio] workload 선택: $FIO_NAME"
  echo "[fio] fio dir      : $FIO_DIR"
  echo "[fio] current.env 대기 중"

  while true; do
    touch_fio_side_heartbeat
    if [[ ! -f "$STATE_DIR/current.env" ]]; then
      sleep 0.2
      wait_msg_tick=$((wait_msg_tick + 1))
      if ((wait_msg_tick % 25 == 0)); then
        echo "[fio] state 파일 대기 중"
      fi
      continue
    fi

    unset RUN_ID MODE CORES REP DONE STEP_IDX TOTAL_STEPS BACKEND_KIND TARGET_ROOT SESSION_TAG DIO_MODE PHASE
    # shellcheck disable=SC1090
    . "$STATE_DIR/current.env"

    if [[ "${DONE:-0}" == "1" ]]; then
      if ! is_mount_active; then
        echo
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
    target_dir="$MNT/$sanitized_fio_name/create_${file_no}-run"
    tmpjob="$TMP_JOB_DIR/${sanitized_fio_name}.${MODE}.create_${file_no}.r${REP}.fio"

    if [[ ! -f "$jobfile" ]]; then
      echo "[fio] jobfile 없음: $jobfile" >&2
      exit 1
    fi

    session_tag="${SESSION_TAG:-$(date +%m%d_%H%M%S)}"
    session_dir="$LOG_ROOT_DIR/log_${session_tag}"
    if [[ ! -d "$session_dir" ]]; then
      mkdir -p "$session_dir"
    fi

    tmp_log="$session_dir/log_${session_tag}___${sanitized_fio_name}___running___${file_no}core___r${REP}.log"

    echo
    echo "############################################################"
    if [[ -n "${STEP_IDX:-}" && -n "${TOTAL_STEPS:-}" ]]; then
      echo "# [fio] progress : ${STEP_IDX}/${TOTAL_STEPS}"
    fi
    echo "# [fio] workload : $FIO_NAME"
    echo "# [fio] backend  : $BACKEND_KIND"
    echo "# [fio] mode     : $MODE"
    if [[ -n "${DIO_MODE:-}" ]]; then
      echo "# [fio] dio mode : $DIO_MODE"
    fi
    echo "# [fio] cores    : $CORES"
    echo "# [fio] repeat   : $REP"
    echo "# [fio] jobfile  : $jobfile"
    echo "# [fio] target   : $target_dir"
    echo "# [fio] cpuset   : $cpuset"
    echo "# [fio] numa     : $numa_args"
    echo "# [fio] log dir  : $session_dir"
    echo "############################################################"

    mkdir -p "$target_dir"
    rm -rf "$target_dir"/*

    make_tmp_jobfile "$jobfile" "$tmpjob" "$target_dir"
    check_space_budget "$tmpjob" "$MNT"
    prepare_input_files_if_needed "$tmpjob" "$target_dir"

    write_fio_side_presence
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
    final_log="$(build_final_log_path "$session_tag" "$sanitized_fio_name" "$MODE" "$CORES" "$REP" "$elapsed_tag" "$session_dir")"

    mv "$tmp_log" "$final_log"

    echo "[fio] 완료: workload=$FIO_NAME backend=$BACKEND_KIND mode=$MODE cores=$CORES rep=$REP"
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
}

main "$@"
