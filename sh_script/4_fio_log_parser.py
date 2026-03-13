#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PCT_MAP = {
    '1.00': 'p01',
    '5.00': 'p05',
    '10.00': 'p10',
    '20.00': 'p20',
    '30.00': 'p30',
    '40.00': 'p40',
    '50.00': 'p50',
    '60.00': 'p60',
    '70.00': 'p70',
    '80.00': 'p80',
    '90.00': 'p90',
    '95.00': 'p95',
    '99.00': 'p99',
    '99.50': 'p995',
    '99.90': 'p999',
    '99.95': 'p9995',
    '99.99': 'p9999',
}

PRIMARY_FIELDS = [
    'session', 'mode', 'threads', 'repeat', 'elapsed_tag', 'elapsed_sec',
    'jobs', 'total_ops', 'ops_per_job', 'iops', 'bw_kib_per_sec',
    'bw_mib_per_sec', 'io_mib', 'run_msec', 'clat_avg_usec',
    'clat_stdev_usec', 'cpu_usr_pct', 'cpu_sys_pct', 'ctx',
]

FIELDNAMES = PRIMARY_FIELDS + [
    'log_file', 'job_name', 'fio_version', 'threads_started',
    'groupid', 'err', 'pid', 'start_time',
    'rw', 'ioengine', 'iodepth',
    'bs_r_min', 'bs_r_max', 'bs_w_min', 'bs_w_max', 'bs_t_min', 'bs_t_max',
    'op', 'bw_decimal_value', 'bw_decimal_unit',
    'io_decimal_value', 'io_decimal_unit',
    'run_bw_kib_per_sec', 'run_bw_mib_per_sec', 'run_io_mib', 'run_msec_min', 'run_msec_max',
    'bw_stat_unit', 'bw_min_kib_per_sec', 'bw_max_kib_per_sec', 'bw_avg_kib_per_sec',
    'bw_stdev_kib_per_sec', 'bw_per_pct', 'bw_samples',
    'iops_min', 'iops_max', 'iops_avg', 'iops_stdev', 'iops_samples',
    'cpu_majf', 'cpu_minf',
    'total_read_ops', 'total_write_ops', 'total_trim_ops', 'total_sync_ops',
    'short_read_ops', 'short_write_ops', 'short_trim_ops', 'short_sync_ops',
    'dropped_read_ops', 'dropped_write_ops', 'dropped_trim_ops', 'dropped_sync_ops',
    'latency_target', 'latency_window', 'latency_percentile', 'latency_depth',
    'io_depth_1_pct', 'io_depth_2_pct', 'io_depth_4_pct', 'io_depth_8_pct',
    'io_depth_16_pct', 'io_depth_32_pct', 'io_depth_ge64_pct',
    'submit_0_pct', 'submit_4_pct', 'submit_8_pct', 'submit_16_pct',
    'submit_32_pct', 'submit_64_pct', 'submit_ge64_pct',
    'complete_0_pct', 'complete_4_pct', 'complete_8_pct', 'complete_16_pct',
    'complete_32_pct', 'complete_64_pct', 'complete_ge64_pct',
]

for kind in ('slat', 'clat', 'lat'):
    FIELDNAMES += [
        f'{kind}_unit', f'{kind}_min_usec', f'{kind}_max_usec',
        f'{kind}_avg_usec', f'{kind}_stdev_usec',
    ]
    for suffix in PCT_MAP.values():
        FIELDNAMES.append(f'{kind}_{suffix}_usec')

FILENAME_RE = re.compile(
    r'^log_(\d{4}_\d{6})___([A-Za-z0-9_-]+?)_+([0-9]{2})core___r(\d+)___([0-9A-Za-z_]+)\.log$'
)
HEADER_RE = re.compile(
    r'^(\S+): \(g=(\d+)\): rw=([^,]+), bs=\(R\)\s*([^\s]+)-([^\s]+), '
    r'\(W\)\s*([^\s]+)-([^\s]+), \(T\)\s*([^\s]+)-([^\s]+), ioengine=([^,]+), iodepth=(\d+)'
)
GROUP_RE = re.compile(
    r'^(\S+): \(groupid=(\d+), jobs=(\d+)\): err=\s*(\d+): pid=(\d+):\s+(.+)$'
)
START_THREADS_RE = re.compile(r'^Starting\s+(\d+)\s+thread')
IOPS_RE = re.compile(
    r'^\s+(read|write|trim|sync):\s+IOPS=([\d.]+)([kKmMgGtTpPeE]?),\s+'
    r'BW=([\d.]+)(\S+)\s+\(([\d.]+)([A-Za-z/]+)\)\(([\d.]+)([A-Za-z]+)/(\d+)msec\)'
)
LAT_RE = re.compile(
    r'^\s+(slat|clat|lat)\s+\(([A-Za-z]+)\):\s+min=\s*([\d.]+)([kKmMgGtTpPeE]?)?,\s+'
    r'max=\s*([\d.]+)([kKmMgGtTpPeE]?)?,\s+avg=\s*([\d.]+),\s+stdev=\s*([\d.]+)'
)
PCT_HEADER_RE = re.compile(r'^\s+(slat|clat|lat) percentiles \(([A-Za-z]+)\):')
BW_STATS_RE = re.compile(
    r'^\s+bw\s+\(\s*([A-Za-z/]+)\):\s+min=\s*([\d.]+),\s+max=\s*([\d.]+),\s+'
    r'per=\s*([\d.]+)%,\s+avg=\s*([\d.]+),\s+stdev=\s*([\d.]+),\s+samples=(\d+)'
)
IOPS_STATS_RE = re.compile(
    r'^\s+iops\s*:\s+min=\s*([\d.]+),\s+max=\s*([\d.]+),\s+avg=\s*([\d.]+),\s+'
    r'stdev=\s*([\d.]+),\s+samples=(\d+)'
)
CPU_RE = re.compile(
    r'^\s+cpu\s*:\s*usr=([\d.]+)%,\s*sys=([\d.]+)%,\s*ctx=(\d+),\s*majf=(\d+),\s*minf=(\d+)'
)
ISSUED_RE = re.compile(
    r'^\s+issued rwts:\s+total=(\d+),(\d+),(\d+),(\d+)\s+'
    r'short=(\d+),(\d+),(\d+),(\d+)\s+'
    r'dropped=(\d+),(\d+),(\d+),(\d+)'
)
LATENCY_RE = re.compile(
    r'^\s+latency\s*:\s*target=(\d+),\s*window=(\d+),\s*percentile=([\d.]+)%,\s*depth=(\d+)'
)
RUN_RE = re.compile(
    r'^\s+(READ|WRITE|TRIM|SYNC):\s+bw=([\d.]+)([A-Za-z/]+)\s+\(([\d.]+)([A-Za-z/]+)\),.*?'
    r'io=([\d.]+)([A-Za-z]+)\s+\(([\d.]+)([A-Za-z]+)\),\s*run=(\d+)-(\d+)msec'
)


def scaled_to_num(value: Optional[str], suffix: str = '', base: float = 1000.0) -> Optional[float]:
    if value in (None, ''):
        return None
    v = float(value)
    if not suffix:
        return v
    pows = {'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5, 'e': 6}
    s = suffix.lower()
    return v * (base ** pows[s]) if s in pows else v


def to_kib_per_sec(value: Optional[str], unit: Optional[str]) -> Optional[float]:
    if value in (None, '') or not unit:
        return None
    v = float(value)
    table = {
        'B/s': 1 / 1024,
        'KiB/s': 1,
        'MiB/s': 1024,
        'GiB/s': 1024 * 1024,
        'TiB/s': 1024 * 1024 * 1024,
        'kB/s': 1000 / 1024,
        'MB/s': 1000 * 1000 / 1024,
        'GB/s': 1000 * 1000 * 1000 / 1024,
        'TB/s': 1000 * 1000 * 1000 * 1000 / 1024,
    }
    return v * table[unit] if unit in table else None


def to_mib(value: Optional[str], unit: Optional[str]) -> Optional[float]:
    if value in (None, '') or not unit:
        return None
    v = float(value)
    table = {
        'B': 1 / (1024 * 1024),
        'KiB': 1 / 1024,
        'MiB': 1,
        'GiB': 1024,
        'TiB': 1024 * 1024,
        'kB': 1000 / (1024 * 1024),
        'MB': 1000 * 1000 / (1024 * 1024),
        'GB': 1000 * 1000 * 1000 / (1024 * 1024),
        'TB': 1000 * 1000 * 1000 * 1000 / (1024 * 1024),
    }
    return v * table[unit] if unit in table else None


def lat_to_usec(value: Optional[str], unit: Optional[str], suffix: str = '') -> Optional[float]:
    if value in (None, '') or not unit:
        return None
    v = scaled_to_num(value, suffix, 1000.0)
    table = {
        'nsec': 1 / 1000,
        'usec': 1,
        'msec': 1000,
        'sec': 1000 * 1000,
    }
    return v * table[unit] if unit in table else None


def fmt(v):
    if v is None:
        return ''
    if isinstance(v, str):
        return v
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return f'{v:.6f}' if math.isfinite(v) else ''
    return str(v)


def parse_pct_triplets(payload: str, prefix: str) -> Dict[str, float]:
    result: Dict[str, float] = {}
    for key, value in re.findall(r'([<>]?=?\d+|>=64)=([\d.]+)%', payload):
        k = key.replace('>=', 'ge').replace('>', 'gt').replace('<=', 'le').replace('<', 'lt').replace('=', '')
        result[f'{prefix}_{k}_pct'] = float(value)
    return result


def elapsed_tag_to_sec(tag: str) -> int:
    h = int(re.search(r'(\d+)h', tag).group(1)) if re.search(r'(\d+)h', tag) else 0
    m = int(re.search(r'(\d+)m', tag).group(1)) if re.search(r'(\d+)m', tag) else 0
    s = int(re.search(r'(\d+)s', tag).group(1)) if re.search(r'(\d+)s', tag) else 0
    return h * 3600 + m * 60 + s


def parse_log(path: Path) -> Optional[Dict[str, object]]:
    m = FILENAME_RE.match(path.name)
    if not m:
        return None

    session, mode, threads, repeat, elapsed_tag = m.groups()
    row: Dict[str, object] = {k: None for k in FIELDNAMES}
    row.update({
        'session': session,
        'mode': mode.rstrip('_'),
        'threads': int(threads),
        'repeat': int(repeat),
        'elapsed_tag': elapsed_tag,
        'elapsed_sec': elapsed_tag_to_sec(elapsed_tag),
        'log_file': path.name,
    })

    lines = path.read_text(errors='replace').splitlines()
    pct_kind = None
    pct_unit = None

    for line in lines:
        if not line.strip():
            if pct_kind and not line.startswith('     |'):
                pct_kind = None
                pct_unit = None
            continue

        if not row['fio_version']:
            m2 = re.match(r'^fio-(.+)$', line)
            if m2:
                row['fio_version'] = m2.group(1).strip()
                continue

        if not row['threads_started']:
            m2 = START_THREADS_RE.match(line)
            if m2:
                row['threads_started'] = int(m2.group(1))
                continue

        if not row['job_name']:
            m2 = HEADER_RE.match(line)
            if m2:
                (
                    row['job_name'], _g, row['rw'], row['bs_r_min'], row['bs_r_max'],
                    row['bs_w_min'], row['bs_w_max'], row['bs_t_min'], row['bs_t_max'],
                    row['ioengine'], row['iodepth']
                ) = m2.groups()
                row['iodepth'] = int(row['iodepth'])
                continue

        if row['groupid'] is None:
            m2 = GROUP_RE.match(line)
            if m2:
                row['job_name'] = row['job_name'] or m2.group(1)
                row['groupid'] = int(m2.group(2))
                row['jobs'] = int(m2.group(3))
                row['err'] = int(m2.group(4))
                row['pid'] = int(m2.group(5))
                row['start_time'] = m2.group(6).strip()
                continue

        m2 = IOPS_RE.match(line)
        if m2 and row['iops'] is None:
            op, iops_v, iops_suf, bw_v, bw_u, bw_dec_v, bw_dec_u, io_v, io_u, run_ms = m2.groups()
            row['op'] = op
            row['iops'] = scaled_to_num(iops_v, iops_suf, 1000.0)
            row['bw_kib_per_sec'] = to_kib_per_sec(bw_v, bw_u)
            row['bw_mib_per_sec'] = (row['bw_kib_per_sec'] / 1024) if row['bw_kib_per_sec'] is not None else None
            row['io_mib'] = to_mib(io_v, io_u)
            row['run_msec'] = int(run_ms)
            row['bw_decimal_value'] = float(bw_dec_v)
            row['bw_decimal_unit'] = bw_dec_u
            row['io_decimal_value'] = float(io_v)
            row['io_decimal_unit'] = io_u
            continue

        m2 = LAT_RE.match(line)
        if m2:
            kind, unit, min_v, min_s, max_v, max_s, avg_v, std_v = m2.groups()
            row[f'{kind}_unit'] = unit
            row[f'{kind}_min_usec'] = lat_to_usec(min_v, unit, min_s or '')
            row[f'{kind}_max_usec'] = lat_to_usec(max_v, unit, max_s or '')
            row[f'{kind}_avg_usec'] = lat_to_usec(avg_v, unit)
            row[f'{kind}_stdev_usec'] = lat_to_usec(std_v, unit)
            continue

        m2 = PCT_HEADER_RE.match(line)
        if m2:
            pct_kind, pct_unit = m2.groups()
            continue

        if pct_kind and line.lstrip().startswith('|'):
            for pct, val, suf in re.findall(r'(\d+\.\d+)th=\[\s*([\d.]+)([kKmMgGtTpPeE]?)\]', line):
                suffix = PCT_MAP.get(pct)
                if suffix:
                    row[f'{pct_kind}_{suffix}_usec'] = lat_to_usec(val, pct_unit, suf)
            continue

        m2 = BW_STATS_RE.match(line)
        if m2:
            unit, min_v, max_v, per_v, avg_v, std_v, samples = m2.groups()
            row['bw_stat_unit'] = unit
            row['bw_min_kib_per_sec'] = to_kib_per_sec(min_v, unit)
            row['bw_max_kib_per_sec'] = to_kib_per_sec(max_v, unit)
            row['bw_avg_kib_per_sec'] = to_kib_per_sec(avg_v, unit)
            row['bw_stdev_kib_per_sec'] = to_kib_per_sec(std_v, unit)
            row['bw_per_pct'] = float(per_v)
            row['bw_samples'] = int(samples)
            continue

        m2 = IOPS_STATS_RE.match(line)
        if m2:
            min_v, max_v, avg_v, std_v, samples = m2.groups()
            row['iops_min'] = float(min_v)
            row['iops_max'] = float(max_v)
            row['iops_avg'] = float(avg_v)
            row['iops_stdev'] = float(std_v)
            row['iops_samples'] = int(samples)
            continue

        m2 = CPU_RE.match(line)
        if m2:
            usr, sys, ctx, majf, minf = m2.groups()
            row['cpu_usr_pct'] = float(usr)
            row['cpu_sys_pct'] = float(sys)
            row['ctx'] = int(ctx)
            row['cpu_majf'] = int(majf)
            row['cpu_minf'] = int(minf)
            continue

        if line.startswith('  IO depths'):
            row.update(parse_pct_triplets(line.split(':', 1)[1], 'io_depth'))
            continue

        if line.lstrip().startswith('submit'):
            row.update(parse_pct_triplets(line.split(':', 1)[1], 'submit'))
            continue

        if line.lstrip().startswith('complete'):
            row.update(parse_pct_triplets(line.split(':', 1)[1], 'complete'))
            continue

        m2 = ISSUED_RE.match(line)
        if m2:
            vals = [int(x) for x in m2.groups()]
            row['total_read_ops'], row['total_write_ops'], row['total_trim_ops'], row['total_sync_ops'] = vals[0:4]
            row['short_read_ops'], row['short_write_ops'], row['short_trim_ops'], row['short_sync_ops'] = vals[4:8]
            row['dropped_read_ops'], row['dropped_write_ops'], row['dropped_trim_ops'], row['dropped_sync_ops'] = vals[8:12]
            row['total_ops'] = sum(vals[0:4])
            continue

        m2 = LATENCY_RE.match(line)
        if m2:
            target, window, pct, depth = m2.groups()
            row['latency_target'] = int(target)
            row['latency_window'] = int(window)
            row['latency_percentile'] = float(pct)
            row['latency_depth'] = int(depth)
            continue

        m2 = RUN_RE.match(line)
        if m2:
            op_up, bw_v, bw_u, _bw_dec_v, _bw_dec_u, io_v, io_u, _io_dec_v, _io_dec_u, run_min, run_max = m2.groups()
            row['run_bw_kib_per_sec'] = to_kib_per_sec(bw_v, bw_u)
            row['run_bw_mib_per_sec'] = (row['run_bw_kib_per_sec'] / 1024) if row['run_bw_kib_per_sec'] is not None else None
            row['run_io_mib'] = to_mib(io_v, io_u)
            row['run_msec_min'] = int(run_min)
            row['run_msec_max'] = int(run_max)
            row['op'] = row['op'] or op_up.lower()
            continue

    if row['jobs'] is None:
        row['jobs'] = row['threads']

    if row['ops_per_job'] is None and row['jobs'] not in (None, 0) and row['total_ops'] is not None:
        row['ops_per_job'] = float(row['total_ops']) / float(row['jobs'])

    return row


def write_results(rows: List[Dict[str, object]], out_csv: Path) -> None:
    with out_csv.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: fmt(row.get(k)) for k in FIELDNAMES})


def build_summary(rows: List[Dict[str, object]]) -> Tuple[List[str], List[Dict[str, object]]]:
    non_numeric = {
        'session', 'mode', 'threads', 'repeat', 'elapsed_tag', 'log_file',
        'job_name', 'fio_version', 'start_time', 'rw', 'ioengine', 'op',
        'bw_stat_unit', 'slat_unit', 'clat_unit', 'lat_unit',
        'bs_r_min', 'bs_r_max', 'bs_w_min', 'bs_w_max', 'bs_t_min', 'bs_t_max'
    }
    summary_fields = ['session', 'mode', 'threads', 'runs', 'job_name', 'rw', 'ioengine', 'op']
    numeric_fields = [f for f in FIELDNAMES if f not in non_numeric and f not in {'repeat', 'elapsed_tag', 'log_file'}]
    for field in numeric_fields:
        if field not in {'session', 'mode', 'threads'}:
            summary_fields.append(f'avg_{field}')

    groups: Dict[Tuple[object, object, object], List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        groups[(row['session'], row['mode'], row['threads'])].append(row)

    out_rows: List[Dict[str, object]] = []
    for key in sorted(groups.keys(), key=lambda x: (str(x[1]), int(x[2]), str(x[0]))):
        items = groups[key]
        first = items[0]
        out: Dict[str, object] = {
            'session': key[0],
            'mode': key[1],
            'threads': key[2],
            'runs': len(items),
            'job_name': first.get('job_name') or '',
            'rw': first.get('rw') or '',
            'ioengine': first.get('ioengine') or '',
            'op': first.get('op') or '',
        }
        for field in numeric_fields:
            if field in {'session', 'mode', 'threads'}:
                continue
            vals: List[float] = []
            for item in items:
                v = item.get(field)
                if isinstance(v, (int, float)) and math.isfinite(v):
                    vals.append(float(v))
            out[f'avg_{field}'] = (sum(vals) / len(vals)) if vals else None
        out_rows.append({k: fmt(v) for k, v in out.items()})
    return summary_fields, out_rows


def write_summary(rows: List[Dict[str, object]], out_csv: Path) -> None:
    fields, out_rows = build_summary(rows)
    with out_csv.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in out_rows:
            writer.writerow(row)


def print_report(rows: List[Dict[str, object]]) -> None:
    print(f'parsed logs: {len(rows)}')
    missing = Counter()
    for field in FIELDNAMES:
        missing[field] = sum(1 for row in rows if row.get(field) in (None, ''))
    print('missing counts (top 40):')
    for field, count in missing.most_common(40):
        print(f'  {field}: {count}')

    core_fields = ['iops', 'bw_mib_per_sec', 'io_mib', 'run_msec', 'clat_avg_usec', 'cpu_usr_pct']
    print('core fields missing:')
    for field in core_fields:
        print(f'  {field}: {missing[field]} / {len(rows)}')


def resolve_session_dir(arg: str) -> Path:
    script_dir = Path(__file__).resolve().parent
    candidates = [
        Path(arg),
        Path.cwd() / arg,
        script_dir / arg,
        script_dir.parent / 'logs' / arg,
        Path.cwd() / 'logs' / arg,
    ]

    seen = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except FileNotFoundError:
            resolved = candidate.absolute()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        if candidate.is_dir():
            return candidate.resolve()

    raise SystemExit(
        '로그 디렉토리를 찾지 못했다: '\
        f'{arg}\n'
        '찾아본 경로:\n' + '\n'.join(f'  - {c}' for c in candidates)
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description='fio 텍스트 로그를 results.csv와 summary.csv로 변환한다.'
    )
    parser.add_argument('log_dir', help='세션 디렉토리 이름 또는 .log 파일들이 있는 디렉토리')
    parser.add_argument('--out-prefix', default=None, help='출력 파일 접두어. 기본값은 디렉토리 이름')
    parser.add_argument('--report', action='store_true', help='파싱 후 누락 통계를 stdout에 출력한다')
    args = parser.parse_args()

    session_dir = resolve_session_dir(args.log_dir)
    out_prefix = args.out_prefix or session_dir.name
    out_csv = session_dir / f'{out_prefix}_fio_results.csv'
    summary_csv = session_dir / f'{out_prefix}_fio_summary.csv'

    rows: List[Dict[str, object]] = []
    skipped: List[str] = []
    for path in sorted(session_dir.glob('*.log')):
        row = parse_log(path)
        if row is None:
            skipped.append(path.name)
            continue
        rows.append(row)

    if not rows:
        raise SystemExit(f'처리할 .log 파일이 없습니다: {session_dir}')

    write_results(rows, out_csv)
    write_summary(rows, summary_csv)

    print(f'raw csv : {out_csv}')
    print(f'summary : {summary_csv}')

    if skipped:
        print(f'skipped files: {len(skipped)}', file=sys.stderr)
        for name in skipped[:20]:
            print(f'  {name}', file=sys.stderr)

    if args.report:
        print_report(rows)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
