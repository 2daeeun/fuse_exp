#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import sys
import textwrap

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
LOGS_ROOT = (SCRIPT_DIR / "../logs").resolve()


def print_help_and_exit(parser: argparse.ArgumentParser, code: int = 0) -> None:
    parser.print_help(sys.stdout if code == 0 else sys.stderr)
    raise SystemExit(code)


def import_pandas():
    try:
        import pandas as pd
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "pandas가 설치되어 있지 않다.\n"
            "Arch Linux: sudo pacman -S python-pandas\n"
            "pip: python3 -m pip install pandas"
        ) from e
    return pd


def import_pyplot():
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "matplotlib가 설치되어 있지 않다.\n"
            "Arch Linux: sudo pacman -S python-matplotlib\n"
            "pip: python3 -m pip install matplotlib"
        ) from e
    return plt


def find_csv_path(user_input: str) -> pathlib.Path:
    p = pathlib.Path(user_input).expanduser()

    if p.exists():
        return p.resolve()

    name = p.name
    if not name:
        raise FileNotFoundError(f"잘못된 CSV 경로: {user_input}")

    matches = sorted(LOGS_ROOT.rglob(name))
    if not matches:
        raise FileNotFoundError(
            f"CSV 파일을 찾지 못했다: {user_input}\n"
            f"검색 위치: {LOGS_ROOT}"
        )

    if len(matches) > 1:
        match_list = "\n".join(str(m) for m in matches)
        raise FileExistsError(
            "같은 이름의 CSV가 여러 개 있다. 더 정확한 파일명을 지정해야 한다.\n"
            f"{match_list}"
        )

    return matches[0].resolve()


def find_impl_col(df) -> str:
    for col in ("mode", "session"):
        if col not in df.columns:
            continue

        vals = df[col].astype(str).str.lower().unique().tolist()
        has_base = any(v in ("base", "baseline") or "base" in v for v in vals)
        has_uring = any("uring" in v for v in vals)

        if has_base or has_uring:
            return col

    raise ValueError("base/uring 구분 컬럼(mode/session)을 찾지 못했다.")


def find_iops_col(df) -> str:
    for col in ("avg_iops", "iops", "avg_iops_avg"):
        if col in df.columns:
            return col
    raise ValueError("IOPS 컬럼(avg_iops/iops/avg_iops_avg)을 찾지 못했다.")


def normalize_impl(v: str) -> str:
    s = str(v).strip().lower()

    if "uring" in s:
        return "uring"

    if "base" in s or "baseline" in s:
        return "base"

    return s


def filter_workload(df, keyword: str | None):
    if not keyword:
        return df

    cols = ("job_name", "workload", "rw", "op")
    pd = import_pandas()
    mask = pd.Series(False, index=df.index)

    for col in cols:
        if col in df.columns:
            mask |= df[col].astype(str).str.lower().str.contains(keyword.lower(), na=False)

    if mask.any():
        return df[mask].copy()

    return df


def load_and_prepare(csv_path: pathlib.Path, workload: str | None):
    pd = import_pandas()

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]

    if "threads" not in df.columns:
        raise ValueError("threads 컬럼이 없다.")

    df = filter_workload(df, workload)

    impl_col = find_impl_col(df)
    iops_col = find_iops_col(df)

    df["threads"] = pd.to_numeric(df["threads"], errors="coerce")
    df[iops_col] = pd.to_numeric(df[iops_col], errors="coerce")
    df["impl_norm"] = df[impl_col].map(normalize_impl)

    df = df.dropna(subset=["threads", iops_col]).copy()

    agg = (
        df.groupby(["threads", "impl_norm"], as_index=False)[iops_col]
        .mean()
        .rename(columns={"threads": "cores", iops_col: "iops"})
    )

    return agg, csv_path.name


def build_output_path(csv_path: pathlib.Path, target: str) -> pathlib.Path:
    stem = csv_path.stem

    suffix_map = {
        "both": "__both_iops.png",
        "base": "__base_iops.png",
        "uring": "__uring_iops.png",
    }
    suffix = suffix_map.get(target, "__iops.png")

    return csv_path.parent / f"{stem}{suffix}"


def build_title(csv_name: str, target: str) -> str:
    if target == "base":
        return f"base IOPS - {csv_name}"
    if target == "uring":
        return f"uring IOPS - {csv_name}"
    return f"base vs uring IOPS - {csv_name}"


def plot_iops(agg, title: str, target: str, output: pathlib.Path, show: bool) -> None:
    plt = import_pyplot()

    piv = agg.pivot(index="cores", columns="impl_norm", values="iops").sort_index()

    fig = plt.figure(figsize=(12, 6))
    plotted = False

    if target in ("base", "both") and "base" in piv.columns:
        plt.plot(piv.index, piv["base"], marker="o", label="base")
        plotted = True

    if target in ("uring", "both") and "uring" in piv.columns:
        plt.plot(piv.index, piv["uring"], marker="o", label="uring")
        plotted = True

    if not plotted:
        raise ValueError(f"요청한 target={target} 에 해당하는 데이터가 없다.")

    plt.xlabel("Cores")
    plt.ylabel("IOPS")
    plt.title(title)
    plt.xticks(list(piv.index), rotation=45, ha="right")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output, dpi=200)
    print(f"saved: {output}")

    if show:
        plt.show()
    else:
        plt.close(fig)


def parse_args() -> argparse.ArgumentParser:
    examples = textwrap.dedent(
        f"""\
        examples:
          python3 plot_fio_iops.py
          python3 plot_fio_iops.py --help
          python3 plot_fio_iops.py ./log_0324_044243_dio_off_nvm_both_fio_summary.csv
          python3 plot_fio_iops.py ./log_0324_044243_dio_off_nvm_both_fio_summary.csv --target both
          python3 plot_fio_iops.py ./log_0324_044243_dio_off_nvm_both_fio_summary.csv --target uring
          python3 plot_fio_iops.py ./log_0324_044243_dio_off_nvm_both_fio_summary.csv --target base
          python3 plot_fio_iops.py ./log_0324_044243_dio_off_nvm_both_fio_summary.csv --workload randwrite --show

        behavior:
          - 입력한 CSV 경로가 존재하면 그대로 사용
          - 입력한 CSV가 현재 위치에 없으면 {LOGS_ROOT} 아래에서 같은 파일명을 재귀 탐색
          - 출력 PNG는 찾은 CSV와 같은 디렉토리에 저장
        """
    )

    parser = argparse.ArgumentParser(
        description="fio summary CSV에서 base/uring IOPS 그림 생성",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=examples,
    )
    parser.add_argument(
        "csv",
        nargs="?",
        help="CSV 파일명 또는 경로. 없으면 ../logs 아래에서 같은 파일명을 찾는다.",
    )
    parser.add_argument(
        "--target",
        choices=("base", "uring", "both"),
        default="both",
        help="그릴 대상 선택 (기본값: both)",
    )
    parser.add_argument(
        "--workload",
        default=None,
        help="workload 필터 문자열 예: randwrite, create",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="그림을 화면에 표시",
    )
    return parser


def main() -> int:
    parser = parse_args()
    args = parser.parse_args()

    if not args.csv:
        print_help_and_exit(parser, 0)

    try:
        csv_path = find_csv_path(args.csv)
        agg, csv_name = load_and_prepare(csv_path, args.workload)
        out_path = build_output_path(csv_path, args.target)
        title = build_title(csv_name, args.target)

        plot_iops(
            agg=agg,
            title=title,
            target=args.target,
            output=out_path,
            show=args.show,
        )
        return 0

    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

