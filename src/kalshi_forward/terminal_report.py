"""
Institutional-style terminal output: banners, tables, timestamps, and notices.

Dependency-free (no Rich). Uses ANSI bold/dim when stdout is a TTY.
"""

from __future__ import annotations

import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence


def _use_color() -> bool:
    return sys.stdout.isatty()


def _bold(s: str) -> str:
    return f"\033[1m{s}\033[0m" if _use_color() else s


def _dim(s: str) -> str:
    return f"\033[2m{s}\033[0m" if _use_color() else s


def _green(s: str) -> str:
    return f"\033[32m{s}\033[0m" if _use_color() else s


def _yellow(s: str) -> str:
    return f"\033[33m{s}\033[0m" if _use_color() else s


def _red(s: str) -> str:
    return f"\033[31m{s}\033[0m" if _use_color() else s


WIDTH = 80


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def hline(char: str = "─", width: int = WIDTH) -> None:
    print(char * width)


def blank() -> None:
    print()


def banner(title: str, subtitle: str | None = None, *, variant: str = "double") -> None:
    """Print a prominent section header."""
    top = "╔" + "═" * (WIDTH - 2) + "╗" if variant == "double" else "┌" + "─" * (WIDTH - 2) + "┐"
    mid = "║" if variant == "double" else "│"
    bot = "╚" + "═" * (WIDTH - 2) + "╝" if variant == "double" else "└" + "─" * (WIDTH - 2) + "┘"
    print()
    print(top)
    t = title.upper()
    if len(t) > WIDTH - 6:
        t = t[: WIDTH - 9] + "…"
    inner = f" {t} "
    print(f"{mid}{inner:^{WIDTH - 2}}{mid}")
    if subtitle:
        st = subtitle if len(subtitle) <= WIDTH - 6 else subtitle[: WIDTH - 9] + "…"
        print(f"{mid}  {_dim(st)}{mid}")
    print(bot)


def simple_banner(title: str) -> None:
    """Single-line emphasized header."""
    blank()
    hline("═")
    print(_bold(f"  {title}"))
    hline("═")


def kv_table(
    rows: Sequence[tuple[str, str]],
    title: str | None = None,
    *,
    value_align: str = "left",
) -> None:
    """Print a two-column table. Labels left; values aligned."""
    if title:
        print(_dim(f"  {title}"))
    if not rows:
        print(_dim("  (empty)"))
        return
    lw = max(len(r[0]) for r in rows) + 1
    lw = min(lw, 36)
    for k, v in rows:
        k = k[:35]
        v_lines = str(v).splitlines() or [""]
        for j, vl in enumerate(v_lines):
            if j == 0:
                label = f"{k}:"
                pad = lw - len(label)
                if value_align == "right":
                    print(f"  {label}{' ' * max(0, pad)}{vl}")
                else:
                    print(f"  {label:<{lw}} {vl}")
            else:
                print(f"  {' ' * lw} {vl}")


def notice(msg: str) -> None:
    print(f"  {_bold('•')} {msg}")


def success(msg: str) -> None:
    print(f"  {_green('✓')} {msg}")


def warn(msg: str) -> None:
    print(f"  {_yellow('!')} {msg}")


def err(msg: str) -> None:
    print(f"  {_red('✗')} {msg}")


def preflight_disk(data_path: Path) -> tuple[int, int]:
    """Return (free_bytes, total_bytes) for filesystem containing data_path."""
    try:
        usage = shutil.disk_usage(data_path.resolve())
        return usage.free, usage.total
    except OSError:
        return 0, 0


def format_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024.0 or unit == "TiB":
            return f"{n:.2f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n:.2f} PiB"


def bullet_list(items: Iterable[str], indent: str = "  ") -> None:
    for item in items:
        print(f"{indent}{_bold('•')} {item}")


def phase(n: int, name: str, detail: str | None = None) -> None:
    blank()
    print(_bold(f"── PHASE {n}: {name} ──"))
    if detail:
        print(_dim(f"   {detail}"))
