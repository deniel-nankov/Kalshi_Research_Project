#!/usr/bin/env python3
# ruff: noqa: E402
"""
Institutional-style full-terminal operations console for fix_orphan_tickers.py.

Streams every line from the worker, parses key metrics, and polls checkpoint / lock files.

Usage (pass-through args go to fix_orphan_tickers.py):

  uv run python scripts/orphan_backfill_dashboard.py -- \\
    --max 10000 --delay 0.2 \\
    --checkpoint data/kalshi/state/orphan_backfill_checkpoint.txt \\
    --repeat-until-done

Dashboard-only options must appear before ``--``:

  --log-lines N   Lines kept in the event buffer (default 500)
  --no-fullscreen Use inline layout instead of alternate screen
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

from rich import box
from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.kalshi_forward.paths import PROJECT_ROOT, STATE_DIR

ORPHAN_LOCK = STATE_DIR / "orphan_backfill.lock"
DEFAULT_CHECKPOINT = PROJECT_ROOT / "data/kalshi/state/orphan_backfill_checkpoint.txt"
FIX_SCRIPT = PROJECT_ROOT / "scripts" / "fix_orphan_tickers.py"

RE_ORPHANS = re.compile(r"Orphan tickers \(in trades but not in markets\):\s*([\d,]+)")
RE_BATCH = re.compile(r"BATCH\s+(\d+)\s+\(repeat until done\)", re.I)
RE_PROGRESS = re.compile(r"\[\s*(\d+)/(\d+)\s*\]")
RE_SUMMARY = re.compile(r"Summary:\s*fetched=(\d+),\s*404=(\d+),\s*errors=(\d+)")
RE_STEP = re.compile(r"STEP\s+(\d+):")
RE_CAP = re.compile(r"Capping this run to --max\s*([\d,]+)")


@dataclass
class Metrics:
    orphan_count: str | None = None
    batch: int | None = None
    step: str | None = None
    progress_i: int | None = None
    progress_n: int | None = None
    cap_max: str | None = None
    last_fetched: int | None = None
    last_404: int | None = None
    last_errors: int | None = None
    exit_code: int | None = None
    lines_total: int = 0


@dataclass
class DashboardState:
    log: deque[str] = field(default_factory=lambda: deque(maxlen=500))
    metrics: Metrics = field(default_factory=Metrics)
    lock: threading.Lock = field(default_factory=threading.Lock)


def _parse_checkpoint_arg(argv: list[str]) -> Path:
    p = DEFAULT_CHECKPOINT
    i = 0
    while i < len(argv):
        if argv[i] == "--checkpoint" and i + 1 < len(argv):
            p = Path(argv[i + 1])
            if not p.is_absolute():
                p = (PROJECT_ROOT / p).resolve()
            return p
        i += 1
    return p


def _checkpoint_display(checkpoint: Path) -> str:
    try:
        return str(checkpoint.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(checkpoint)


def _count_lines_quick(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        n = 0
        with path.open("rb") as f:
            for _ in f:
                n += 1
        return n
    except OSError:
        return None


def _read_lock() -> str:
    if not ORPHAN_LOCK.exists():
        return "absent"
    try:
        return ORPHAN_LOCK.read_text(encoding="utf-8", errors="replace").strip().replace("\n", " | ")
    except OSError as e:
        return f"read error: {e}"


def _apply_line(state: DashboardState, line: str) -> None:
    m = state.metrics
    line_stripped = line.strip()

    o = RE_ORPHANS.search(line)
    if o:
        m.orphan_count = o.group(1)

    b = RE_BATCH.search(line)
    if b:
        m.batch = int(b.group(1))

    s = RE_STEP.search(line_stripped)
    if s:
        m.step = f"STEP {s.group(1)}"

    c = RE_CAP.search(line)
    if c:
        m.cap_max = c.group(1).replace(",", "")

    pr = RE_PROGRESS.search(line)
    if pr:
        m.progress_i = int(pr.group(1))
        m.progress_n = int(pr.group(2))

    sm = RE_SUMMARY.search(line)
    if sm:
        m.last_fetched = int(sm.group(1))
        m.last_404 = int(sm.group(2))
        m.last_errors = int(sm.group(3))


def _reader_thread(proc: subprocess.Popen[str], state: DashboardState, done: threading.Event) -> None:
    assert proc.stdout is not None
    try:
        for line in iter(proc.stdout.readline, ""):
            with state.lock:
                state.log.append(line.rstrip("\n\r"))
                state.metrics.lines_total += 1
                _apply_line(state, line)
    finally:
        done.set()


def _build_metrics_table(m: Metrics, checkpoint: Path, cp_lines: int | None, lock_info: str) -> Table:
    t = Table.grid(padding=(0, 2))
    t.add_column(style="dim", justify="right")
    t.add_column(style="cyan", justify="left")

    def row(label: str, val: str | None) -> None:
        t.add_row(label, val if val is not None else "—")

    row("Orphans (last STEP 1)", m.orphan_count)
    row("Repeat batch", str(m.batch) if m.batch is not None else None)
    row("Phase", m.step)
    row("Batch cap (--max)", m.cap_max)
    if m.progress_i is not None and m.progress_n is not None:
        row("API progress", f"{m.progress_i:,} / {m.progress_n:,}")
    row("Last summary OK / 404 / err", _fmt_summary(m))
    row("Stdout lines", f"{m.lines_total:,}")
    row(
        "Checkpoint file",
        _checkpoint_display(checkpoint),
    )
    row("Checkpoint lines", f"{cp_lines:,}" if cp_lines is not None else "—")
    row("Lock", lock_info[:120] + ("…" if len(lock_info) > 120 else ""))
    return t


def _fmt_summary(m: Metrics) -> str | None:
    if m.last_fetched is None:
        return None
    return f"{m.last_fetched} / {m.last_404} / {m.last_errors}"


def _build_log_panel(state: DashboardState, max_visible: int) -> Panel:
    with state.lock:
        lines = list(state.log)
    tail = lines[-max_visible:] if len(lines) > max_visible else lines
    text = Text()
    for i, ln in enumerate(tail):
        if i:
            text.append("\n")
        style = "white"
        if "ERROR" in ln or "error" in ln.lower() and "errors=" not in ln:
            style = "red"
        elif "404" in ln:
            style = "yellow"
        elif "200 OK" in ln:
            style = "green"
        text.append(ln, style=style)
    return Panel(
        text,
        title="[bold]Event stream[/bold] (full stdout)",
        border_style="blue",
        box=box.ROUNDED,
        subtitle=f"{len(lines)} lines buffered",
    )


def _layout(state: DashboardState, checkpoint: Path, cp_lines: int | None, lock_info: str, log_visible: int) -> Layout:
    m = state.metrics
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = Panel(
        Align.center(
            Text.from_markup(
                f"[bold white]KALSHI · ORPHAN MARKET BACKFILL[/bold white]  ·  "
                f"[dim]{now}[/dim]  ·  [dim]prediction-market-data / operations console[/dim]"
            )
        ),
        style="on #1a1d24",
        border_style="blue",
        box=box.DOUBLE,
    )

    left = Panel(
        _build_metrics_table(m, checkpoint, cp_lines, lock_info),
        title="[bold]Run state[/bold]",
        border_style="cyan",
        box=box.ROUNDED,
    )

    right = _build_log_panel(state, log_visible)

    body = Layout(name="body")
    body.split_row(
        Layout(left, name="metrics", ratio=1, minimum_size=36),
        Layout(right, name="log", ratio=2, minimum_size=50),
    )

    exit_note = ""
    if m.exit_code is not None:
        color = "green" if m.exit_code == 0 else "red"
        exit_note = f"  ·  [{color}]Exit {m.exit_code}[/{color}]"

    footer = Panel(
        Text.from_markup(
            "[dim]Ctrl+C[/dim] stops the dashboard; the worker may keep running unless you terminate it.  "
            "[dim]Checkpoint grows on each successful ticker.[/dim]"
            + exit_note
        ),
        style="dim",
        border_style="dim",
    )

    root = Layout(name="root")
    root.split_column(
        Layout(header, size=5),
        Layout(body, name="main"),
        Layout(footer, size=3),
    )
    return root


def main() -> int:
    argv = sys.argv[1:]
    log_lines = 500
    fullscreen = True

    if "--" in argv:
        idx = argv.index("--")
        pre = argv[:idx]
        child = argv[idx + 1 :]
        p_pre = argparse.ArgumentParser(prog="orphan_backfill_dashboard.py", add_help=True)
        p_pre.add_argument("--log-lines", type=int, default=500, metavar="N", help="Lines in event buffer")
        p_pre.add_argument("--no-fullscreen", action="store_true", help="Do not use alternate screen buffer")
        known, rest = p_pre.parse_known_args(pre)
        if rest:
            print("Unknown dashboard args (use before --):", rest, file=sys.stderr)
            return 2
        log_lines = known.log_lines
        fullscreen = not known.no_fullscreen
    else:
        child = argv

    if not FIX_SCRIPT.is_file():
        print(f"Missing {FIX_SCRIPT}", file=sys.stderr)
        return 2

    if not sys.stdout.isatty():
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        return subprocess.call([sys.executable, "-u", str(FIX_SCRIPT), *child], cwd=str(PROJECT_ROOT), env=env)

    checkpoint = _parse_checkpoint_arg(child)
    state = DashboardState()
    state.log = deque(maxlen=max(50, log_lines))

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    cmd = [sys.executable, "-u", str(FIX_SCRIPT), *child]
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1,
        encoding="utf-8",
        errors="replace",
    )

    done = threading.Event()
    threading.Thread(target=_reader_thread, args=(proc, state, done), daemon=True).start()

    console = Console()
    # Reserve rows for log panel (terminal height minus chrome)
    term_h = console.size.height
    log_visible = max(12, min(log_lines, term_h - 14))

    t0 = time.monotonic()
    poll_cache: dict[str, float | int | None | str] = {
        "t": t0,
        "cp": _count_lines_quick(checkpoint),
        "lk": _read_lock(),
    }

    def refresh_outer() -> Layout:
        now = time.monotonic()
        if now - float(poll_cache["t"]) > 0.75:
            poll_cache["t"] = now
            poll_cache["cp"] = _count_lines_quick(checkpoint)
            poll_cache["lk"] = _read_lock()

        with state.lock:
            if proc.poll() is not None and state.metrics.exit_code is None:
                state.metrics.exit_code = proc.returncode
        cp_val = poll_cache["cp"]
        cp_lines: int | None = cp_val if isinstance(cp_val, (int, type(None))) else None
        return _layout(state, checkpoint, cp_lines, str(poll_cache["lk"]), log_visible)

    try:
        with Live(
            refresh_outer(),
            console=console,
            refresh_per_second=12,
            screen=fullscreen,
            transient=False,
        ) as live:
            while proc.poll() is None or not done.is_set():
                live.update(refresh_outer())
                time.sleep(1 / 12)
            # Drain
            time.sleep(0.15)
            live.update(refresh_outer())
    except KeyboardInterrupt:
        print(f"\n[Dashboard closed. Child PID {proc.pid} — still running unless you stopped it.]", file=sys.stderr)
        return 130

    # Wait for process
    rc = proc.wait()
    with state.lock:
        state.metrics.exit_code = rc

    console.print(Panel.fit(f"[bold]Worker finished[/bold] with exit code [cyan]{rc}[/cyan]", border_style="green" if rc == 0 else "red"))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
