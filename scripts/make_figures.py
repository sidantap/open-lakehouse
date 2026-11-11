"""Draw the figures in docs/figures/ from a loaded lakehouse.

    uv sync --extra docs
    LAKEHOUSE_HOME=lakehouse_data uv run python scripts/make_figures.py

Plain matplotlib, deliberately styled like a printed technical report: boxed axes, inward ticks,
dotted grid, framed legends.
"""

import json
import os
import zipfile
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, MultipleLocator, PercentFormatter  # noqa: E402

from lakehouse.catalog import get_catalog, register  # noqa: E402
from lakehouse.config import Config  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "docs" / "figures"
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"

plt.rcParams.update(
    {
        "font.family": ["Arial", "Liberation Sans", "DejaVu Sans"],
        "font.size": 9,
        "axes.titlesize": 9.5,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.edgecolor": "#222222",
        "axes.linewidth": 0.8,
        "axes.grid": True,
        "axes.axisbelow": True,
        "grid.color": "#9a9a9a",
        "grid.linestyle": ":",
        "grid.linewidth": 0.6,
        "xtick.direction": "in",
        "ytick.direction": "in",
        "xtick.top": True,
        "ytick.right": True,
        "xtick.major.size": 3.5,
        "ytick.major.size": 3.5,
        "legend.frameon": True,
        "legend.edgecolor": "#222222",
        "legend.fancybox": False,
        "legend.framealpha": 1,
        "legend.fontsize": 8.5,
        "svg.fonttype": "none",
        "figure.dpi": 100,
    }
)
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
label = lambda ym: f"{MONTHS[int(ym[5:]) - 1]} {ym[:4]}"  # noqa: E731


def save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / f"{name}.svg", bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)
    print("wrote", OUT / f"{name}.svg")


def load_times(cfg, cat):
    """Seconds per month: download (from the landing manifest), bronze and silver (from ops.load_audit)."""
    audit = cat.load_table("ops.load_audit").scan().to_arrow().to_pylist()
    latest = {}
    for r in sorted(audit, key=lambda r: r["started_at"]):
        if r["status"] == "succeeded" and r["source_month"]:
            latest[(r["source_month"], r["stage"])] = r["duration_ms"] / 1000
    months, download, bronze, silver = [], [], [], []
    for m in sorted(cfg.landing.glob("ontime_*.json")):
        month = m.stem.removeprefix("ontime_")
        months.append(label(month))
        download.append(json.loads(m.read_text()).get("download_seconds") or 0)
        bronze.append(latest.get((month, "bronze"), 0))
        silver.append(latest.get((month, "silver"), 0))
    fig, ax = plt.subplots(figsize=(6.4, 3.0))
    x = range(len(months))
    ax.bar(x, download, 0.6, label="Download from BTS", color=BLUE, edgecolor="#222222", linewidth=0.5)
    ax.bar(
        x, bronze, 0.6, bottom=download, label="Bronze load", color=ORANGE, edgecolor="#222222", linewidth=0.5
    )
    ax.bar(
        x,
        silver,
        0.6,
        bottom=[a + b for a, b in zip(download, bronze, strict=True)],
        label="Silver (contract checks)",
        color=AQUA,
        edgecolor="#222222",
        linewidth=0.5,
    )
    for i in x:
        tot = download[i] + bronze[i] + silver[i]
        ax.text(i, tot + 0.8, f"{tot:.1f} s", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(list(x), months)
    ax.set_ylabel("seconds")
    ax.set_ylim(0, max(d + b + s for d, b, s in zip(download, bronze, silver, strict=True)) * 1.45)
    ax.set_title("Time to land and load each month (laptop-class container, 4 vCPU)")
    ax.legend(loc="upper center", ncol=3)
    save(fig, "load_times")


def storage(cfg, cat):
    csv_bytes = 0
    for z in cfg.landing.glob("ontime_*.zip"):
        with zipfile.ZipFile(z) as zf:
            csv_bytes += sum(i.file_size for i in zf.infolist() if i.filename.lower().endswith(".csv"))
    zipped = sum(z.stat().st_size for z in cfg.landing.glob("ontime_*.zip"))
    rows = [("Source CSV, unzipped", csv_bytes, "#bdbdbd"), ("Source zips (landing)", zipped, "#bdbdbd")]
    for ident, name in (
        ("bronze.flights", "bronze.flights (all columns, text)"),
        ("silver.flights", "silver.flights (47 columns)"),
        ("gold.fact_flight", "gold.fact_flight (28 columns)"),
    ):
        files = cat.load_table(ident).inspect.files().select(["file_size_in_bytes"]).to_pylist()
        rows.append((name, sum(f["file_size_in_bytes"] for f in files), BLUE))
    fig, ax = plt.subplots(figsize=(6.4, 2.5))
    names = [r[0] for r in rows][::-1]
    sizes = [r[1] / 1e6 for r in rows][::-1]
    ax.barh(names, sizes, 0.62, color=[r[2] for r in rows][::-1], edgecolor="#222222", linewidth=0.5)
    for i, v in enumerate(sizes):
        ax.text(v + max(sizes) * 0.01, i, f"{v:,.0f} MB", va="center", fontsize=8)
    ax.set_xlim(0, max(sizes) * 1.15)
    ax.set_xlabel("megabytes on disk")
    ax.grid(axis="y", visible=False)
    ax.set_title("Six months of flights at each stage (3.45 million rows)")
    save(fig, "storage")


def otp_by_carrier(con):
    rows = con.execute("""
        WITH top AS (SELECT carrier_code FROM gold_mart_carrier_month
                     GROUP BY 1 ORDER BY sum(scheduled) DESC LIMIT 4)
        SELECT year_month, carrier_name, on_time_rate
        FROM gold_mart_carrier_month JOIN top USING (carrier_code)
        UNION ALL
        SELECT year_month, 'All carriers', sum(on_time) / sum(scheduled - cancelled - diverted)
        FROM gold_mart_carrier_month GROUP BY 1
        ORDER BY 2, 1""").fetchall()
    series = {}
    for ym, name, rate in rows:
        series.setdefault(name, []).append((ym, rate))
    fig, ax = plt.subplots(figsize=(6.4, 3.2))
    styles = [(BLUE, "o"), (ORANGE, "s"), (AQUA, "^"), (YELLOW, "D")]
    carriers = [n for n in series if n != "All carriers"]
    carriers.sort(key=lambda n: -sum(r for _, r in series[n]))
    for name, (color, marker) in zip(carriers, styles, strict=False):
        xs, ys = zip(*series[name], strict=True)
        ax.plot(
            [label(x)[:3] for x in xs],
            ys,
            color=color,
            marker=marker,
            markersize=4.5,
            linewidth=1.4,
            label=name,
        )
    xs, ys = zip(*series["All carriers"], strict=True)
    ax.plot(
        [label(x)[:3] for x in xs],
        ys,
        color="#222222",
        linestyle="--",
        linewidth=1.2,
        marker="x",
        markersize=5,
        label="All 14 carriers",
    )
    ax.yaxis.set_major_locator(MultipleLocator(0.02))
    ax.yaxis.set_major_formatter(PercentFormatter(1.0, decimals=0))
    ax.set_ylabel("arrived < 15 min late")
    ax.set_title("On-time arrival rate, four largest carriers, 2025")
    ax.legend(loc="lower left", ncol=2)
    save(fig, "otp_by_carrier")


def delay_histogram(con):
    rows = con.execute("""
        SELECT floor(least(greatest(arr_delay_min, -60), 239) / 5) * 5 AS bin, count(*)
        FROM gold_fact_flight WHERE NOT cancelled AND NOT diverted GROUP BY 1 ORDER BY 1""").fetchall()
    total, late60 = con.execute("""SELECT count(*), count(*) FILTER (WHERE arr_delay_min >= 60)
        FROM gold_fact_flight WHERE NOT cancelled AND NOT diverted""").fetchone()
    fig, ax = plt.subplots(figsize=(6.4, 3.0))
    ax.bar(
        [b for b, _ in rows],
        [n for _, n in rows],
        width=5,
        align="edge",
        color="#9ec5f4",
        edgecolor="#222222",
        linewidth=0.4,
    )
    ax.axvline(15, color=ORANGE, linewidth=1.2, linestyle="--")
    ax.text(17, ax.get_ylim()[1] * 0.92, "15 min: DOT on-time threshold", color="#222222", fontsize=8)
    ax.text(
        150,
        ax.get_ylim()[1] * 0.45,
        f"{late60 / total:.1%} of completed flights\narrived 60+ min late\n"
        "(240+ min is grouped at the right edge)",
        fontsize=8,
        ha="center",
    )
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:,.0f}K"))
    ax.set_xlim(-60, 240)
    ax.set_xlabel("arrival delay, minutes (negative = early)")
    ax.set_ylabel("flights per 5-minute bin")
    ax.set_title(f"Arrival delay of {total:,} completed flights, Jan–Jun 2025")
    save(fig, "delay_histogram")


def main():
    cfg = Config.from_env()
    cat = get_catalog(cfg)
    load_times(cfg, cat)
    storage(cfg, cat)
    with duckdb.connect() as con:
        register(con, cat, ["gold.mart_carrier_month", "gold.fact_flight"])
        otp_by_carrier(con)
        delay_histogram(con)


if __name__ == "__main__":
    os.environ.setdefault("LAKEHOUSE_HOME", "lakehouse_data")
    main()
