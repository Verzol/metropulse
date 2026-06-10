import json
import os
from typing import Any
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
import streamlit.components.v1 as components


load_dotenv()

API_URL = os.getenv("DASHBOARD_API_URL", "http://127.0.0.1:8000").rstrip("/")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ML_DEMO_DIR = PROJECT_ROOT / "ml" / "demo" / "data_demo"
ML_PREDICTIONS_PATH = ML_DEMO_DIR / "demand_december_predictions.csv"
ML_METRICS_PATH = PROJECT_ROOT / "ml" / "logs" / "demand_metrics.json"

PAYMENT_TYPE_LABELS = {
    1: "Thẻ tín dụng",
    2: "Tiền mặt",
    3: "Miễn phí",
    4: "Tranh chấp",
    5: "Không xác định",
    6: "Chuyến hủy",
}
DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_LABELS_VI = {
    "Monday": "Thứ Hai",
    "Tuesday": "Thứ Ba",
    "Wednesday": "Thứ Tư",
    "Thursday": "Thứ Năm",
    "Friday": "Thứ Sáu",
    "Saturday": "Thứ Bảy",
    "Sunday": "Chủ Nhật",
}


st.set_page_config(
    page_title="Bảng điều khiển MetroPulse",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        :root {
            --mp-bg: #eef2f6;
            --mp-surface: #ffffff;
            --mp-surface-soft: #f7f9fc;
            --mp-border: rgba(37, 48, 63, 0.12);
            --mp-text: #0f172a;
            --mp-muted: #5c6b7d;
            --mp-primary: #12324a;
            --mp-secondary: #14766f;
            --mp-accent: #b45309;
            --mp-danger: #b42318;
            --mp-shadow: 0 1px 2px rgba(15, 23, 42, 0.06), 0 8px 24px rgba(15, 23, 42, 0.05);
        }

        html, body, [class*="css"] {
            background: var(--mp-bg);
            color: var(--mp-text);
        }

        #MainMenu, footer, [data-testid="stDecoration"], [data-testid="stToolbar"] {
            display: none;
        }
        [data-testid="stHeader"] {
            height: 0;
            background: transparent;
        }
        [data-testid="stSidebar"] {
            background: #20242c;
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] .stCaptionContainer {
            color: #f8fafc;
        }
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: #ffffff;
            letter-spacing: 0;
        }
        [data-testid="stSidebar"] .stDataFrame {
            border: 1px solid rgba(255, 255, 255, 0.08);
        }
        [data-testid="stSidebar"] code {
            color: #8de1d3;
            background: #161a21;
            border: 1px solid rgba(255, 255, 255, 0.06);
        }
        .block-container {
            max-width: 1540px;
            padding-top: 1rem;
            padding-bottom: 1.5rem;
            padding-left: 1.3rem;
            padding-right: 1.3rem;
        }
        .stApp {
            background: linear-gradient(180deg, #f3f6fa 0%, #eef2f6 100%);
        }
        .mp-hero {
            background: #ffffff;
            border: 1px solid var(--mp-border);
            border-left: 5px solid var(--mp-secondary);
            border-radius: 8px;
            padding: 1rem 1.15rem;
            color: var(--mp-text);
            box-shadow: var(--mp-shadow);
            margin-bottom: 0.8rem;
            display: grid;
            grid-template-columns: minmax(0, 1fr) auto;
            gap: 1rem;
            align-items: center;
        }
        .mp-hero h1 {
            margin: 0;
            font-size: 1.55rem;
            line-height: 1.15;
            letter-spacing: 0;
        }
        .mp-hero p {
            margin: 0.28rem 0 0;
            color: var(--mp-muted);
            font-size: 0.95rem;
            max-width: 960px;
        }
        .mp-status {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            color: #0f513d;
            background: #dff7ed;
            border: 1px solid #b7ebd3;
            border-radius: 999px;
            padding: 0.38rem 0.65rem;
            font-size: 0.82rem;
            font-weight: 700;
        }
        .mp-card {
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: 8px;
            padding: 1rem 1rem 0.9rem;
            box-shadow: var(--mp-shadow);
        }
        .mp-card-label {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--mp-muted);
            margin-bottom: 0.35rem;
        }
        .mp-card-value {
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--mp-text);
            line-height: 1.1;
        }
        .mp-card-hint {
            margin-top: 0.35rem;
            color: #475569;
            font-size: 0.9rem;
        }
        .mp-section-title {
            font-size: 1rem;
            font-weight: 700;
            color: var(--mp-text);
            margin-bottom: 0.3rem;
        }
        .mp-section-caption {
            color: var(--mp-muted);
            margin-bottom: 0.9rem;
        }
        .mp-metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
            gap: 0.65rem;
            margin: 0.75rem 0 0.8rem;
        }
        .mp-metric-card {
            background: #ffffff;
            border: 1px solid var(--mp-border);
            border-left: 4px solid var(--mp-primary);
            border-radius: 8px;
            padding: 0.8rem 0.85rem 0.75rem;
            box-shadow: var(--mp-shadow);
            position: relative;
            overflow: hidden;
        }
        .mp-metric-label {
            text-transform: uppercase;
            letter-spacing: 0.04em;
            font-size: 0.72rem;
            color: var(--mp-muted);
            margin-bottom: 0.32rem;
        }
        .mp-metric-value {
            font-size: 1.35rem;
            font-weight: 800;
            color: var(--mp-text);
            line-height: 1.1;
        }
        .mp-metric-hint {
            margin-top: 0.3rem;
            color: #475569;
            font-size: 0.82rem;
        }
        .mp-panel {
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: 8px;
            padding: 0.9rem 0.95rem 1rem;
            box-shadow: var(--mp-shadow);
            margin-bottom: 0.8rem;
        }
        .mp-panel-title {
            font-size: 0.98rem;
            font-weight: 700;
            color: var(--mp-text);
        }
        .mp-panel-subtitle {
            color: var(--mp-muted);
            font-size: 0.9rem;
            margin-top: 0.2rem;
            margin-bottom: 0.8rem;
        }
        .mp-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
            margin-bottom: 0.4rem;
            color: #334155;
            font-size: 0.88rem;
        }
        .mp-legend-item {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
        }
        .mp-legend-dot {
            width: 10px;
            height: 10px;
            border-radius: 999px;
            display: inline-block;
        }
        .mp-chart {
            width: 100%;
            height: 235px;
            display: block;
            overflow: visible;
        }
        .mp-grid-line {
            stroke: rgba(148, 163, 184, 0.28);
            stroke-width: 1;
        }
        .mp-axis-label {
            fill: var(--mp-muted);
            font-size: 11px;
        }
        .mp-bar-list {
            display: grid;
            gap: 0.52rem;
        }
        .mp-bar-row {
            display: grid;
            grid-template-columns: minmax(96px, 1.25fr) minmax(110px, 2.4fr) minmax(58px, 0.6fr);
            gap: 0.6rem;
            align-items: center;
        }
        .mp-bar-label {
            color: var(--mp-text);
            font-weight: 600;
            font-size: 0.86rem;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .mp-bar-track {
            background: #e8edf3;
            border-radius: 4px;
            height: 8px;
            overflow: hidden;
        }
        .mp-bar-fill {
            height: 100%;
            border-radius: 4px;
        }
        .mp-bar-value {
            text-align: right;
            color: #334155;
            font-weight: 600;
        }
        .mp-empty {
            color: var(--mp-muted);
            background: var(--mp-surface-soft);
            border: 1px dashed rgba(148, 163, 184, 0.35);
            border-radius: 8px;
            padding: 0.9rem 1rem;
        }
        .mp-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.92rem;
        }
        .mp-table th {
            text-align: left;
            color: var(--mp-muted);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            padding: 0.55rem 0.35rem;
            border-bottom: 1px solid rgba(148, 163, 184, 0.22);
        }
        .mp-table td {
            padding: 0.6rem 0.35rem;
            border-bottom: 1px solid rgba(226, 232, 240, 0.8);
            color: var(--mp-text);
        }
        .mp-summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 0.65rem;
            margin: 0.2rem 0 0.8rem;
        }
        .mp-summary-card {
            background: #ffffff;
            border: 1px solid var(--mp-border);
            border-radius: 8px;
            padding: 0.8rem 0.9rem;
            box-shadow: var(--mp-shadow);
        }
        .mp-summary-label {
            font-size: 0.76rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--mp-muted);
            margin-bottom: 0.35rem;
        }
        .mp-summary-value {
            font-size: 1.2rem;
            font-weight: 800;
            color: var(--mp-text);
            line-height: 1.2;
        }
        .mp-summary-note {
            margin-top: 0.3rem;
            color: #475569;
            font-size: 0.88rem;
        }
        .mp-badge-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin: 0.15rem 0 0.8rem;
        }
        .mp-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            background: #ffffff;
            border: 1px solid var(--mp-border);
            border-radius: 6px;
            padding: 0.35rem 0.65rem;
            color: var(--mp-text);
            font-size: 0.84rem;
        }
        .mp-badge strong {
            color: var(--mp-primary);
            font-weight: 700;
        }
        .mp-signal-list {
            margin: 0.6rem 0 0;
            padding-left: 1.1rem;
            color: var(--mp-text);
        }
        .mp-signal-list li {
            margin-bottom: 0.55rem;
            line-height: 1.45;
        }
        .stDataFrame, .stDataFrame div {
            border-radius: 8px;
        }
        div[data-testid="stTabs"] button {
            font-weight: 700;
            letter-spacing: 0;
        }
        .stDownloadButton button {
            border-radius: 6px;
            border: 1px solid var(--mp-border);
            background: #ffffff;
            color: var(--mp-primary);
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(f"{API_URL}{path}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


@st.cache_data(ttl=300)
def load_meta() -> dict[str, Any]:
    return api_get("/api/meta")


@st.cache_data(ttl=300)
def load_summary(start_month: str, end_month: str) -> dict[str, Any]:
    return api_get("/api/summary", {"start_month": start_month, "end_month": end_month})


@st.cache_data(ttl=300)
def load_hourly(start_month: str, end_month: str, limit: int) -> pd.DataFrame:
    rows = api_get(
        "/api/hourly-demand",
        {"start_month": start_month, "end_month": end_month, "limit": limit},
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["pickup_hour"] = pd.to_datetime(df["pickup_hour"])
    return df


@st.cache_data(ttl=300)
def load_zones(limit: int) -> pd.DataFrame:
    return pd.DataFrame(api_get("/api/zone-summary", {"limit": limit}))


@st.cache_data(ttl=300)
def load_payment_tip(start_month: str, end_month: str) -> pd.DataFrame:
    return pd.DataFrame(
        api_get(
            "/api/payment-tip-summary",
            {"start_month": start_month, "end_month": end_month},
        )
    )


def metric_value(value: Any, suffix: str = "") -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    return f"{value:,}{suffix}" if isinstance(value, int) else str(value)


def parse_month(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        return pd.to_datetime(f"{value}-01")
    except ValueError:
        return None


def format_month(value: str | None) -> str:
    parsed = parse_month(value)
    return parsed.strftime("%m/%Y") if parsed is not None else (value or "-")


def format_percent(value: float | int | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}f}%"


@st.cache_data(ttl=3600)
def load_json_payload(path: str) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    with file_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@st.cache_data(ttl=3600)
def load_ml_metrics() -> dict[str, Any]:
    return load_json_payload(str(ML_METRICS_PATH))


@st.cache_data(ttl=3600)
def load_ml_predictions() -> pd.DataFrame:
    if not ML_PREDICTIONS_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(ML_PREDICTIONS_PATH)
    if not df.empty:
        df["pickup_hour"] = pd.to_datetime(df["pickup_hour"])
        df["hour_of_day"] = df["pickup_hour"].dt.hour
        df["day_name"] = pd.Categorical(df["pickup_hour"].dt.day_name(), categories=DAY_ORDER, ordered=True)
        df["day_name_vi"] = df["day_name"].astype(str).map(DAY_LABELS_VI).fillna(df["day_name"].astype(str))
        df["zone_type"] = "Bình thường"
        df.loc[df["is_airport"] == 1, "zone_type"] = "Sân bay"
        df.loc[df["is_manhattan_core"] == 1, "zone_type"] = "Manhattan core"
        df["forecast_error_pct"] = df["pct_error"]
        df["forecast_abs_error"] = df["abs_error"]
    return df


@st.cache_data(ttl=300)
def load_all_zones() -> list[dict[str, Any]]:
    try:
        return api_get("/api/zones-all")
    except Exception:
        return []


@st.cache_data(ttl=300)
def load_all_ml_metrics() -> dict[str, Any]:
    try:
        return api_get("/api/ml-metrics-all")
    except Exception:
        return {}


def month_bounds(values: list[str], start_month: str, end_month: str) -> tuple[str, str]:
    if not values:
        return start_month, end_month
    if start_month in values and end_month in values and values.index(start_month) <= values.index(end_month):
        return start_month, end_month
    return values[0], values[-1]


def make_card(label: str, value: str, hint: str = "") -> str:
    hint_html = f'<div class="mp-card-hint">{hint}</div>' if hint else ""
    return f"""
        <div class="mp-card">
            <div class="mp-card-label">{label}</div>
            <div class="mp-card-value">{value}</div>
            {hint_html}
        </div>
    """


def normalize_payment_label(value: Any) -> str:
    try:
        return PAYMENT_TYPE_LABELS.get(int(value), f"Type {int(value)}")
    except (TypeError, ValueError):
        return str(value)


def safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def df_to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def human_series_label(value: Any) -> str:
    if pd.isna(value):
        return "Unknown"
    if isinstance(value, (pd.Timestamp,)):
        return value.strftime("%b %Y")
    return str(value)


def render_metric_strip(items: list[tuple[str, str, str]]) -> str:
    cards = "".join(
        f'<div class="mp-metric-card"><div class="mp-metric-label">{label}</div><div class="mp-metric-value">{value}</div><div class="mp-metric-hint">{hint}</div></div>'
        for label, value, hint in items
    )
    return f'<div class="mp-metric-grid">{cards}</div>'


def render_bar_panel(title: str, subtitle: str, labels: list[str], values: list[float], accent: str = "#1d4ed8") -> str:
    if not values:
        return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-panel-subtitle">{subtitle}</div><div class="mp-empty">Không có dữ liệu.</div></div>'

    max_value = max(values) or 1
    rows = []
    for label, value in zip(labels, values):
        width = max(4, (value / max_value) * 100)
        rows.append(
            f'<div class="mp-bar-row"><div class="mp-bar-label">{label}</div><div class="mp-bar-track"><div class="mp-bar-fill" style="width:{width:.1f}%; background:{accent};"></div></div><div class="mp-bar-value">{metric_value(value)}</div></div>'
        )
    return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-panel-subtitle">{subtitle}</div><div class="mp-bar-list">{"".join(rows)}</div></div>'


def render_line_panel(
    title: str,
    subtitle: str,
    x_labels: list[str],
    series: list[tuple[str, list[float], str]],
) -> str:
    if not x_labels or not series:
        return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-panel-subtitle">{subtitle}</div><div class="mp-empty">Không có dữ liệu.</div></div>'

    width = 900
    height = 260
    left_pad = 40
    top_pad = 20
    right_pad = 24
    bottom_pad = 36
    plot_width = width - left_pad - right_pad
    plot_height = height - top_pad - bottom_pad
    max_value = max((max(values) for _, values, _ in series if values), default=1)
    if max_value <= 0:
        max_value = 1

    def points(values: list[float]) -> str:
        if len(values) == 1:
            y = top_pad + plot_height - (values[0] / max_value) * plot_height
            return f"{left_pad},{y:.1f}"
        coords = []
        for index, value in enumerate(values):
            x = left_pad + (index * plot_width / (len(values) - 1))
            y = top_pad + plot_height - (value / max_value) * plot_height
            coords.append(f"{x:.1f},{y:.1f}")
        return " ".join(coords)

    grid_lines = []
    for step in range(5):
        y = top_pad + (plot_height * step / 4)
        grid_lines.append(f'<line x1="{left_pad}" y1="{y:.1f}" x2="{width - right_pad}" y2="{y:.1f}" class="mp-grid-line" />')

    x_ticks = []
    tick_positions = [0, len(x_labels) // 2, len(x_labels) - 1] if len(x_labels) > 2 else list(range(len(x_labels)))
    for index in sorted(set(tick_positions)):
        x = left_pad + (index * plot_width / max(len(x_labels) - 1, 1))
        x_ticks.append(
            f'<text x="{x:.1f}" y="{height - 10}" class="mp-axis-label" text-anchor="middle">{x_labels[index]}</text>'
        )

    legend = "".join(
        f'<span class="mp-legend-item"><span class="mp-legend-dot" style="background:{color};"></span>{label}</span>'
        for label, _, color in series
    )

    lines = []
    for label, values, color in series:
        lines.append(
            f'<polyline points="{points(values)}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />'
        )
        for index, value in enumerate(values):
            x = left_pad + (index * plot_width / max(len(values) - 1, 1))
            y = top_pad + plot_height - (value / max_value) * plot_height
            lines.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{color}" />')

    return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-panel-subtitle">{subtitle}</div><div class="mp-legend">{legend}</div><svg viewBox="0 0 {width} {height}" class="mp-chart" preserveAspectRatio="none">{"".join(grid_lines)}{"".join(lines)}{"".join(x_ticks)}</svg></div>'


def render_table_panel(
    title: str,
    subtitle: str,
    df: pd.DataFrame,
    columns: list[str],
    headers: dict[str, str] | None = None,
) -> str:
    if df.empty:
        return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-panel-subtitle">{subtitle}</div><div class="mp-empty">Không có dữ liệu.</div></div>'
    table_rows = []
    for _, row in df[columns].head(8).iterrows():
        cells = "".join(f"<td>{row[col]}</td>" for col in columns)
        table_rows.append(f"<tr>{cells}</tr>")
    visible_headers = headers or {}
    header_html = "".join(f"<th>{visible_headers.get(col, col)}</th>" for col in columns)
    return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-panel-subtitle">{subtitle}</div><table class="mp-table"><thead><tr>{header_html}</tr></thead><tbody>{"".join(table_rows)}</tbody></table></div>'


def render_summary_grid(items: list[tuple[str, str, str]]) -> str:
    cards = "".join(
        f'<div class="mp-summary-card"><div class="mp-summary-label">{label}</div><div class="mp-summary-value">{value}</div><div class="mp-summary-note">{note}</div></div>'
        for label, value, note in items
    )
    return f'<div class="mp-summary-grid">{cards}</div>'


def render_signal_panel(title: str, signals: list[str]) -> str:
    if not signals:
        return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><div class="mp-empty">Chưa có tín hiệu.</div></div>'
    items = "".join(f"<li>{signal}</li>" for signal in signals)
    return f'<div class="mp-panel"><div class="mp-panel-title">{title}</div><ul class="mp-signal-list">{items}</ul></div>'


def render_badge_strip(items: list[tuple[str, str]]) -> str:
    badges = "".join(f'<span class="mp-badge"><strong>{label}</strong>{value}</span>' for label, value in items)
    return f'<div class="mp-badge-row">{badges}</div>'


def agg_monthly(hourly_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_df.empty:
        return hourly_df
    monthly = (
        hourly_df.assign(pickup_month=pd.to_datetime(hourly_df["pickup_year_month"] + "-01"))
        .groupby("pickup_month", as_index=False)
        .agg(
            total_demand=("total_demand", "sum"),
            active_zones=("active_zones", "mean"),
            avg_temperature_f=("avg_temperature_f", "mean"),
            avg_precipitation_mm=("avg_precipitation_mm", "mean"),
        )
        .sort_values("pickup_month")
    )
    return monthly


def enrich_hourly(hourly_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_df.empty:
        return hourly_df
    enriched = hourly_df.copy()
    enriched["pickup_hour"] = pd.to_datetime(enriched["pickup_hour"])
    enriched["hour_of_day"] = enriched["pickup_hour"].dt.hour
    enriched["day_name"] = pd.Categorical(enriched["pickup_hour"].dt.day_name(), categories=DAY_ORDER, ordered=True)
    enriched["pickup_month"] = pd.to_datetime(enriched["pickup_year_month"] + "-01")
    return enriched


def enrich_zones(zone_df: pd.DataFrame) -> pd.DataFrame:
    if zone_df.empty:
        return zone_df
    enriched = zone_df.copy()
    enriched["demand_share"] = enriched["total_demand"] / enriched["total_demand"].sum()
    enriched["zone_rank"] = range(1, len(enriched) + 1)
    enriched["demand_intensity"] = enriched["total_demand"] / enriched["active_hours"].replace(0, pd.NA)
    return enriched


def enrich_payments(payment_df: pd.DataFrame) -> pd.DataFrame:
    if payment_df.empty:
        return payment_df
    enriched = payment_df.copy()
    enriched["payment_type"] = enriched["payment_type"].apply(normalize_payment_label)
    enriched["trip_share"] = enriched["trip_count"] / enriched["trip_count"].sum()
    enriched["tip_per_fare"] = enriched.apply(lambda row: safe_ratio(row["avg_tip_amount"], row["avg_fare_amount"]), axis=1)
    return enriched


st.markdown(
    """
    <div class="mp-hero">
        <div>
            <h1>Bảng điều khiển vận hành MetroPulse</h1>
            <p>Nhu cầu taxi NYC, bối cảnh thời tiết, hiệu suất zone, hành vi thanh toán/tip và kết quả dự báo ML từ PostgreSQL mart qua FastAPI.</p>
        </div>
        <div class="mp-status">Mart đang hoạt động</div>
    </div>
    """,
    unsafe_allow_html=True,
)

try:
    meta = load_meta()
except requests.RequestException as exc:
    st.error(f"Không thể kết nối tới FastAPI tại {API_URL}: {exc}")
    st.stop()

available_months = meta.get("available_months") or []
table_counts = {row.get("table_name"): row.get("row_count") for row in meta.get("tables", []) if isinstance(row, dict)}
default_start_month = meta.get("min_month") or (available_months[0] if available_months else "2024-01")
default_end_month = meta.get("max_month") or (available_months[-1] if available_months else default_start_month)
default_start_month, default_end_month = month_bounds(available_months, default_start_month, default_end_month)

if available_months and len(available_months) >= 2:
    default_start_index = available_months.index(default_start_month)
    default_end_index = available_months.index(default_end_month)
else:
    default_start_index = 0
    default_end_index = 0

with st.sidebar:
    st.header("Bộ lọc báo cáo")
    if available_months and len(available_months) >= 2:
        selected_months = st.select_slider(
            "Khoảng tháng",
            options=available_months,
            value=(available_months[default_start_index], available_months[default_end_index]),
        )
        if isinstance(selected_months, tuple):
            start_month, end_month = selected_months
        else:
            start_month = end_month = selected_months
    else:
        start_month = st.text_input("Tháng bắt đầu", value=default_start_month)
        end_month = st.text_input("Tháng kết thúc", value=default_end_month)

    hourly_limit = st.slider("Số điểm giờ", min_value=200, max_value=20000, value=5000, step=200)
    zone_limit = st.slider("Số zone top", min_value=5, max_value=50, value=20, step=5)
    show_raw_tables = st.checkbox("Hiện bảng thô", value=False)
    st.divider()
    st.subheader("Kết nối")
    st.code(API_URL)
    st.caption(f"Giai đoạn: {format_month(start_month)} đến {format_month(end_month)}")
    st.subheader("Các mart PostgreSQL")
    st.dataframe(pd.DataFrame(meta.get("tables", [])), hide_index=True, use_container_width=True)

try:
    summary = load_summary(start_month, end_month)
    hourly_df = enrich_hourly(load_hourly(start_month, end_month, hourly_limit))
    zone_df = enrich_zones(load_zones(zone_limit))
    payment_df = enrich_payments(load_payment_tip(start_month, end_month))
except requests.RequestException as exc:
    st.error(f"Không thể tải dữ liệu dashboard từ FastAPI: {exc}")
    st.stop()

monthly_df = agg_monthly(hourly_df)

latest_hour = hourly_df["pickup_hour"].max() if not hourly_df.empty else None
peak_hour = pd.to_datetime(summary.get("peak_hour")) if summary.get("peak_hour") else None
peak_hour_label = peak_hour.strftime("%Y-%m-%d %H:00") if peak_hour is not None and not pd.isna(peak_hour) else "-"
peak_hour_total = metric_value(summary.get("peak_total_demand"))
top_zone = zone_df.iloc[0] if not zone_df.empty else None
top_payment = payment_df.sort_values("trip_count", ascending=False).iloc[0] if not payment_df.empty else None
avg_tip_percent = summary.get("avg_tip_percent")
avg_fare_amount = summary.get("avg_fare_amount")
avg_hourly_demand = safe_ratio(summary.get("total_demand"), summary.get("hourly_points"))
total_demand = float(summary.get("total_demand") or 0)
peak_share = safe_ratio(summary.get("peak_total_demand"), summary.get("total_demand"))
top_zone_share = float(top_zone["demand_share"]) if top_zone is not None and pd.notna(top_zone["demand_share"]) else None
weekend_mask = hourly_df["day_name"].isin(["Saturday", "Sunday"]) if not hourly_df.empty else pd.Series(dtype=bool)
weekend_share = safe_ratio(hourly_df.loc[weekend_mask, "total_demand"].sum() if not hourly_df.empty else None, total_demand)
cashless_share = None
if not payment_df.empty:
    payment_mix_total = payment_df.groupby("payment_type", as_index=False).agg(trip_count=("trip_count", "sum")).sort_values("trip_count", ascending=False)
    if not payment_mix_total.empty:
        payment_leader = payment_mix_total.iloc[0]
        cashless_share = safe_ratio(payment_leader["trip_count"], payment_mix_total["trip_count"].sum())

metric_rows = [
    ("Tổng nhu cầu", metric_value(summary.get("total_demand")), f"{metric_value(summary.get('hourly_points'))} bản ghi theo giờ"),
    ("Nhu cầu giờ trung bình", metric_value(avg_hourly_demand), "Mức lưu lượng trung bình trên mỗi giờ đã xử lý"),
    ("Zone hoạt động trung bình", metric_value(summary.get("avg_active_zones")), "Phạm vi phục vụ trong giai đoạn chọn"),
    ("Nhu cầu trên mỗi zone", metric_value(summary.get("avg_demand_per_active_zone")), "Hữu ích để đánh giá áp lực theo khu vực"),
    ("Giờ đỉnh nhu cầu", peak_hour_label, f"{peak_hour_total} chuyến tại đỉnh"),
    ("Giá cước trung bình", metric_value(avg_fare_amount, " USD"), f"Tip trung bình: {metric_value(avg_tip_percent, '%') }".replace("  ", " ")),
]
st.markdown(render_metric_strip(metric_rows), unsafe_allow_html=True)

badge_items = [
    ("Độ phủ", f" {len(available_months)} tháng" if available_months else " không rõ"),
    ("Mart giờ", f" {table_counts.get('dashboard_hourly_demand_kpi', 0):,} dòng"),
    ("Mart zone", f" {table_counts.get('dashboard_zone_summary', 0):,} dòng"),
    ("Mart thanh toán", f" {table_counts.get('dashboard_payment_tip_summary', 0):,} dòng"),
    ("Fare/tip", f" {metric_value(summary.get('fare_tip_trip_count'))} chuyến"),
    ("Latest", f" {latest_hour.strftime('%Y-%m-%d %H:00') if latest_hour is not None else '-'}"),
]
st.markdown(render_badge_strip(badge_items), unsafe_allow_html=True)

summary_items = [
    (
        "Mức tập trung nhu cầu",
        f"{metric_value((top_zone_share or 0) * 100, '%')}" if top_zone_share is not None else "-",
        "Zone dẫn đầu đóng góp lớn nhất vào nhu cầu của giai đoạn đã chọn.",
    ),
    (
        "Tỷ trọng giờ đỉnh",
        f"{metric_value((peak_share or 0) * 100, '%')}" if peak_share is not None else "-",
        "Phần nhu cầu được ghi nhận tại khung giờ có lưu lượng cao nhất.",
    ),
    (
        "Tỷ trọng cuối tuần",
        f"{metric_value((weekend_share or 0) * 100, '%')}" if weekend_share is not None else "-",
        "Hữu ích cho bố trí nhân sự và kế hoạch vận hành cuối tuần.",
    ),
    (
        "Tỷ trọng phương thức thanh toán dẫn đầu",
        f"{metric_value((cashless_share or 0) * 100, '%')}" if cashless_share is not None else "-",
        f"Dẫn đầu: {top_payment['payment_type']}" if top_payment is not None else "Phương thức thanh toán phổ biến nhất trong giai đoạn.",
    ),
]
st.markdown(render_summary_grid(summary_items), unsafe_allow_html=True)

business_signals = [
    f"Nhu cầu tập trung mạnh ở zone đứng đầu, chiếm {metric_value((top_zone_share or 0) * 100, '%')} trong giai đoạn đã chọn." if top_zone_share is not None else "Chưa tính được mức tập trung nhu cầu cho giai đoạn này.",
    f"Giờ đỉnh đóng góp {metric_value((peak_share or 0) * 100, '%')} tổng nhu cầu, vì vậy năng lực phục vụ cần được bố trí cho các khung giờ cao điểm." if peak_share is not None else "Chưa có số liệu về mức tập trung ở giờ đỉnh.",
    f"Nhu cầu cuối tuần chiếm {metric_value((weekend_share or 0) * 100, '%')} tổng nhu cầu, hữu ích cho kế hoạch ca trực và phân bổ phương tiện." if weekend_share is not None else "Chưa có số liệu nhu cầu cuối tuần.",
    f"Phương thức thanh toán phổ biến nhất là {top_payment['payment_type']} với tỷ trọng {metric_value((cashless_share or 0) * 100, '%')} trên toàn giai đoạn." if top_payment is not None and cashless_share is not None else "Chưa có số liệu về cơ cấu thanh toán.",
]
st.markdown(render_signal_panel("Tín hiệu nghiệp vụ", business_signals), unsafe_allow_html=True)

st.markdown(
    f"""
    <div class="mp-section-title">Tóm tắt điều hành</div>
    <div class="mp-section-caption">
        Giai đoạn {format_month(start_month)} đến {format_month(end_month)} | Giờ xử lý mới nhất {latest_hour.strftime("%Y-%m-%d %H:00") if latest_hour is not None else '-'}
    </div>
    """,
    unsafe_allow_html=True,
)

insight_cols = st.columns([1.4, 1, 1])
with insight_cols[0]:
    st.markdown("**Nhận định vận hành**")
    insight_lines = [
        f"• Giờ có nhu cầu cao nhất: {peak_hour_label} với {peak_hour_total} chuyến",
        f"• Zone dẫn đầu: {top_zone['pickup_zone']} ({top_zone['pickup_borough']})" if top_zone is not None else "• Zone dẫn đầu: -",
        f"• Phương thức thanh toán chủ đạo: {top_payment['payment_type']}" if top_payment is not None else "• Phương thức thanh toán chủ đạo: -",
        f"• Tỷ lệ tip trung bình: {metric_value(avg_tip_percent, '%')}" if avg_tip_percent is not None else "• Tỷ lệ tip trung bình: -",
    ]
    st.markdown("\n".join(insight_lines))
with insight_cols[1]:
    st.markdown("**Độ phủ dữ liệu**")
    st.write(f"Số tháng trong warehouse: {len(available_months) if available_months else metric_value(summary.get('hourly_points'))}")
    st.write(f"Số điểm giờ đã tải: {len(hourly_df):,}")
    st.write(f"Số dòng zone: {len(zone_df):,}")
with insight_cols[2]:
    st.markdown("**Bối cảnh thời tiết**")
    st.write(f"Nhiệt độ trung bình: {metric_value(summary.get('avg_temperature_f'), ' F')}")
    st.write(f"Lượng mưa trung bình: {metric_value(summary.get('avg_precipitation_mm'), ' mm')}")
    st.write(f"Tip trung bình: {metric_value(avg_tip_percent, '%')}")

tab_demand, tab_zones, tab_payments, tab_ml, tab_simulator = st.tabs(["Nhu cầu", "Zone", "Thanh toán & tip", "Dự báo ML", "Simulator Giá cước & Tip"])

with tab_demand:
    st.markdown("<div class='mp-section-title'>Động lực nhu cầu</div>", unsafe_allow_html=True)
    st.caption("Xem nhu cầu taxi thay đổi theo tháng, theo giờ và theo điều kiện thời tiết.")
    if hourly_df.empty or monthly_df.empty:
        st.warning("Không có dữ liệu nhu cầu theo giờ cho giai đoạn đã chọn.")
    else:
        hourly_profile = hourly_df.groupby("hour_of_day", as_index=False).agg(
            total_demand=("total_demand", "sum"),
            avg_temperature_f=("avg_temperature_f", "mean"),
            avg_precipitation_mm=("avg_precipitation_mm", "mean"),
        ).sort_values("hour_of_day")
        day_profile = hourly_df.groupby("day_name", observed=False, as_index=False).agg(
            total_demand=("total_demand", "sum"),
            avg_temperature_f=("avg_temperature_f", "mean"),
        )
        day_profile["day_name"] = pd.Categorical(day_profile["day_name"], categories=DAY_ORDER, ordered=True)
        day_profile = day_profile.sort_values("day_name")
        day_profile["day_name_vi"] = day_profile["day_name"].astype(str).map(DAY_LABELS_VI).fillna(day_profile["day_name"].astype(str))

        top_row = st.columns([1.3, 1])
        with top_row[0]:
            st.markdown(
                render_line_panel(
                    "Xu hướng nhu cầu theo tháng",
                    "So sánh tổng nhu cầu với số zone hoạt động trong giai đoạn báo cáo đã chọn.",
                    [x.strftime("%b %y") for x in monthly_df["pickup_month"]],
                    [
                        ("Tổng nhu cầu", monthly_df["total_demand"].astype(float).tolist(), "#1d4ed8"),
                        ("Zone hoạt động", monthly_df["active_zones"].astype(float).tolist(), "#0f766e"),
                    ],
                ),
                unsafe_allow_html=True,
            )
        with top_row[1]:
            st.markdown(
                render_bar_panel(
                    "Giờ cao điểm",
                    "Khung giờ tập trung lưu lượng trong ngày.",
                    [f"{int(hour):02d}:00" for hour in hourly_profile["hour_of_day"]],
                    hourly_profile["total_demand"].astype(float).tolist(),
                    accent="#dc2626",
                ),
                unsafe_allow_html=True,
            )

        bottom_row = st.columns([1, 1.1])
        with bottom_row[0]:
            st.markdown(
                render_bar_panel(
                    "Nhu cầu theo ngày trong tuần",
                    "Hữu ích cho bố trí nhân sự, kế hoạch vận hành và năng lực phục vụ.",
                    day_profile["day_name_vi"].tolist(),
                    day_profile["total_demand"].astype(float).tolist(),
                    accent="#7c3aed",
                ),
                unsafe_allow_html=True,
            )
        with bottom_row[1]:
            st.markdown(
                render_line_panel(
                    "Bối cảnh thời tiết",
                    "Nhiệt độ và lượng mưa trung bình trong giai đoạn đã chọn.",
                    [x.strftime("%b %y") for x in monthly_df["pickup_month"]],
                    [
                        ("Nhiệt độ (F)", monthly_df["avg_temperature_f"].astype(float).tolist(), "#ea580c"),
                        ("Lượng mưa (mm)", monthly_df["avg_precipitation_mm"].astype(float).tolist(), "#0284c7"),
                    ],
                ),
                unsafe_allow_html=True,
            )

        st.markdown("**Bảng vận hành**")
        hourly_table = hourly_df[
            [
                "pickup_hour",
                "pickup_year_month",
                "total_demand",
                "active_zones",
                "avg_demand_per_active_zone",
                "avg_temperature_f",
                "avg_precipitation_mm",
            ]
        ].sort_values("pickup_hour", ascending=False)
        st.markdown(
            render_table_panel(
                "Snapshot nhu cầu theo giờ",
                "Các dòng gần nhất để kiểm tra nhanh và đối soát chất lượng.",
                hourly_table,
                ["pickup_hour", "total_demand", "active_zones", "avg_demand_per_active_zone"],
                headers={
                    "pickup_hour": "Giờ đón",
                    "total_demand": "Tổng nhu cầu",
                    "active_zones": "Zone hoạt động",
                    "avg_demand_per_active_zone": "Nhu cầu/zone",
                },
            ),
            unsafe_allow_html=True,
        )
        st.dataframe(hourly_table, use_container_width=True, hide_index=True)
        st.download_button(
            "Tải CSV nhu cầu theo giờ",
            data=df_to_csv(hourly_df),
            file_name=f"hourly_demand_{start_month}_{end_month}.csv",
            mime="text/csv",
        )

with tab_zones:
    st.markdown("<div class='mp-section-title'>Hiệu suất theo zone</div>", unsafe_allow_html=True)
    st.caption("Xác định nơi nhu cầu tập trung và zone nào hoạt động lâu nhất.")
    if zone_df.empty:
        st.warning("Không có dữ liệu tổng hợp zone.")
    else:
        borough_df = (
            zone_df.groupby("pickup_borough", as_index=False)
            .agg(total_demand=("total_demand", "sum"), active_hours=("active_hours", "sum"))
            .sort_values("total_demand", ascending=False)
        )

        zone_top = zone_df.sort_values("total_demand", ascending=False).head(10)
        zone_cols = st.columns([1.15, 0.95])
        with zone_cols[0]:
            st.markdown(
                render_bar_panel(
                    "Zone có nhu cầu cao nhất",
                    "Các zone ưu tiên cho kế hoạch cung ứng và giám sát dịch vụ.",
                    zone_top["pickup_zone"].astype(str).tolist(),
                    zone_top["total_demand"].astype(float).tolist(),
                    accent="#1d4ed8",
                ),
                unsafe_allow_html=True,
            )
        with zone_cols[1]:
            st.markdown(
                render_bar_panel(
                    "Nhu cầu theo borough",
                    "Mức tập trung ở cấp borough theo điểm đón.",
                    borough_df["pickup_borough"].astype(str).tolist(),
                    borough_df["total_demand"].astype(float).tolist(),
                    accent="#0f766e",
                ),
                unsafe_allow_html=True,
            )

        zone_view = zone_df[[
            "zone_rank",
            "pickup_borough",
            "pickup_zone",
            "total_demand",
            "avg_hourly_demand",
            "max_hourly_demand",
            "active_hours",
            "demand_share",
            "demand_intensity",
        ]].copy()
        zone_view["demand_share"] = (zone_view["demand_share"] * 100).round(2).astype(str) + "%"
        zone_view["demand_intensity"] = zone_view["demand_intensity"].round(2)
        st.markdown(
            render_table_panel(
                "Snapshot hiệu suất zone",
                "Danh sách zone xếp hạng theo tổng nhu cầu và số giờ hoạt động.",
                zone_view,
                ["zone_rank", "pickup_borough", "pickup_zone", "total_demand"],
                headers={
                    "zone_rank": "Xếp hạng",
                    "pickup_borough": "Borough",
                    "pickup_zone": "Zone",
                    "total_demand": "Tổng nhu cầu",
                },
            ),
            unsafe_allow_html=True,
        )
        st.dataframe(zone_view, use_container_width=True, hide_index=True)
        st.download_button(
            "Tải CSV tổng hợp zone",
            data=df_to_csv(zone_df),
            file_name=f"zone_summary_top_{zone_limit}.csv",
            mime="text/csv",
        )

with tab_payments:
    st.markdown("<div class='mp-section-title'>Hành vi thanh toán và tip</div>", unsafe_allow_html=True)
    st.caption("Theo dõi cách khách hàng thanh toán và mức tip thay đổi theo tháng.")
    if payment_df.empty:
        st.warning("Không có dữ liệu thanh toán/tip cho giai đoạn đã chọn.")
    else:
        payment_monthly = (
            payment_df.groupby("pickup_year_month", as_index=False)
            .agg(trip_count=("trip_count", "sum"), avg_tip_percent=("avg_tip_percent", "mean"), avg_fare_amount=("avg_fare_amount", "mean"))
            .sort_values("pickup_year_month")
        )
        payment_mix = payment_df.groupby("payment_type", as_index=False).agg(
            trip_count=("trip_count", "sum"),
            avg_fare_amount=("avg_fare_amount", "mean"),
            avg_tip_percent=("avg_tip_percent", "mean"),
        ).sort_values("trip_count", ascending=False)

        payment_cols = st.columns([1.15, 0.95])
        with payment_cols[0]:
            st.markdown(
                render_line_panel(
                    "Chỉ số thanh toán theo tháng",
                    "Xu hướng số chuyến và tỷ lệ tip trung bình trong giai đoạn đã chọn.",
                    payment_monthly["pickup_year_month"].astype(str).tolist(),
                    [
                        ("Số chuyến", payment_monthly["trip_count"].astype(float).tolist(), "#1d4ed8"),
                        ("Tip TB %", payment_monthly["avg_tip_percent"].astype(float).tolist(), "#dc2626"),
                    ],
                ),
                unsafe_allow_html=True,
            )
        with payment_cols[1]:
            st.markdown(
                render_bar_panel(
                    "Cơ cấu phương thức thanh toán",
                    "Hữu ích để đánh giá mức độ dùng thanh toán không tiền mặt và chất lượng giao dịch.",
                    payment_mix["payment_type"].astype(str).tolist(),
                    payment_mix["trip_count"].astype(float).tolist(),
                    accent="#7c3aed",
                ),
                unsafe_allow_html=True,
            )

        payment_view = payment_df[[
            "pickup_year_month",
            "payment_type",
            "trip_count",
            "trip_share",
            "avg_fare_amount",
            "avg_tip_amount",
            "avg_tip_percent",
            "median_tip_percent",
            "median_fare_amount",
            "avg_trip_distance",
            "tip_per_fare",
        ]].copy()
        payment_view["trip_share"] = (payment_view["trip_share"] * 100).round(2).astype(str) + "%"
        payment_view["tip_per_fare"] = (payment_view["tip_per_fare"] * 100).round(2).astype(str) + "%" if payment_view["tip_per_fare"].notna().any() else payment_view["tip_per_fare"]
        st.markdown(
            render_table_panel(
                "Snapshot thanh toán và tip",
                "Các dòng thanh toán/tip quan trọng nhất trong kỳ báo cáo hiện tại.",
                payment_view,
                ["pickup_year_month", "payment_type", "trip_count", "avg_tip_percent"],
                headers={
                    "pickup_year_month": "Tháng",
                    "payment_type": "Phương thức thanh toán",
                    "trip_count": "Số chuyến",
                    "avg_tip_percent": "Tip TB %",
                },
            ),
            unsafe_allow_html=True,
        )
        st.dataframe(payment_view, use_container_width=True, hide_index=True)
        st.download_button(
            "Tải CSV thanh toán-tip",
            data=df_to_csv(payment_df),
            file_name=f"payment_tip_summary_{start_month}_{end_month}.csv",
            mime="text/csv",
        )

with tab_ml:
    import numpy as np
    st.markdown("<div class='mp-section-title'>Dự báo nhu cầu tháng 12/2024</div>", unsafe_allow_html=True)
    st.caption("Kết quả từ mô hình XGBoost huấn luyện trên dữ liệu lịch sử, dùng để dự báo nhu cầu theo giờ và theo zone cho tháng 12/2024 chưa từng thấy.")

    ml_df = load_ml_predictions()
    all_ml_metrics = load_all_ml_metrics()
    ml_metrics = all_ml_metrics.get("demand", {}) if all_ml_metrics else load_ml_metrics()

    if ml_df.empty:
        st.warning(f"Chưa tìm thấy file dự đoán tại {ML_PREDICTIONS_PATH}. Hãy chạy lại `python demo/predict_demand_demo.py` trong thư mục `ml/`.")
    else:
        # ─── BỘ LỌC ĐỘNG TƯƠNG TÁC TẠI TAB ML ───
        st.markdown("<div style='font-weight:700; margin-bottom:0.4rem;'>🔍 Bộ lọc dự báo động</div>", unsafe_allow_html=True)
        filter_cols = st.columns([1, 1, 1, 1])
        
        # 1. Lọc theo Zone
        zone_names_list = ["Tất cả"] + sorted(list(ml_df["zone_name"].dropna().unique()))
        selected_zone = filter_cols[0].selectbox("Khu vực (Zone)", options=zone_names_list, index=0, key="ml_zone_filter")
        
        # 2. Lọc theo loại Zone
        zone_types = ["Tất cả", "Sân bay", "Manhattan core", "Bình thường"]
        selected_zone_type = filter_cols[1].selectbox("Loại Zone", options=zone_types, index=0, key="ml_zone_type_filter")
        
        # 3. Lọc theo Thời tiết
        weather_options = ["Tất cả", "Chỉ ngày mưa (>0mm)", "Chỉ ngày lạnh buốt (<36°F)", "Khô ráo và ấm"]
        selected_weather = filter_cols[2].selectbox("Thời tiết", options=weather_options, index=0, key="ml_weather_filter")
        
        # 4. Lọc theo Loại ngày
        day_options = ["Tất cả", "Ngày thường (Thứ 2-6)", "Cuối tuần (Thứ 7, CN)", "Ngày lễ"]
        selected_day_type = filter_cols[3].selectbox("Phân loại ngày", options=day_options, index=0, key="ml_day_type_filter")
        
        # Slider chọn khoảng ngày trong tháng 12
        min_date = ml_df["pickup_hour"].min().date()
        max_date = ml_df["pickup_hour"].max().date()
        selected_date_range = st.slider(
            "Chọn khoảng thời gian dự báo (Tháng 12/2024)",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="DD/MM",
            key="ml_date_range_slider"
        )

        # Tiến hành lọc dữ liệu
        filtered_ml_df = ml_df.copy()
        
        # Lọc theo khoảng thời gian
        filtered_ml_df = filtered_ml_df[
            (filtered_ml_df["pickup_hour"].dt.date >= selected_date_range[0]) &
            (filtered_ml_df["pickup_hour"].dt.date <= selected_date_range[1])
        ]
        
        # Lọc theo Zone
        if selected_zone != "Tất cả":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["zone_name"] == selected_zone]
            
        # Lọc theo loại Zone
        if selected_zone_type != "Tất cả":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["zone_type"] == selected_zone_type]
            
        # Lọc theo thời tiết
        if selected_weather == "Chỉ ngày mưa (>0mm)":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["precipitation_mm"] > 0]
        elif selected_weather == "Chỉ ngày lạnh buốt (<36°F)":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["temperature_f"] < 36]
        elif selected_weather == "Khô ráo và ấm":
            filtered_ml_df = filtered_ml_df[(filtered_ml_df["precipitation_mm"] == 0) & (filtered_ml_df["temperature_f"] >= 36)]
            
        # Lọc theo loại ngày
        if selected_day_type == "Ngày thường (Thứ 2-6)":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["is_weekend"] == 0]
        elif selected_day_type == "Cuối tuần (Thứ 7, CN)":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["is_weekend"] == 1]
        elif selected_day_type == "Ngày lễ":
            filtered_ml_df = filtered_ml_df[filtered_ml_df["is_holiday"] == 1]

        if filtered_ml_df.empty:
            st.warning("Không có dữ liệu thỏa mãn bộ lọc hiện tại. Vui lòng nới rộng các tùy chọn chọn lọc.")
        else:
            # ─── TÍNH TOÁN METRIC ĐỘNG THỜI GIAN THỰC ───
            actual = filtered_ml_df["demand"].values
            pred = filtered_ml_df["predicted_demand"].values
            
            rmse_val = float(np.sqrt(np.mean((actual - pred) ** 2)))
            mae_val = float(np.mean(np.abs(actual - pred)))
            
            # MAPE
            mask_non_zero = actual > 0
            mape_val = float(np.mean(np.abs((actual[mask_non_zero] - pred[mask_non_zero]) / actual[mask_non_zero]))) if mask_non_zero.sum() > 0 else 0.0
            
            # R2
            mean_actual = np.mean(actual)
            ss_res = np.sum((actual - pred) ** 2)
            ss_tot = np.sum((actual - mean_actual) ** 2)
            r2_val = float(1 - (ss_res / ss_tot)) if ss_tot > 0 else 1.0

            paper_rmse = 38.51
            paper_r2 = 0.97
            best_iteration = ml_metrics.get("best_iteration", 1499)
            n_features = ml_metrics.get("n_features", 24)

            badge_paper = "🟢 Tốt hơn paper" if rmse_val < paper_rmse else "🔴 Kém hơn paper"

            ml_metric_rows = [
                ("RMSE", f"{rmse_val:.4f}", f"Baseline: {paper_rmse} ({badge_paper})"),
                ("MAE", f"{mae_val:.4f}", "Sai số tuyệt đối trung bình"),
                ("MAPE", f"{mape_val * 100:.2f}%", "Tỷ lệ sai số tương đối"),
                ("R²", f"{r2_val:.4f}", f"Baseline R²: {paper_r2}"),
                ("Best iter", metric_value(best_iteration), "Số vòng lặp tối ưu nhất"),
                ("Features", metric_value(n_features), "Số đặc trưng đầu vào"),
            ]
            st.markdown(render_metric_strip(ml_metric_rows), unsafe_allow_html=True)

            ml_summary = [
                ("Dữ liệu lọc", f"{len(filtered_ml_df):,} dòng", f"{filtered_ml_df['pu_location_id'].nunique():,} zone đang lọc"),
                ("Khoảng ngày lọc", f"{selected_date_range[0].strftime('%d/%m')} → {selected_date_range[1].strftime('%d/%m')}", "Thời gian tháng 12/2024"),
                ("Độ lệch RMSE vs paper", f"{rmse_val:.2f} vs {paper_rmse}", f"Độ lệch: {abs(rmse_val - paper_rmse):.2f} chuyến/giờ"),
                ("Mô hình sử dụng", "XGBoost Regressor", "Suy luận động dựa trên tập lag từ tháng 11"),
            ]
            st.markdown(render_summary_grid(ml_summary), unsafe_allow_html=True)

            ml_signals = [
                f"Mô hình đạt RMSE {rmse_val:.4f} trên tập dữ liệu đang lọc (so với paper baseline {paper_rmse}).",
                f"Chỉ số R² đạt {r2_val:.4f}, mô hình giải thích được {r2_val*100:.1f}% phương sai nhu cầu thực tế.",
                f"Tập dữ liệu lọc hiện tại có {len(filtered_ml_df):,} dòng, bao trùm {filtered_ml_df['pu_location_id'].nunique():,} zone khác nhau.",
                "Các biến lag chu kỳ dài (lag 168h, lag 48h) và trung bình trượt 3 ngày giúp khử nhiễu dự báo trong các điều kiện bất thường.",
            ]
            st.markdown(render_signal_panel("Tín hiệu ML & Hiệu năng phân tích", ml_signals), unsafe_allow_html=True)

            # ─── BIỂU ĐỒ TRỰC QUAN ĐỘNG ───
            ml_cols = st.columns([1.15, 0.95])
            
            # Tính toán profile 24h
            hourly_ml = (
                filtered_ml_df.groupby("hour_of_day", as_index=False)
                .agg(
                    actual_mean=("demand", "mean"),
                    predicted_mean=("predicted_demand", "mean"),
                )
                .sort_values("hour_of_day")
            )
            
            # Tính toán timeline theo ngày
            daily_ml = (
                filtered_ml_df
                .assign(pickup_date=filtered_ml_df["pickup_hour"].dt.date)
                .groupby("pickup_date", as_index=False)
                .agg(
                    actual_sum=("demand", "sum"),
                    predicted_sum=("predicted_demand", "sum")
                )
                .sort_values("pickup_date")
            )

            with ml_cols[0]:
                st.markdown(
                    render_line_panel(
                        "Trung bình thực tế so với dự báo theo giờ",
                        f"Profile 24 giờ trung bình trên tập dữ liệu đang chọn lọc ({len(filtered_ml_df):,} dòng).",
                        hourly_ml["hour_of_day"].astype(str).str.zfill(2).tolist(),
                        [
                            ("Thực tế", hourly_ml["actual_mean"].astype(float).tolist(), "#1d4ed8"),
                            ("Dự báo", hourly_ml["predicted_mean"].astype(float).tolist(), "#dc2626"),
                        ],
                    ),
                    unsafe_allow_html=True,
                )
            with ml_cols[1]:
                st.markdown(
                    render_line_panel(
                        "Tổng nhu cầu thực tế so với dự báo theo ngày",
                        "Xu hướng tổng nhu cầu qua các ngày trong khoảng thời gian đang lọc.",
                        [d.strftime("%d/%m") for d in daily_ml["pickup_date"]],
                        [
                            ("Thực tế", daily_ml["actual_sum"].astype(float).tolist(), "#10b981"),
                            ("Dự báo", daily_ml["predicted_sum"].astype(float).tolist(), "#f59e0b"),
                        ],
                    ),
                    unsafe_allow_html=True,
                )

            # Sai số theo loại zone
            zone_type_error = (
                filtered_ml_df.groupby("zone_type", as_index=False)
                .agg(
                    rows=("demand", "count"),
                    actual_mean=("demand", "mean"),
                    predicted_mean=("predicted_demand", "mean"),
                    mae=("abs_error", "mean"),
                    mape=("pct_error", "mean"),
                )
                .sort_values("actual_mean", ascending=False)
            )
            zone_type_error["mape"] = zone_type_error["mape"].round(2).astype(str) + "%"
            
            # Các điểm sai số lớn nhất
            error_rows = (
                filtered_ml_df.sort_values("abs_error", ascending=False)
                .loc[:, ["pickup_hour", "zone_name", "demand", "predicted_demand", "abs_error", "pct_error", "temperature_f", "precipitation_mm"]]
                .head(10)
                .copy()
            )
            error_rows["pickup_hour"] = pd.to_datetime(error_rows["pickup_hour"]).dt.strftime("%d-%m %H:%M")
            error_rows["pct_error"] = error_rows["pct_error"].round(1).astype(str) + "%"
            error_rows["abs_error"] = error_rows["abs_error"].round(1)

            st.markdown(
                render_table_panel(
                    "Sai số theo loại zone",
                    "Đánh giá năng lực dự báo khác biệt giữa khu vực sân bay, lõi Manhattan và các vùng bình thường.",
                    zone_type_error,
                    ["zone_type", "rows", "actual_mean", "predicted_mean"],
                    headers={
                        "zone_type": "Loại zone",
                        "rows": "Số dòng",
                        "actual_mean": "TB thực tế",
                        "predicted_mean": "TB dự báo",
                    },
                ),
                unsafe_allow_html=True,
            )

            st.markdown(
                render_table_panel(
                    "Các điểm dự báo lệch nhiều nhất (Outliers)",
                    "Dùng để phân tích các trường hợp đột biến về nhu cầu hoặc các điểm thời tiết cực đoan.",
                    error_rows,
                    ["pickup_hour", "zone_name", "demand", "predicted_demand", "abs_error"],
                    headers={
                        "pickup_hour": "Thời gian",
                        "zone_name": "Khu vực",
                        "demand": "Thực tế",
                        "predicted_demand": "Dự báo",
                        "abs_error": "Sai số tuyệt đối",
                    },
                ),
                unsafe_allow_html=True,
            )

            # So sánh Christmas
            christmas_mask = filtered_ml_df["pickup_hour"].dt.strftime("%m-%d") == "12-25"
            comparison_df = pd.DataFrame(
                [
                    {
                        "period": "Christmas 25/12",
                        "actual_mean": filtered_ml_df.loc[christmas_mask, "demand"].mean() if christmas_mask.any() else 0.0,
                        "predicted_mean": filtered_ml_df.loc[christmas_mask, "predicted_demand"].mean() if christmas_mask.any() else 0.0,
                        "mae": filtered_ml_df.loc[christmas_mask, "abs_error"].mean() if christmas_mask.any() else 0.0,
                    },
                    {
                        "period": "Ngày thường",
                        "actual_mean": filtered_ml_df.loc[~christmas_mask, "demand"].mean() if (~christmas_mask).any() else 0.0,
                        "predicted_mean": filtered_ml_df.loc[~christmas_mask, "predicted_demand"].mean() if (~christmas_mask).any() else 0.0,
                        "mae": filtered_ml_df.loc[~christmas_mask, "abs_error"].mean() if (~christmas_mask).any() else 0.0,
                    },
                ]
            )
            
            st.markdown(
                render_table_panel(
                    "Đánh giá ngày lễ Christmas (25/12) so với ngày thường trong tập lọc",
                    "Kiểm định mức độ nhạy bén của mô hình khi hành vi đi lại giảm mạnh trong ngày lễ hội.",
                    comparison_df,
                    ["period", "actual_mean", "predicted_mean", "mae"],
                    headers={
                        "period": "Nhóm ngày",
                        "actual_mean": "TB thực tế",
                        "predicted_mean": "TB dự báo",
                        "mae": "MAE",
                    },
                ),
                unsafe_allow_html=True,
            )

            # Mẫu dự đoán chi tiết
            ml_sample = (
                filtered_ml_df.sort_values(["pickup_hour", "pu_location_id"])
                .loc[:, ["pickup_hour", "zone_name", "demand", "predicted_demand", "abs_error"]]
                .head(30)
                .copy()
            )
            ml_sample["pickup_hour"] = pd.to_datetime(ml_sample["pickup_hour"]).dt.strftime("%d-%m %H:%M")
            ml_sample["abs_error"] = ml_sample["abs_error"].round(1)

            st.markdown(
                render_table_panel(
                    "Mẫu dự đoán chi tiết gần nhất",
                    "Các dòng đầu ra kiểm tra nhanh chất lượng dự báo và đối soát trực tiếp.",
                    ml_sample,
                    ["pickup_hour", "zone_name", "demand", "predicted_demand", "abs_error"],
                    headers={
                        "pickup_hour": "Thời gian",
                        "zone_name": "Khu vực",
                        "demand": "Thực tế",
                        "predicted_demand": "Dự báo",
                        "abs_error": "Sai số",
                    },
                ),
                unsafe_allow_html=True,
            )
            
            st.dataframe(filtered_ml_df.head(100), use_container_width=True, hide_index=True)
            st.download_button(
                "Tải dữ liệu dự báo ML đang lọc (CSV)",
                data=df_to_csv(filtered_ml_df),
                file_name=f"demand_predictions_filtered.csv",
                mime="text/csv",
            )


with tab_simulator:
    st.markdown("<div class='mp-section-title'>Mô phỏng & Dự báo hành trình (Fare & Tip Simulator)</div>", unsafe_allow_html=True)
    st.caption("Nhập các thông số chuyến đi giả định để dự toán ngay lập tức Giá cước & tỷ lệ Tip từ các mô hình XGBoost lưu trên GCP VM thông qua API.")

    all_zones = load_all_zones()
    if not all_zones:
        all_zones = [{"zone_id": k, "zone_name": v, "borough": "Manhattan"} for k, v in ZONE_NAMES.items()]
    
    zone_options = {
    z["zone_name"]: z["zone_id"]
    for z in all_zones
    if z.get("zone_name")
}
    zone_names_sorted = sorted(list(zone_options.keys()))

    sim_cols = st.columns([1.1, 0.9])
    
    with sim_cols[0]:
        st.markdown("<div style='font-weight:700; margin-bottom:0.4rem;'>📍 Cấu hình lộ trình hành trình</div>", unsafe_allow_html=True)
        route_subcols = st.columns(2)
        
        # Default JFK to Midtown Center
        default_pu_idx = zone_names_sorted.index("JFK Airport") if "JFK Airport" in zone_names_sorted else 0
        default_do_idx = zone_names_sorted.index("Midtown Center") if "Midtown Center" in zone_names_sorted else 0
        
        pu_name = route_subcols[0].selectbox("Khu vực đón (Pickup Zone)", options=zone_names_sorted, index=default_pu_idx)
        do_name = route_subcols[1].selectbox("Khu vực trả (Dropoff Zone)", options=zone_names_sorted, index=default_do_idx)
        
        trip_distance = st.number_input("Quãng đường ước tính (miles)", min_value=0.1, max_value=150.0, value=13.5, step=0.5)
        
        st.markdown("<div style='font-weight:700; margin-top:0.8rem; margin-bottom:0.4rem;'>🕒 Thời gian & Hình thức</div>", unsafe_allow_html=True)
        time_subcols = st.columns(3)
        passenger_count = time_subcols[0].selectbox("Số hành khách", options=[1, 2, 3, 4, 5, 6], index=0)
        ratecode_id = time_subcols[1].selectbox("Mã biểu giá (RateCode)", options=[1, 2, 3, 4, 5], index=0, 
                                                format_func=lambda x: {1: "Rate 1: Standard", 2: "Rate 2: JFK Airport", 3: "Rate 3: Newark", 4: "Rate 4: Nassau", 5: "Rate 5: Negotiated"}[x])
        payment_type = time_subcols[2].selectbox("Hình thức thanh toán", options=[1, 2], index=0,
                                                 format_func=lambda x: PAYMENT_TYPE_LABELS.get(x, f"Loại {x}"))
        
        date_subcols = st.columns(3)
        sim_month = date_subcols[0].selectbox("Tháng hành trình", options=[11, 12], index=1)
        sim_day_of_week = date_subcols[1].selectbox("Thứ trong tuần", options=[0, 1, 2, 3, 4, 5, 6], index=0,
                                                    format_func=lambda x: {0: "Thứ Hai", 1: "Thứ Ba", 2: "Thứ Tư", 3: "Thứ Năm", 4: "Thứ Sáu", 5: "Thứ Bảy", 6: "Chủ Nhật"}[x])
        sim_hour = date_subcols[2].selectbox("Giờ xuất hành", options=list(range(24)), index=8)
        
        st.markdown("<div style='font-weight:700; margin-top:0.8rem; margin-bottom:0.4rem;'>☀️ Bối cảnh thời tiết</div>", unsafe_allow_html=True)
        weather_subcols = st.columns(2)
        temperature_f = weather_subcols[0].slider("Nhiệt độ ngoài trời (°F)", min_value=0.0, max_value=100.0, value=34.0, step=1.0)
        precipitation_mm = weather_subcols[1].slider("Lượng mưa tích lũy (mm)", min_value=0.0, max_value=30.0, value=0.0, step=0.1)

        is_rush_hour_val = 1 if sim_hour in [7, 8, 9, 17, 18, 19] else 0
        is_weekend_val = 1 if sim_day_of_week in [5, 6] else 0
        is_raining_val = 1 if precipitation_mm > 0.0 else 0
        is_cold_val = 1 if temperature_f < 36.0 else 0

        st.markdown("<div style='font-size:0.82rem; color:#64748b; font-weight:600;'>Đặc trưng suy luận tự động:</div>", unsafe_allow_html=True)
        badges = [
            ("Giờ cao điểm", "Có" if is_rush_hour_val else "Không"),
            ("Ngày cuối tuần", "Có" if is_weekend_val else "Không"),
            ("Có mưa tuyết", "Có" if is_raining_val else "Không"),
            ("Thời tiết lạnh", "Có" if is_cold_val else "Không"),
        ]
        st.markdown(render_badge_strip(badges), unsafe_allow_html=True)
        
        btn_predict = st.button("💰 Chạy ước lượng chi phí", type="primary", use_container_width=True)

    with sim_cols[1]:
        st.markdown("<div style='font-weight:700; margin-bottom:0.4rem;'>🧾 Hóa đơn điện tử ước tính</div>", unsafe_allow_html=True)
        if btn_predict:
            payload = {
                "trip_distance": float(trip_distance),
                "pu_location_id": int(zone_options[pu_name]),
                "do_location_id": int(zone_options[do_name]),
                "passenger_count": int(passenger_count),
                "ratecode_id": int(ratecode_id),
                "hour": int(sim_hour),
                "day_of_week": int(sim_day_of_week),
                "month": int(sim_month),
                "temperature_f": float(temperature_f),
                "precipitation_mm": float(precipitation_mm),
                "payment_type": int(payment_type)
            }
            
            try:
                response = requests.post(f"{API_URL}/api/predict/fare-tip", json=payload, timeout=15)
                response.raise_for_status()
                res = response.json()
                
                pred_fare = res["predicted_fare"]
                pred_tip_pct = res["predicted_tip_percent"]
                pred_tip_amount = res["predicted_tip_amount"]
                total_amount = res["total_amount"]
                model_used = res["model_used"]
                
                model_badge = "🟢 XGBoost Model (Live)" if model_used else "⚠️ Heuristic Fallback (Offline)"
                
                tip_note = ""
                if payment_type != 1:
                    tip_note = "<br><span style='color:#b45309; font-size:0.8rem;'>*Lưu ý: Chỉ áp dụng mô hình dự đoán tip cho thanh toán Thẻ (Credit Card)</span>"
                
                components.html(
                    f"""
                    <div style="background:#ffffff; border:1px solid rgba(37,48,63,0.12); border-radius:12px; padding:1.4rem; box-shadow:0 10px 15px -3px rgba(0,0,0,0.05); font-family: 'Courier New', Courier, monospace; color:#1e293b;">
                        <h3 style="text-align:center; margin-top:0; color:#0f172a; border-bottom:2px dashed #e2e8f0; padding-bottom:0.8rem; letter-spacing:1px;">METROPULSE TAXI RECEIPT</h3>
                        
                        <div style="display:flex; justify-content:space-between; margin:0.6rem 0;">
                            <span>Điểm đón (Pickup):</span>
                            <strong>{pu_name}</strong>
                        </div>
                        <div style="display:flex; justify-content:space-between; margin:0.6rem 0;">
                            <span>Điểm trả (Dropoff):</span>
                            <strong>{do_name}</strong>
                        </div>
                        <div style="display:flex; justify-content:space-between; margin:0.6rem 0;">
                            <span>Khoảng cách hành trình:</span>
                            <strong>{trip_distance:.2f} miles</strong>
                        </div>
                        <div style="display:flex; justify-content:space-between; margin:0.6rem 0;">
                            <span>Số hành khách đi cùng:</span>
                            <strong>{passenger_count} người</strong>
                        </div>
                        <div style="display:flex; justify-content:space-between; margin:0.6rem 0; border-bottom:1px solid #e2e8f0; padding-bottom:0.5rem;">
                            <span>Hình thức thanh toán:</span>
                            <strong>{PAYMENT_TYPE_LABELS.get(payment_type)}</strong>
                        </div>
                        
                        <div style="display:flex; justify-content:space-between; margin:0.8rem 0; font-size:1.15rem;">
                            <span>Giá cước gốc (Fare Amount):</span>
                            <strong style="color:#0f172a;">${pred_fare:.2f}</strong>
                        </div>
                        <div style="display:flex; justify-content:space-between; margin:0.8rem 0;">
                            <span>Tiền Tip đề xuất ({pred_tip_pct:.1f}%):</span>
                            <strong style="color:#0f766e;">+${pred_tip_amount:.2f}</strong>
                        </div>
                        
                        <div style="display:flex; justify-content:space-between; margin:0.9rem 0 0; padding-top:1rem; border-top:2px dashed #cbd5e1; font-size:1.45rem; font-weight:bold;">
                            <span>TỔNG THÀNH TIỀN:</span>
                            <strong style="color:#1d4ed8;">${total_amount:.2f}</strong>
                        </div>
                        
                        <div style="text-align:center; margin-top:1.4rem; font-size:0.75rem; color:#64748b; border-top:1px solid #f1f5f9; padding-top:0.8rem; line-height:1.4;">
                            Suy luận: {model_badge}{tip_note}<br>
                            Múi giờ dữ liệu chuẩn: America/New_York
                        </div>
                    </div>
                    """,
                    height=550,
                    scrolling=False
                )
                
                # Show model validation metrics
                all_metrics = load_all_ml_metrics()
                fare_m = all_metrics.get("fare", {})
                tip_m = all_metrics.get("tip", {})
                
                if fare_m or tip_m:
                    st.write("")
                    st.markdown("<div style='font-weight:700; font-size:0.9rem; margin-bottom:0.4rem;'>📊 Sai số mô hình huấn luyện (Test Set)</div>", unsafe_allow_html=True)
                    subcols = st.columns(2)
                    if fare_m:
                        subcols[0].markdown(
                            f"""
                            <div style="background:#ffffff; border:1px solid rgba(37,48,63,0.08); border-radius:8px; padding:0.6rem 0.8rem; box-shadow:0 1px 2px rgba(0,0,0,0.02);">
                                <div style="color:#64748b; font-size:0.75rem; font-weight:600; text-transform:uppercase;">Mô hình Fare</div>
                                <div style="font-size:1.05rem; font-weight:700; color:#1e293b; margin:0.2rem 0;">R²: {fare_m.get('r2') or '-'}</div>
                                <div style="font-size:0.76rem; color:#475569;">
                                    RMSE: {fare_m.get('rmse') or '-'} USD<br>
                                    MAE: {fare_m.get('mae') or '-'} USD
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                    if tip_m:
                        subcols[1].markdown(
                            f"""
                            <div style="background:#ffffff; border:1px solid rgba(37,48,63,0.08); border-radius:8px; padding:0.6rem 0.8rem; box-shadow:0 1px 2px rgba(0,0,0,0.02);">
                                <div style="color:#64748b; font-size:0.75rem; font-weight:600; text-transform:uppercase;">Mô hình Tip %</div>
                                <div style="font-size:1.05rem; font-weight:700; color:#0f766e; margin:0.2rem 0;">R²: {tip_m.get('r2') or '-'}</div>
                                <div style="font-size:0.76rem; color:#475569;">
                                    RMSE: {tip_m.get('rmse') or '-'}%<br>
                                    MAE: {tip_m.get('mae') or '-'}%
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
            
            except Exception as e:
                st.error(f"Không thể kết nối đến Dashboard API hoặc suy luận lỗi: {e}")
        else:
            st.markdown(
                """
                <div class="mp-empty" style="text-align:center; padding:3rem 1.5rem; border:2px dashed rgba(37,48,63,0.1); border-radius:12px; background:#f8fafc;">
                    <div style="font-size:2.5rem; margin-bottom:0.8rem;">🚕</div>
                    <div style="font-weight:600; color:#334155; font-size:0.95rem;">Chưa có dữ liệu tính toán</div>
                    <div style="color:#64748b; font-size:0.86rem; margin-top:0.3rem; max-width:280px; margin-left:auto; margin-right:auto;">
                        Vui lòng nhập cấu hình hành trình bên trái và nhấn nút "Chạy ước lượng chi phí" để bắt đầu.
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )


if show_raw_tables:
    with st.expander("Xem dữ liệu thô", expanded=False):
        st.write("Nhu cầu theo giờ")
        st.dataframe(hourly_df.head(50), use_container_width=True, hide_index=True)
        st.write("Tổng hợp zone")
        st.dataframe(zone_df.head(50), use_container_width=True, hide_index=True)
        st.write("Tổng hợp thanh toán/tip")
        st.dataframe(payment_df.head(50), use_container_width=True, hide_index=True)
