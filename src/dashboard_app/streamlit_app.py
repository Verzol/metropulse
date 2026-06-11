import json
import os
from typing import Any
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv


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
            color-scheme: only light;
            --mp-bg: #F7FAFC;
            --mp-surface: #FFFFFF;
            --mp-surface-soft: #F2F6FA;
            --mp-border: #D6DEE8;
            --mp-text: #172433;
            --mp-muted: #5F7083;
            --mp-primary: #2E5B84;
            --mp-primary-dark: #234767;
            --mp-accent: #C96B4A;
            --mp-danger: #DC2626;
            --mp-sidebar: #1B2633;
            --mp-sidebar-soft: #253243;
            --mp-radius: 14px;
            --mp-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 10px 28px rgba(15, 23, 42, 0.05);
            --mp-shadow-hover: 0 4px 10px rgba(15, 23, 42, 0.06), 0 16px 32px rgba(15, 23, 42, 0.08);
        }

        html, body, .stApp, [class*="css"] {
            background: var(--mp-bg);
            color: var(--mp-text);
            color-scheme: only light;
            font-family: Inter, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            font-size: 16.5px;
            line-height: 1.6;
            font-feature-settings: "cv02", "cv03", "cv04", "cv11";
            -webkit-font-smoothing: antialiased;
        }

        .stApp p,
        .stApp span,
        .stApp label,
        .stApp li,
        .stApp div[data-testid="stMarkdownContainer"] {
            color: var(--mp-text);
        }

        .stApp small,
        .stApp [data-testid="stCaptionContainer"],
        .stApp [data-testid="stWidgetLabel"] p {
            color: var(--mp-muted);
            font-size: 0.96rem;
        }

        h1, h2, h3, h4, h5, h6 {
            color: var(--mp-text);
            font-family: Inter, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            letter-spacing: -0.025em;
        }

        #MainMenu, footer, [data-testid="stDecoration"], [data-testid="stToolbar"] {
            display: none;
        }
        [data-testid="stHeader"] {
            height: 0;
            background: transparent;
        }
        [data-testid="stSidebar"] {
            width: 360px !important;
            min-width: 360px !important;
            background:
                radial-gradient(circle at 15% 0%, rgba(75, 139, 190, 0.14), transparent 17rem),
                linear-gradient(180deg, #172536 0%, #13202F 100%);
            border-right: 1px solid #31445A;
            box-shadow: 10px 0 32px rgba(15, 23, 42, 0.12);
        }
        [data-testid="stSidebar"] > div:first-child {
            width: 360px !important;
            padding: 1.25rem 1.15rem 2rem;
        }
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] span,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] label p,
        [data-testid="stSidebar"] .stCaptionContainer,
        [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
            color: #D9E4EF;
        }
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: #F8FAFC;
            letter-spacing: -0.02em;
        }
        [data-testid="stSidebar"] h2 {
            font-size: 1.35rem;
            padding-bottom: 0.3rem;
        }
        .mp-filter-header {
            margin-bottom: 1rem;
            padding: 0.2rem 0.1rem 0.15rem;
        }
        .mp-filter-kicker {
            color: #7DB7E8;
            font-size: 0.76rem;
            font-weight: 800;
            letter-spacing: 0.1em;
            text-transform: uppercase;
        }
        .mp-filter-title {
            margin-top: 0.18rem;
            color: #FFFFFF;
            font-size: 1.45rem;
            font-weight: 800;
            letter-spacing: -0.03em;
        }
        .mp-filter-description {
            margin-top: 0.35rem;
            color: #B6C6D6;
            font-size: 0.9rem;
            line-height: 1.45;
        }
        .mp-filter-section {
            margin: 0.3rem 0 0.65rem;
            color: #8FB7D8;
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        .mp-filter-summary {
            margin: 0.9rem 0 0.65rem;
            padding: 0.8rem 0.9rem;
            color: #C9D7E5;
            background: rgba(91, 139, 181, 0.12);
            border: 1px solid rgba(125, 183, 232, 0.22);
            border-radius: 12px;
            font-size: 0.88rem;
            line-height: 1.5;
        }
        .mp-filter-summary strong {
            color: #FFFFFF;
        }
        [data-testid="stSidebar"] hr {
            border-color: rgba(203, 213, 225, 0.16);
            margin: 1.2rem 0;
        }
        [data-testid="stSidebar"] .stDataFrame {
            border: 1px solid #3A4D62;
            border-radius: 14px;
            background: #F8FAFC;
            box-shadow: 0 8px 24px rgba(2, 8, 23, 0.16);
        }
        [data-testid="stSidebar"] code {
            color: #B8D9F4;
            background: #101B28;
            border: 1px solid #34485D;
            border-radius: 10px;
        }
        [data-testid="stSidebar"] div[data-baseweb="select"] > div,
        [data-testid="stSidebar"] div[data-baseweb="input"] > div,
        [data-testid="stSidebar"] [data-testid="stTextInput"] input,
        [data-testid="stSidebar"] [data-testid="stNumberInput"] input {
            color: var(--mp-text);
            background: #F8FAFC;
            border-color: #AEBECD;
            border-radius: 10px;
            box-shadow: 0 2px 5px rgba(2, 8, 23, 0.12);
        }
        [data-testid="stSidebar"] div[data-baseweb="select"] span,
        [data-testid="stSidebar"] input {
            color: var(--mp-text);
        }
        [data-testid="stSidebar"] [data-testid="stCheckbox"] label,
        [data-testid="stSidebar"] [data-testid="stCheckbox"] label span,
        [data-testid="stSidebar"] [data-testid="stSliderTickBarMin"],
        [data-testid="stSidebar"] [data-testid="stSliderTickBarMax"] {
            color: #C5D3E0;
        }
        [data-testid="stSidebar"] [data-testid="stSliderTickBar"] {
            color: #9FB1C2;
        }
        [data-testid="stSidebar"] [data-testid="stSlider"] [data-baseweb="slider"] > div > div {
            background: #D7E1EC;
        }
        [data-testid="stSidebar"] [data-testid="stSlider"] [role="slider"] {
            background: var(--mp-primary);
            box-shadow: 0 2px 10px rgba(46, 91, 132, 0.18);
        }
        [data-testid="stSidebar"] .stCodeBlock,
        [data-testid="stSidebar"] .stTextInput,
        [data-testid="stSidebar"] .stDataFrame {
            margin-top: 0.35rem;
        }
        [data-testid="stSidebar"] [data-testid="stForm"] {
            padding: 1rem;
            background: rgba(31, 48, 67, 0.92);
            border: 1px solid #354A60;
            border-radius: 16px;
            box-shadow: 0 14px 30px rgba(2, 8, 23, 0.18);
        }
        [data-testid="stSidebar"] [data-testid="stForm"] [data-testid="stWidgetLabel"] p {
            color: #E3EBF3;
            font-size: 0.88rem;
            font-weight: 700;
        }
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button {
            width: 100%;
            margin-top: 0.4rem;
            color: #FFFFFF;
            background: #3F7EAF;
            border: 1px solid #5593C2;
            border-radius: 10px;
            box-shadow: 0 7px 18px rgba(2, 8, 23, 0.24);
        }
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button p {
            color: #FFFFFF;
        }
        [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button:hover {
            color: #FFFFFF;
            background: #4D8DBE;
            border-color: #72ACD7;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] {
            margin-top: 0.55rem;
            background: rgba(31, 48, 67, 0.76);
            border-color: #354A60;
            box-shadow: none;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] summary p {
            color: #DCE7F1;
            font-weight: 700;
        }
        [data-testid="stSidebar"] [data-testid="stExpander"] summary svg {
            fill: #AFC2D4;
        }
        [data-testid="stSidebar"] [data-testid="stCheckbox"] label span {
            color: #D9E4EF;
        }
        .block-container {
            max-width: 1580px;
            padding-top: 1.25rem;
            padding-bottom: 2rem;
            padding-left: 1.6rem;
            padding-right: 1.6rem;
        }
        .stApp {
            background: var(--mp-bg);
        }
        .mp-hero {
            background:
                radial-gradient(circle at 88% 20%, rgba(255, 255, 255, 0.14), transparent 16rem),
                linear-gradient(120deg, #304C68 0%, var(--mp-primary) 68%, #54779B 125%);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 18px;
            padding: 1.35rem 1.5rem;
            color: #ffffff;
            box-shadow: 0 14px 34px rgba(46, 91, 132, 0.16);
            margin-bottom: 1rem;
            display: grid;
            grid-template-columns: minmax(0, 1fr) auto;
            gap: 1rem;
            align-items: center;
        }
        .mp-hero h1 {
            margin: 0;
            color: #ffffff;
            font-size: clamp(1.7rem, 2.6vw, 2.2rem);
            line-height: 1.15;
            letter-spacing: -0.035em;
        }
        .mp-hero p {
            margin: 0.5rem 0 0;
            color: rgba(255, 255, 255, 0.92);
            font-size: 1.04rem;
            line-height: 1.55;
            max-width: 960px;
        }
        .mp-status {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            color: #FFFFFF;
            background: rgba(255, 255, 255, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.22);
            border-radius: 999px;
            padding: 0.45rem 0.75rem;
            font-size: 0.92rem;
            font-weight: 700;
            backdrop-filter: blur(8px);
        }
        .mp-card {
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: var(--mp-radius);
            padding: 1rem 1rem 0.9rem;
            box-shadow: var(--mp-shadow);
        }
        .mp-card-label {
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--mp-muted);
            margin-bottom: 0.35rem;
        }
        .mp-card-value {
            font-size: 1.7rem;
            font-weight: 700;
            color: var(--mp-text);
            line-height: 1.1;
        }
        .mp-card-hint {
            margin-top: 0.35rem;
            color: var(--mp-muted);
            font-size: 0.98rem;
        }
        .mp-section-title {
            font-size: 1.28rem;
            font-weight: 800;
            color: var(--mp-text);
            letter-spacing: -0.02em;
            margin-bottom: 0.25rem;
        }
        .mp-section-caption {
            color: var(--mp-muted);
            font-size: 1rem;
            line-height: 1.5;
            margin-bottom: 1rem;
        }
        .mp-metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
            gap: 0.8rem;
            margin: 0.9rem 0 1rem;
        }
        .mp-metric-card {
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-top: 3px solid var(--mp-primary);
            border-radius: var(--mp-radius);
            padding: 0.95rem 1rem 0.9rem;
            box-shadow: var(--mp-shadow);
            position: relative;
            overflow: hidden;
            transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
        }
        .mp-metric-card:hover {
            transform: translateY(-2px);
            border-color: #B8C9DB;
            box-shadow: var(--mp-shadow-hover);
        }
        .mp-metric-label {
            text-transform: uppercase;
            letter-spacing: 0.075em;
            font-size: 0.8rem;
            color: var(--mp-muted);
            font-weight: 700;
            margin-bottom: 0.45rem;
        }
        .mp-metric-value {
            font-size: 1.6rem;
            font-weight: 800;
            color: var(--mp-primary);
            line-height: 1.1;
            letter-spacing: -0.025em;
        }
        .mp-metric-hint {
            margin-top: 0.3rem;
            color: var(--mp-muted);
            font-size: 0.92rem;
        }
        .mp-panel {
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: var(--mp-radius);
            padding: 1rem 1.05rem 1.1rem;
            box-shadow: var(--mp-shadow);
            margin-bottom: 1rem;
        }
        .mp-panel-title {
            font-size: 1.06rem;
            font-weight: 700;
            color: var(--mp-text);
        }
        .mp-panel-subtitle {
            color: var(--mp-muted);
            font-size: 0.98rem;
            margin-top: 0.2rem;
            margin-bottom: 0.8rem;
        }
        .mp-legend {
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
            margin-bottom: 0.4rem;
            color: var(--mp-muted);
            font-size: 0.96rem;
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
            font-size: 12px;
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
            font-size: 0.94rem;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .mp-bar-track {
            background: #EAF0F5;
            border-radius: 4px;
            height: 9px;
            overflow: hidden;
        }
        .mp-bar-fill {
            height: 100%;
            border-radius: 4px;
        }
        .mp-bar-value {
            text-align: right;
            color: var(--mp-text);
            font-weight: 600;
        }
        .mp-empty {
            color: var(--mp-muted);
            background: var(--mp-surface-soft);
            border: 1px dashed rgba(148, 163, 184, 0.35);
            border-radius: 10px;
            padding: 0.9rem 1rem;
        }
        .mp-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.98rem;
        }
        .mp-table th {
            text-align: left;
            color: var(--mp-muted);
            font-size: 0.84rem;
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
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: var(--mp-radius);
            padding: 0.95rem 1rem;
            box-shadow: var(--mp-shadow);
        }
        .mp-summary-label {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--mp-muted);
            margin-bottom: 0.35rem;
        }
        .mp-summary-value {
            font-size: 1.32rem;
            font-weight: 800;
            color: var(--mp-text);
            line-height: 1.2;
        }
        .mp-summary-note {
            margin-top: 0.3rem;
            color: var(--mp-muted);
            font-size: 0.96rem;
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
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: 999px;
            padding: 0.4rem 0.72rem;
            color: var(--mp-text);
            font-size: 0.92rem;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
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
        .mp-insight-card {
            min-height: 190px;
            border-top: 3px solid var(--mp-primary);
        }
        .mp-insight-card .mp-signal-list {
            color: var(--mp-muted);
            padding-left: 1.05rem;
        }
        .mp-insight-card .mp-signal-list li::marker {
            color: var(--mp-accent);
        }
        .stDataFrame {
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: var(--mp-radius);
            overflow: hidden;
            box-shadow: var(--mp-shadow);
        }
        div[data-testid="stTabs"] [data-baseweb="tab-list"] {
            gap: 0.35rem;
            background: #EAF0F5;
            border-radius: 12px;
            padding: 0.3rem;
        }
        div[data-testid="stTabs"] button[role="tab"] {
            font-weight: 700;
            color: #4D5E70;
            border-radius: 9px;
            padding: 0.65rem 1rem;
            font-size: 0.98rem;
            border-bottom: 0;
        }
        div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
            color: #FFFFFF;
            background: var(--mp-primary);
            box-shadow: 0 2px 6px rgba(46, 91, 132, 0.2);
        }
        div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] p,
        div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] span {
            color: #FFFFFF;
        }
        div[data-testid="stTabs"] button[role="tab"]:not([aria-selected="true"]) p,
        div[data-testid="stTabs"] button[role="tab"]:not([aria-selected="true"]) span {
            color: #4D5E70;
        }
        div[data-testid="stTabs"] [data-baseweb="tab-highlight"] {
            display: none;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextInput"] input {
            color: var(--mp-text);
            background: var(--mp-surface);
            border-color: var(--mp-border);
            border-radius: 10px;
        }
        div[data-baseweb="select"] span,
        div[data-baseweb="input"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stTextInput"] input {
            color: var(--mp-text);
            -webkit-text-fill-color: var(--mp-text);
        }
        div[data-baseweb="popover"],
        div[data-baseweb="popover"] ul,
        div[data-baseweb="menu"],
        div[role="listbox"] {
            color: var(--mp-text);
            background: var(--mp-surface);
        }
        div[role="option"],
        div[role="option"] span {
            color: var(--mp-text);
            background: var(--mp-surface);
        }
        div[role="option"]:hover,
        div[role="option"][aria-selected="true"] {
            background: #EDF3F8;
        }
        [data-testid="stSlider"] [role="slider"] {
            background: var(--mp-primary);
            border-color: #ffffff;
            box-shadow: 0 1px 5px rgba(46, 91, 132, 0.24);
        }
        .stButton button,
        .stDownloadButton button {
            border-radius: 10px;
            min-height: 2.65rem;
            font-weight: 700;
            transition: transform 140ms ease, box-shadow 140ms ease;
        }
        .stButton button:hover,
        .stDownloadButton button:hover {
            transform: translateY(-1px);
            box-shadow: 0 8px 18px rgba(15, 23, 42, 0.12);
        }
        .stDownloadButton button {
            border: 1px solid var(--mp-border);
            background: #FDFEFE;
            color: var(--mp-primary);
        }
        .stButton button[kind="primary"] {
            border: 0;
            background: var(--mp-primary);
            color: #ffffff;
        }
        .stButton button[kind="primary"] p,
        .stButton button[kind="primary"] span {
            color: #FFFFFF;
        }
        .stButton button[kind="primary"]:hover {
            background: var(--mp-primary-dark);
        }
        [data-testid="stAlert"] {
            color: var(--mp-text);
            background: var(--mp-surface);
            border-radius: 12px;
            border: 1px solid var(--mp-border);
        }
        [data-testid="stExpander"] {
            color: var(--mp-text);
            background: var(--mp-surface);
            border: 1px solid var(--mp-border);
            border-radius: 12px;
        }
        [data-testid="stCheckbox"] label span,
        [data-testid="stRadio"] label span,
        [data-testid="stSelectSlider"] p,
        [data-testid="stSlider"] p {
            color: var(--mp-text);
            font-size: 0.98rem;
        }

        [data-testid="stDataFrame"] div[role="columnheader"],
        [data-testid="stDataFrame"] div[role="gridcell"] {
            font-size: 0.96rem;
        }

        [data-testid="stDataFrame"] div[role="columnheader"] {
            color: #4B5E71;
            background: #F3F7FA;
        }

        @media (max-width: 760px) {
            .block-container {
                padding: 0.75rem 0.8rem 1.5rem;
            }
            .mp-hero {
                grid-template-columns: 1fr;
                padding: 1.1rem;
                border-radius: 14px;
            }
            .mp-status {
                justify-self: start;
            }
            .mp-metric-grid {
                grid-template-columns: 1fr 1fr;
            }
        }

        @media (max-width: 480px) {
            .mp-metric-grid {
                grid-template-columns: 1fr;
            }
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
    payload = api_get(
        "/api/hourly-demand",
        {
            "start_month": start_month,
            "end_month": end_month,
            "limit": limit,
            "offset": 0,
        },
    )
    rows = payload.get("rows", []) if isinstance(payload, dict) else payload
    df = pd.DataFrame(rows)
    if not df.empty:
        df["pickup_hour"] = pd.to_datetime(df["pickup_hour"])
    return df


@st.cache_data(ttl=300)
def load_demand_trends(start_month: str, end_month: str) -> dict[str, Any]:
    return api_get(
        "/api/demand-trends",
        {"start_month": start_month, "end_month": end_month},
    )


@st.cache_data(ttl=300)
def load_zones(start_month: str, end_month: str, limit: int) -> pd.DataFrame:
    return pd.DataFrame(
        api_get(
            "/api/zone-summary",
            {
                "start_month": start_month,
                "end_month": end_month,
                "limit": limit,
            },
        )
    )


@st.cache_data(ttl=300)
def load_payment_tip(start_month: str, end_month: str) -> pd.DataFrame:
    return pd.DataFrame(
        api_get(
            "/api/payment-tip-summary",
            {"start_month": start_month, "end_month": end_month},
        )
    )

@st.cache_data(ttl=300)
def load_zone_trend(start_month: str, end_month: str, limit: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        api_get(
            "/api/zone-trend",
            {"start_month": start_month, "end_month": end_month, "limit": limit},
        )
    )

@st.cache_data(ttl=300)
def load_weather_correlation(start_month: str, end_month: str) -> pd.DataFrame:
    return pd.DataFrame(
        api_get(
            "/api/weather-correlation",
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


@st.cache_data(ttl=3600)
def load_route_estimate(pickup_zone_id: int, dropoff_zone_id: int) -> dict[str, Any]:
    return api_get(
        "/api/route-estimate",
        {
            "pickup_zone_id": int(pickup_zone_id),
            "dropoff_zone_id": int(dropoff_zone_id),
        },
    )


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


def render_metric_strip(items: list[tuple[str, str, str] | tuple[str, str, str, str]]) -> str:
    cards = []
    for item in items:
        label = item[0]
        value = item[1]
        hint = item[2]
        delta_html = ""
        if len(item) == 4 and item[3]:
            delta = item[3]
            color = "#10B981" if delta.startswith("↑") else "#EF4444" if delta.startswith("↓") else "#6B7280"
            delta_html = f'<span style="color: {color}; font-weight: 600; margin-left: 0.5rem; font-size: 0.85em;">{delta}</span>'
        cards.append(f'<div class="mp-metric-card"><div class="mp-metric-label">{label}</div><div class="mp-metric-value" style="display: flex; align-items: baseline;">{value}{delta_html}</div><div class="mp-metric-hint">{hint}</div></div>')
    
    cards_html = "".join(cards)
    return f'<div class="mp-metric-grid">{cards_html}</div>'


def render_bar_panel(title: str, subtitle: str, labels: list[str], values: list[float], accent: str = "#0B60AB") -> str:
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


def render_insight_card(title: str, lines: list[str]) -> str:
    items = "".join(f"<li>{line}</li>" for line in lines)
    return (
        f'<div class="mp-panel mp-insight-card">'
        f'<div class="mp-panel-title">{title}</div>'
        f'<ul class="mp-signal-list">{items}</ul>'
        f"</div>"
    )


def render_badge_strip(items: list[tuple[str, str]]) -> str:
    badges = "".join(f'<span class="mp-badge"><strong>{label}</strong>{value}</span>' for label, value in items)
    return f'<div class="mp-badge-row">{badges}</div>'


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
    if "demand_share" not in enriched:
        enriched["demand_share"] = pd.NA
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
            <p>Analytics mart được phục vụ từ PostgreSQL qua FastAPI; Demand demo dùng artifact đã lưu và Fare/Tip inference được phục vụ qua FastAPI.</p>
        </div>
        <div class="mp-status">PostgreSQL + FastAPI</div>
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
    st.markdown(
        """
        <div class="mp-filter-header">
            <div class="mp-filter-kicker">Điều khiển báo cáo</div>
            <div class="mp-filter-title">Bộ lọc dữ liệu</div>
            <div class="mp-filter-description">
                Chọn phạm vi phân tích và mức chi tiết trước khi tải dữ liệu.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.form("dashboard_filter_form", border=False):
        st.markdown('<div class="mp-filter-section">Thời gian phân tích</div>', unsafe_allow_html=True)
        if available_months:
            selected_start_month = st.selectbox(
                "Từ tháng",
                options=available_months,
                index=default_start_index,
                format_func=format_month,
            )
            selected_end_month = st.selectbox(
                "Đến tháng",
                options=available_months,
                index=default_end_index,
                format_func=format_month,
            )
        else:
            selected_start_month = st.text_input(
                "Từ tháng",
                value=default_start_month,
            )
            selected_end_month = st.text_input(
                "Đến tháng",
                value=default_end_month,
            )

        st.markdown('<div class="mp-filter-section">Mức độ chi tiết</div>', unsafe_allow_html=True)
        hourly_limit = st.selectbox(
            "Bảng nhu cầu theo giờ",
            options=[1000, 2500, 5000, 10000, 20000],
            index=2,
            format_func=lambda value: f"{value:,} dòng",
        )
        zone_limit = st.selectbox(
            "Số zone hiển thị",
            options=[10, 15, 20, 30, 50],
            index=2,
            format_func=lambda value: f"Top {value} zone",
        )
        show_raw_tables = st.checkbox("Hiển thị bảng dữ liệu thô", value=False)
        st.form_submit_button("Áp dụng bộ lọc", type="primary", use_container_width=True)

    if (
        available_months
        and available_months.index(selected_start_month) > available_months.index(selected_end_month)
    ):
        start_month, end_month = selected_end_month, selected_start_month
        st.warning("Mốc bắt đầu đứng sau mốc kết thúc, hệ thống đã tự đổi thứ tự.")
    else:
        start_month, end_month = selected_start_month, selected_end_month

    st.markdown(
        f"""
        <div class="mp-filter-summary">
            Đang phân tích<br>
            <strong>{format_month(start_month)} - {format_month(end_month)}</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("Thông tin hệ thống", expanded=False):
        st.caption("Dashboard API")
        st.code(API_URL, language=None)
        st.caption("Các mart PostgreSQL")
        mart_table = pd.DataFrame(meta.get("tables", [])).rename(
            columns={"table_name": "Tên mart", "row_count": "Số dòng"}
        )
        st.dataframe(mart_table, hide_index=True, use_container_width=True)

try:
    summary = load_summary(start_month, end_month)
    trends = load_demand_trends(start_month, end_month)
    hourly_df = enrich_hourly(load_hourly(start_month, end_month, hourly_limit))
    zone_df = enrich_zones(load_zones(start_month, end_month, 263))
    payment_df = enrich_payments(load_payment_tip(start_month, end_month))
    zone_trend_df = load_zone_trend(start_month, end_month, 5)
    weather_corr_df = load_weather_correlation(start_month, end_month)
except requests.RequestException as exc:
    st.error(f"Không thể tải dữ liệu dashboard từ FastAPI: {exc}")
    st.stop()

monthly_df = pd.DataFrame(trends.get("monthly", []))
if not monthly_df.empty:
    monthly_df["pickup_month"] = pd.to_datetime(
        monthly_df["pickup_year_month"] + "-01"
    )
hourly_profile = pd.DataFrame(trends.get("hourly", []))
day_profile = pd.DataFrame(trends.get("weekday", []))
if not day_profile.empty:
    day_profile["day_name_vi"] = day_profile["iso_day_of_week"].map(
        {
            1: "Thứ Hai",
            2: "Thứ Ba",
            3: "Thứ Tư",
            4: "Thứ Năm",
            5: "Thứ Sáu",
            6: "Thứ Bảy",
            7: "Chủ Nhật",
        }
    )

latest_hour = pd.to_datetime(summary.get("last_data_hour")) if summary.get("last_data_hour") else None
pipeline_updated_at = (
    pd.to_datetime(summary.get("latest_dashboard_processed_at"))
    if summary.get("latest_dashboard_processed_at")
    else None
)
peak_hour = pd.to_datetime(summary.get("peak_hour")) if summary.get("peak_hour") else None
peak_hour_label = peak_hour.strftime("%Y-%m-%d %H:00") if peak_hour is not None and not pd.isna(peak_hour) else "-"
peak_hour_total = metric_value(summary.get("peak_total_demand"))
top_zone = zone_df.iloc[0] if not zone_df.empty else None
leading_payment_type = summary.get("leading_payment_type")
leading_payment_label = (
    normalize_payment_label(leading_payment_type)
    if leading_payment_type is not None
    else None
)
avg_tip_percent = summary.get("avg_tip_percent")
avg_fare_amount = summary.get("avg_fare_amount")
avg_hourly_demand = safe_ratio(summary.get("total_demand"), summary.get("hourly_points"))
peak_share = summary.get("rush_hour_share")
top_zone_share = float(top_zone["demand_share"]) if top_zone is not None and pd.notna(top_zone["demand_share"]) else None
weekend_share = summary.get("weekend_share")
cashless_share = summary.get("leading_payment_share")

if latest_hour is not None and latest_hour.strftime("%Y-%m") < end_month:
    st.warning(
        "Dữ liệu API hiện chỉ phủ đến "
        f"{latest_hour.strftime('%m/%Y')}, chưa đạt tháng kết thúc "
        f"{format_month(end_month)} đã chọn."
    )

def get_delta_str(current: float, prev: float) -> str:
    if not prev or prev == 0:
        return ""
    pct = ((current - prev) / prev) * 100
    if pct > 0:
        return f"↑ {pct:.1f}% vs kỳ trước"
    elif pct < 0:
        return f"↓ {abs(pct):.1f}% vs kỳ trước"
    return "0% vs kỳ trước"

metric_rows = [
    (
        "Tổng nhu cầu", 
        metric_value(summary.get("total_demand")), 
        f"{metric_value(summary.get('hourly_points'))} bản ghi theo giờ",
        get_delta_str(summary.get("total_demand", 0), summary.get("prev_total_demand", 0))
    ),
    (
        "Nhu cầu giờ trung bình", 
        metric_value(avg_hourly_demand), 
        "Mức lưu lượng trung bình trên mỗi giờ dữ liệu",
        get_delta_str(avg_hourly_demand, summary.get("prev_avg_hourly_demand", 0))
    ),
    (
        "Zone hoạt động trung bình", 
        metric_value(summary.get("avg_active_zones")), 
        "Phạm vi phục vụ trong giai đoạn chọn",
        get_delta_str(summary.get("avg_active_zones", 0), summary.get("prev_avg_active_zones", 0))
    ),
    (
        "Nhu cầu trên mỗi zone", 
        metric_value(summary.get("avg_demand_per_active_zone")), 
        "Hữu ích để đánh giá áp lực theo khu vực",
        get_delta_str(summary.get("avg_demand_per_active_zone", 0), summary.get("prev_avg_demand_per_active_zone", 0))
    ),
    (
        "Giờ đỉnh nhu cầu", 
        peak_hour_label, 
        f"{peak_hour_total} chuyến tại đỉnh"
    ),
    (
        "Giá cước trung bình", 
        metric_value(avg_fare_amount, " USD"), 
        f"Tip trung bình: {metric_value(avg_tip_percent, '%')}".replace("  ", " "),
        get_delta_str(avg_fare_amount, summary.get("prev_avg_fare_amount", 0))
    ),
]
st.markdown(render_metric_strip(metric_rows), unsafe_allow_html=True)

badge_items = [
    ("Độ phủ", f" {len(available_months)} tháng" if available_months else " không rõ"),
    ("Mart giờ", f" {table_counts.get('dashboard_hourly_demand_kpi', 0):,} dòng"),
    ("Mart zone", f" {table_counts.get('dashboard_zone_summary', 0):,} dòng"),
    ("Mart thanh toán", f" {table_counts.get('dashboard_payment_tip_summary', 0):,} dòng"),
    ("Fare/tip", f" {metric_value(summary.get('fare_tip_trip_count'))} chuyến"),
    ("Dữ liệu mới nhất", f" {latest_hour.strftime('%Y-%m-%d %H:00') if latest_hour is not None else '-'}"),
    ("Pipeline cập nhật", f" {pipeline_updated_at.strftime('%Y-%m-%d %H:%M') if pipeline_updated_at is not None else '-'}"),
]
st.markdown(render_badge_strip(badge_items), unsafe_allow_html=True)

summary_items = [
    (
        "Mức tập trung nhu cầu",
        f"{metric_value((top_zone_share or 0) * 100, '%')}" if top_zone_share is not None else "-",
        "Zone dẫn đầu đóng góp lớn nhất vào nhu cầu của giai đoạn đã chọn.",
    ),
    (
        "Tỷ trọng giờ cao điểm",
        f"{metric_value((peak_share or 0) * 100, '%')}" if peak_share is not None else "-",
        "Phần nhu cầu trong các giờ 07--09h và 17--19h.",
    ),
    (
        "Tỷ trọng cuối tuần",
        f"{metric_value((weekend_share or 0) * 100, '%')}" if weekend_share is not None else "-",
        "Hữu ích cho bố trí nhân sự và kế hoạch vận hành cuối tuần.",
    ),
    (
        "Tỷ trọng phương thức thanh toán dẫn đầu",
        f"{metric_value((cashless_share or 0) * 100, '%')}" if cashless_share is not None else "-",
        f"Dẫn đầu: {leading_payment_label}" if leading_payment_label is not None else "Phương thức thanh toán phổ biến nhất trong giai đoạn.",
    ),
]
st.markdown(render_summary_grid(summary_items), unsafe_allow_html=True)

business_signals = [
    f"Nhu cầu tập trung mạnh ở zone đứng đầu, chiếm {metric_value((top_zone_share or 0) * 100, '%')} trong giai đoạn đã chọn." if top_zone_share is not None else "Chưa tính được mức tập trung nhu cầu cho giai đoạn này.",
    f"Các khung giờ cao điểm 07--09h và 17--19h đóng góp {metric_value((peak_share or 0) * 100, '%')} tổng nhu cầu." if peak_share is not None else "Chưa có số liệu về tỷ trọng giờ cao điểm.",
    f"Nhu cầu cuối tuần chiếm {metric_value((weekend_share or 0) * 100, '%')} tổng nhu cầu, hữu ích cho kế hoạch ca trực và phân bổ phương tiện." if weekend_share is not None else "Chưa có số liệu nhu cầu cuối tuần.",
    f"Phương thức thanh toán phổ biến nhất là {leading_payment_label} với tỷ trọng {metric_value((cashless_share or 0) * 100, '%')} trên toàn giai đoạn." if leading_payment_label is not None and cashless_share is not None else "Chưa có số liệu về cơ cấu thanh toán.",
]
st.markdown(render_signal_panel("Tín hiệu nghiệp vụ", business_signals), unsafe_allow_html=True)

st.markdown(
    f"""
    <div class="mp-section-title">Tóm tắt điều hành</div>
    <div class="mp-section-caption">
        Giai đoạn {format_month(start_month)} đến {format_month(end_month)} | Giờ dữ liệu mới nhất {latest_hour.strftime("%Y-%m-%d %H:00") if latest_hour is not None else '-'}
    </div>
    """,
    unsafe_allow_html=True,
)

insight_cols = st.columns([1.4, 1, 1])
with insight_cols[0]:
    insight_lines = [
        f"Giờ có nhu cầu cao nhất: {peak_hour_label} với {peak_hour_total} chuyến",
        f"Zone dẫn đầu: {top_zone['pickup_zone']} ({top_zone['pickup_borough']})" if top_zone is not None else "Zone dẫn đầu: -",
        f"Phương thức thanh toán chủ đạo: {leading_payment_label}" if leading_payment_label is not None else "Phương thức thanh toán chủ đạo: -",
        f"Tỷ lệ tip trung bình: {metric_value(avg_tip_percent, '%')}" if avg_tip_percent is not None else "Tỷ lệ tip trung bình: -",
    ]
    st.markdown(render_insight_card("Nhận định vận hành", insight_lines), unsafe_allow_html=True)
with insight_cols[1]:
    coverage_lines = [
        f"Số tháng trong warehouse: {len(available_months) if available_months else metric_value(summary.get('hourly_points'))}",
        f"Số điểm giờ trong kỳ: {metric_value(summary.get('hourly_points'))}",
        f"Số điểm giờ tải cho bảng: {len(hourly_df):,}",
        f"Số dòng zone: {len(zone_df):,}",
    ]
    st.markdown(render_insight_card("Độ phủ dữ liệu", coverage_lines), unsafe_allow_html=True)
with insight_cols[2]:
    weather_lines = [
        f"Nhiệt độ trung bình: {metric_value(summary.get('avg_temperature_f'), ' F')}",
        f"Lượng mưa trung bình: {metric_value(summary.get('avg_precipitation_mm'), ' mm')}",
        f"Tip trung bình: {metric_value(avg_tip_percent, '%')}",
    ]
    st.markdown(render_insight_card("Bối cảnh thời tiết", weather_lines), unsafe_allow_html=True)

tab_demand, tab_zones, tab_payments, tab_ml, tab_simulator = st.tabs(["Nhu cầu", "Zone", "Thanh toán & tip", "Dự báo ML", "Simulator Giá cước & Tip"])

with tab_demand:
    st.markdown("<div class='mp-section-title'>Động lực nhu cầu</div>", unsafe_allow_html=True)
    st.caption("Xem nhu cầu taxi thay đổi theo tháng, theo giờ và theo điều kiện thời tiết.")
    if monthly_df.empty or hourly_profile.empty or day_profile.empty:
        st.warning("Không có dữ liệu nhu cầu theo giờ cho giai đoạn đã chọn.")
    else:
        top_row = st.columns([1.3, 1])
        with top_row[0]:
            st.markdown(
                render_line_panel(
                    "Xu hướng nhu cầu theo tháng",
                    "So sánh tổng nhu cầu với số zone hoạt động trong giai đoạn báo cáo đã chọn.",
                    [x.strftime("%b %y") for x in monthly_df["pickup_month"]],
                    [
                        ("Tổng nhu cầu", monthly_df["total_demand"].astype(float).tolist(), "#0B60AB"),
                        ("Zone hoạt động", monthly_df["avg_active_zones"].astype(float).tolist(), "#475569"),
                    ],
                ),
                unsafe_allow_html=True,
            )
        with top_row[1]:
            st.markdown(
                render_bar_panel(
                    "Giờ cao điểm",
                    "Khung giờ tập trung lưu lượng trong ngày.",
                    [f"{int(hour):02d}:00" for hour in hourly_profile["hour"]],
                    hourly_profile["total_demand"].astype(float).tolist(),
                    accent="#EF4444",
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
                    accent="#0B60AB",
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
                        ("Nhiệt độ (F)", monthly_df["avg_temperature_f"].astype(float).tolist(), "#EF4444"),
                        ("Lượng mưa (mm)", monthly_df["avg_precipitation_mm"].astype(float).tolist(), "#0B60AB"),
                    ],
                ),
                unsafe_allow_html=True,
            )

        st.markdown("**Bảng vận hành**")
        if hourly_df.empty:
            st.info("Không có dòng chi tiết theo giờ cho trang dữ liệu hiện tại.")
        else:
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

        st.markdown("<br><div class='mp-section-title'>Tương quan Thời tiết & Nhu cầu</div>", unsafe_allow_html=True)
        st.caption("Biểu đồ phân tán thể hiện sự thay đổi của nhu cầu theo nhiệt độ và lượng mưa.")
        if weather_corr_df.empty:
            st.warning("Không có dữ liệu thời tiết cho giai đoạn đã chọn.")
        else:
            weather_cols = st.columns(2)
            with weather_cols[0]:
                st.markdown("**Nhu cầu vs. Nhiệt độ (F)**")
                st.scatter_chart(weather_corr_df, x="temp_f", y="daily_demand")
            with weather_cols[1]:
                st.markdown("**Nhu cầu vs. Lượng mưa (mm)**")
                st.scatter_chart(weather_corr_df, x="precip_mm", y="daily_demand")

with tab_zones:
    st.markdown("<div class='mp-section-title'>Hiệu suất theo zone</div>", unsafe_allow_html=True)
    st.caption("Xác định nơi nhu cầu tập trung và zone nào hoạt động lâu nhất.")
    if zone_df.empty:
        st.warning("Không có dữ liệu tổng hợp zone.")
    else:
        st.markdown("<br><div class='mp-section-title'>Xu hướng các Zone nổi bật</div>", unsafe_allow_html=True)
        st.caption("Tổng nhu cầu hàng tháng của Top 5 Zone hàng đầu.")
        if zone_trend_df.empty:
            st.info("Không đủ dữ liệu cho biểu đồ xu hướng.")
        else:
            pivoted_trend = zone_trend_df.pivot(index="pickup_year_month", columns="pickup_zone", values="total_demand").fillna(0)
            st.line_chart(pivoted_trend)
            
        visible_zone_df = zone_df.head(zone_limit).copy()
        borough_df = (
            zone_df.groupby("pickup_borough", as_index=False)
            .agg(total_demand=("total_demand", "sum"), active_hours=("active_hours", "sum"))
            .sort_values("total_demand", ascending=False)
        )

        zone_top = visible_zone_df.head(10)
        zone_cols = st.columns([1.15, 0.95])
        with zone_cols[0]:
            st.markdown(
                render_bar_panel(
                    "Zone có nhu cầu cao nhất",
                    "Các zone ưu tiên cho kế hoạch cung ứng và giám sát dịch vụ.",
                    zone_top["pickup_zone"].astype(str).tolist(),
                    zone_top["total_demand"].astype(float).tolist(),
                    accent="#0B60AB",
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
                    accent="#475569",
                ),
                unsafe_allow_html=True,
            )

        zone_view = visible_zone_df[[
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
        zone_view["demand_share"] = (zone_view["demand_share"].fillna(0) * 100).round(2).astype(str) + "%"
        zone_view["demand_intensity"] = zone_view["demand_intensity"].fillna(0).round(2)
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
            data=df_to_csv(visible_zone_df),
            file_name=f"zone_summary_top_{zone_limit}.csv",
            mime="text/csv",
        )

with tab_payments:
    st.markdown("<div class='mp-section-title'>Hành vi thanh toán và tip</div>", unsafe_allow_html=True)
    st.caption("Theo dõi cách khách hàng thanh toán và mức tip thay đổi theo tháng.")
    if payment_df.empty:
        st.warning("Không có dữ liệu thanh toán/tip cho giai đoạn đã chọn.")
    else:
        payment_weighted = payment_df.assign(
            fare_total=payment_df["avg_fare_amount"] * payment_df["trip_count"],
            tip_pct_total=payment_df["avg_tip_percent"] * payment_df["trip_count"],
        )
        payment_monthly = (
            payment_weighted.groupby("pickup_year_month", as_index=False)
            .agg(
                trip_count=("trip_count", "sum"),
                fare_total=("fare_total", "sum"),
                tip_pct_total=("tip_pct_total", "sum"),
            )
            .sort_values("pickup_year_month")
        )
        payment_monthly["avg_fare_amount"] = (
            payment_monthly["fare_total"] / payment_monthly["trip_count"]
        )
        payment_monthly["avg_tip_percent"] = (
            payment_monthly["tip_pct_total"] / payment_monthly["trip_count"]
        )
        payment_mix = payment_weighted.groupby("payment_type", as_index=False).agg(
            trip_count=("trip_count", "sum"),
            fare_total=("fare_total", "sum"),
            tip_pct_total=("tip_pct_total", "sum"),
        ).sort_values("trip_count", ascending=False)
        payment_mix["avg_fare_amount"] = payment_mix["fare_total"] / payment_mix["trip_count"]
        payment_mix["avg_tip_percent"] = payment_mix["tip_pct_total"] / payment_mix["trip_count"]

        payment_cols = st.columns([1.15, 0.95])
        with payment_cols[0]:
            st.markdown(
                render_line_panel(
                    "Chỉ số thanh toán theo tháng",
                    "Xu hướng số chuyến và tỷ lệ tip trung bình trong giai đoạn đã chọn.",
                    payment_monthly["pickup_year_month"].astype(str).tolist(),
                    [
                        ("Số chuyến", payment_monthly["trip_count"].astype(float).tolist(), "#0B60AB"),
                        ("Tip TB %", payment_monthly["avg_tip_percent"].astype(float).tolist(), "#EF4444"),
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
                    accent="#0B60AB",
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
        # Filters are applied only on submit so chart/table results remain stable
        # while the user edits multiple controls.
        st.markdown("<div style='font-weight:700; margin-bottom:0.4rem;'>🔍 Bộ lọc dự báo động</div>", unsafe_allow_html=True)
        zone_names_list = ["Tất cả"] + sorted(list(ml_df["zone_name"].dropna().unique()))
        zone_types = ["Tất cả", "Sân bay", "Manhattan core", "Bình thường"]
        weather_options = ["Tất cả", "Chỉ ngày mưa (>0mm)", "Chỉ ngày lạnh buốt (<36°F)", "Khô ráo và ấm"]
        day_options = ["Tất cả", "Ngày thường (Thứ 2-6)", "Cuối tuần (Thứ 7, CN)", "Ngày lễ"]
        min_date = ml_df["pickup_hour"].min().date()
        max_date = ml_df["pickup_hour"].max().date()
        st.session_state.setdefault(
            "ml_applied_filters",
            {
                "zone": "Tất cả",
                "zone_type": "Tất cả",
                "weather": "Tất cả",
                "day_type": "Tất cả",
                "date_range": (min_date, max_date),
            },
        )

        with st.form("ml_forecast_filter_form", clear_on_submit=False):
            filter_cols = st.columns([1, 1, 1, 1])
            selected_zone_input = filter_cols[0].selectbox(
                "Khu vực (Zone)",
                options=zone_names_list,
                key="ml_zone_filter",
            )
            selected_zone_type_input = filter_cols[1].selectbox(
                "Loại Zone",
                options=zone_types,
                key="ml_zone_type_filter",
            )
            selected_weather_input = filter_cols[2].selectbox(
                "Thời tiết",
                options=weather_options,
                key="ml_weather_filter",
            )
            selected_day_type_input = filter_cols[3].selectbox(
                "Phân loại ngày",
                options=day_options,
                key="ml_day_type_filter",
            )
            selected_date_range_input = st.slider(
                "Chọn khoảng thời gian dự báo (Tháng 12/2024)",
                min_value=min_date,
                max_value=max_date,
                value=(min_date, max_date),
                format="DD/MM",
                key="ml_date_range_slider",
            )
            apply_ml_filters = st.form_submit_button(
                "Dự báo ML với bộ lọc này",
                type="primary",
                use_container_width=True,
            )

        if apply_ml_filters:
            st.session_state["ml_applied_filters"] = {
                "zone": selected_zone_input,
                "zone_type": selected_zone_type_input,
                "weather": selected_weather_input,
                "day_type": selected_day_type_input,
                "date_range": selected_date_range_input,
            }

        applied_filters = st.session_state["ml_applied_filters"]
        selected_zone = applied_filters["zone"]
        selected_zone_type = applied_filters["zone_type"]
        selected_weather = applied_filters["weather"]
        selected_day_type = applied_filters["day_type"]
        selected_date_range = applied_filters["date_range"]

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
                            ("Thực tế", hourly_ml["actual_mean"].astype(float).tolist(), "#0B60AB"),
                            ("Dự báo", hourly_ml["predicted_mean"].astype(float).tolist(), "#EF4444"),
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
                            ("Thực tế", daily_ml["actual_sum"].astype(float).tolist(), "#0B60AB"),
                            ("Dự báo", daily_ml["predicted_sum"].astype(float).tolist(), "#EF4444"),
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
    st.caption("Backend tự tính quãng đường Haversine từ centroid taxi zone và xây dựng đúng feature schema trước khi gọi mô hình.")

    all_zones = load_all_zones()
    if not all_zones:
        st.error("Không tải được taxi zone lookup từ Dashboard API.")
    else:
        zone_by_id = {int(zone["zone_id"]): zone for zone in all_zones}
        zone_ids = sorted(
            zone_by_id,
            key=lambda zone_id: (
                zone_by_id[zone_id].get("borough") or "",
                zone_by_id[zone_id].get("zone_name") or "",
                zone_id,
            ),
        )

        def zone_label(zone_id: int) -> str:
            zone = zone_by_id[zone_id]
            return f"{zone['zone_name']} ({zone['borough']}) [ID {zone_id}]"

        def default_zone_id(zone_name: str, fallback: int) -> int:
            return next(
                (zone_id for zone_id, zone in zone_by_id.items() if zone.get("zone_name") == zone_name),
                fallback,
            )

        st.session_state.setdefault("sim_pickup_zone_id", default_zone_id("JFK Airport", zone_ids[0]))
        st.session_state.setdefault("sim_dropoff_zone_id", default_zone_id("Midtown Center", zone_ids[0]))
        st.session_state.setdefault("sim_passenger_count", 1)
        st.session_state.setdefault("sim_ratecode_id", 1)
        st.session_state.setdefault("sim_payment_type", 1)
        st.session_state.setdefault("sim_month", 12)
        st.session_state.setdefault("sim_day_of_week", 2)
        st.session_state.setdefault("sim_hour", 8)
        st.session_state.setdefault("sim_temperature_f", 34.0)
        st.session_state.setdefault("sim_precipitation_mm", 0.0)
        st.session_state.setdefault("sim_result", None)
        st.session_state.setdefault("sim_error", None)

        route_cols = st.columns(2)
        pickup_zone_id = route_cols[0].selectbox(
            "Khu vực đón (Pickup Zone)",
            options=zone_ids,
            format_func=zone_label,
            key="sim_pickup_zone_id",
        )
        dropoff_zone_id = route_cols[1].selectbox(
            "Khu vực trả (Dropoff Zone)",
            options=zone_ids,
            format_func=zone_label,
            key="sim_dropoff_zone_id",
        )

        route_preview = None
        route_error = None
        try:
            route_preview = load_route_estimate(pickup_zone_id, dropoff_zone_id)
        except requests.RequestException as exc:
            route_error = f"Không tính được quãng đường từ Dashboard API: {exc}"

        if route_preview and route_preview.get("can_predict"):
            st.info(
                f"Quãng đường ước tính: **{route_preview['trip_distance']:.2f} miles** "
                f"(Haversine giữa centroid hai taxi zone, không phải khoảng cách đường bộ)."
            )
        elif route_preview and route_preview.get("same_zone"):
            st.warning(
                "Pickup và dropoff cùng một zone. Centroid distance bằng 0 nên không thể truyền vào model đã train với trip_distance > 0."
            )
        elif route_error:
            st.error(route_error)

        sim_cols = st.columns([1.1, 0.9])
        with sim_cols[0]:
            with st.form("fare_tip_simulator_form", clear_on_submit=False):
                st.markdown("<div style='font-weight:700; margin-bottom:0.4rem;'>Cấu hình mô phỏng</div>", unsafe_allow_html=True)
                time_subcols = st.columns(3)
                passenger_count = time_subcols[0].selectbox(
                    "Số hành khách",
                    options=[1, 2, 3, 4, 5, 6],
                    key="sim_passenger_count",
                )
                ratecode_id = time_subcols[1].selectbox(
                    "Mã biểu giá",
                    options=[1, 2, 3, 4, 5],
                    key="sim_ratecode_id",
                    format_func=lambda value: {
                        1: "Standard",
                        2: "JFK Airport",
                        3: "Newark",
                        4: "Nassau",
                        5: "Negotiated",
                    }[value],
                )
                payment_type = time_subcols[2].selectbox(
                    "Thanh toán",
                    options=[1, 2],
                    key="sim_payment_type",
                    format_func=lambda value: PAYMENT_TYPE_LABELS.get(value, f"Loại {value}"),
                )

                date_subcols = st.columns(3)
                sim_month = date_subcols[0].selectbox(
                    "Tháng",
                    options=list(range(1, 13)),
                    key="sim_month",
                )
                sim_day_of_week = date_subcols[1].selectbox(
                    "Thứ trong tuần",
                    options=[1, 2, 3, 4, 5, 6, 7],
                    key="sim_day_of_week",
                    format_func=lambda value: {
                        1: "Chủ Nhật",
                        2: "Thứ Hai",
                        3: "Thứ Ba",
                        4: "Thứ Tư",
                        5: "Thứ Năm",
                        6: "Thứ Sáu",
                        7: "Thứ Bảy",
                    }[value],
                )
                sim_hour = date_subcols[2].selectbox(
                    "Giờ xuất hành",
                    options=list(range(24)),
                    key="sim_hour",
                )

                weather_subcols = st.columns(2)
                temperature_f = weather_subcols[0].slider(
                    "Nhiệt độ (°F)",
                    min_value=0.0,
                    max_value=100.0,
                    step=1.0,
                    key="sim_temperature_f",
                )
                precipitation_mm = weather_subcols[1].slider(
                    "Lượng mưa (mm)",
                    min_value=0.0,
                    max_value=30.0,
                    step=0.1,
                    key="sim_precipitation_mm",
                )

                run_simulation = st.form_submit_button(
                    "Tính Fare & Tip",
                    type="primary",
                    use_container_width=True,
                    disabled=not bool(route_preview and route_preview.get("can_predict")),
                )

            if run_simulation:
                payload = {
                    "pu_location_id": int(pickup_zone_id),
                    "do_location_id": int(dropoff_zone_id),
                    "passenger_count": int(passenger_count),
                    "ratecode_id": int(ratecode_id),
                    "hour": int(sim_hour),
                    "day_of_week": int(sim_day_of_week),
                    "month": int(sim_month),
                    "temperature_f": float(temperature_f),
                    "precipitation_mm": float(precipitation_mm),
                    "payment_type": int(payment_type),
                }
                try:
                    with st.spinner("Đang tính quãng đường và chạy mô hình Fare/Tip..."):
                        response = requests.post(
                            f"{API_URL}/api/predict/fare-tip",
                            json=payload,
                            timeout=30,
                        )
                        response.raise_for_status()
                    st.session_state["sim_result"] = response.json()
                    st.session_state["sim_error"] = None
                except requests.RequestException as exc:
                    detail = str(exc)
                    if getattr(exc, "response", None) is not None:
                        try:
                            detail = exc.response.json().get("detail", detail)
                        except ValueError:
                            pass
                    st.session_state["sim_error"] = detail

            derived_preview = {
                "Giờ cao điểm": sim_hour in [7, 8, 9, 17, 18, 19],
                "Tín hiệu weekend của model": sim_day_of_week in [5, 6],
                "Có mưa": precipitation_mm > 0,
                "Thời tiết lạnh": temperature_f < 36,
            }
            st.markdown(
                render_badge_strip(
                    [(label, "Có" if enabled else "Không") for label, enabled in derived_preview.items()]
                ),
                unsafe_allow_html=True,
            )

        with sim_cols[1]:
            st.markdown("<div style='font-weight:700; margin-bottom:0.4rem;'>Kết quả mô phỏng gần nhất</div>", unsafe_allow_html=True)
            if st.session_state.get("sim_error"):
                st.error(f"Không thể chạy mô phỏng: {st.session_state['sim_error']}")

            result = st.session_state.get("sim_result")
            if result:
                pickup_result = result["pickup_zone"]
                dropoff_result = result["dropoff_zone"]
                model_badge = "XGBoost Model" if result["model_used"] else "Heuristic fallback"
                st.markdown(
                    render_metric_strip(
                        [
                            ("Quãng đường", f"{result['trip_distance']:.2f} mi", result["distance_method"]),
                            ("Fare dự báo", f"${result['predicted_fare']:.2f}", model_badge),
                            ("Tip dự báo", f"{result['predicted_tip_percent']:.2f}%", f"${result['predicted_tip_amount']:.2f}"),
                            ("Tổng thanh toán", f"${result['total_amount']:.2f}", PAYMENT_TYPE_LABELS.get(result["prediction_input"]["payment_type"], "-")),
                        ]
                    ),
                    unsafe_allow_html=True,
                )
                st.markdown(
                    render_signal_panel(
                        "Chi tiết route và feature",
                        [
                            f"Pickup: {pickup_result['zone_name']} ({pickup_result['borough']}) [ID {pickup_result['zone_id']}].",
                            f"Dropoff: {dropoff_result['zone_name']} ({dropoff_result['borough']}) [ID {dropoff_result['zone_id']}].",
                            f"Feature schema: {', '.join(result['feature_columns'])}.",
                            f"Derived features: {result['derived_features']}.",
                        ],
                    ),
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    """
                    <div class="mp-empty" style="text-align:center; padding:3rem 1.5rem;">
                        Chọn route, nhập thông số và bấm “Tính Fare & Tip”.
                        Kết quả sẽ được giữ nguyên khi bạn thay đổi widget cho đến lần tính tiếp theo.
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


if show_raw_tables:
    with st.expander("Xem dữ liệu thô", expanded=False):
        st.write("Nhu cầu theo giờ")
        st.dataframe(hourly_df.head(50), use_container_width=True, hide_index=True)
        st.write("Tổng hợp zone")
        st.dataframe(zone_df.head(50), use_container_width=True, hide_index=True)
        st.write("Tổng hợp thanh toán/tip")
        st.dataframe(payment_df.head(50), use_container_width=True, hide_index=True)
