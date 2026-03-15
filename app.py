#!/usr/bin/env python3
"""
Streamlit app for reviewing OCR-extracted parity price data.
Cloud deployment version — images from GCS, credentials from Streamlit secrets.
"""

import json
import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageDraw

SCRIPT_DIR = Path(__file__).parent
EXTRACTED_CSV = SCRIPT_DIR / "extracted_pct_of_parity.csv"
COLUMN_POSITIONS_FILE = SCRIPT_DIR / "extracted_pct_of_parity_column_positions.json"

# Google Cloud Storage base URL for page images
GCS_IMAGE_BASE = "https://storage.googleapis.com/parity-price-review-images"

# Google Sheets config
GSHEET_ID = '1KyWnEIeOqZDn394jWQB74bh2cI7A1moMZ3KGEtUjbz4'


# ---------------------------------------------------------------------------
# Google Sheets backend
# ---------------------------------------------------------------------------

def get_gsheet_connection():
    """Return an authorized gspread worksheet, or None if unavailable."""
    if 'gsheet_ws' in st.session_state:
        return st.session_state.gsheet_ws
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        # Try Streamlit secrets first, then local file
        if 'gcp_service_account' in st.secrets:
            creds = Credentials.from_service_account_info(
                dict(st.secrets['gcp_service_account']),
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        else:
            # Local fallback
            creds_path = Path('/Users/soconn8/Dropbox/Personal/tokens/digitizeparitypriceseries-36b3d2770be6.json')
            if not creds_path.exists():
                return None
            creds = Credentials.from_service_account_file(
                str(creds_path),
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(GSHEET_ID)
        ws = sheet.sheet1
        st.session_state.gsheet_ws = ws
        return ws
    except Exception as e:
        st.sidebar.warning(f"Google Sheets unavailable: {e}")
        return None


def load_corrections_from_gsheet(ws):
    """Read all corrections from Google Sheet into a dict keyed by key column."""
    records = ws.get_all_records()
    corrections = {}
    for row in records:
        key = row.get('key', '')
        if not key:
            continue
        corrections[key] = {
            'pct_of_parity': row.get('pct_of_parity', ''),
            'original_pct': row.get('original_pct', ''),
            'parity_price': row.get('parity_price', '') or None,
            'original_parity_price': row.get('original_parity_price', '') or None,
            'status': row.get('status', 'unreviewed'),
            'note': row.get('note', ''),
            'reviewer': row.get('reviewer', ''),
            'timestamp': row.get('timestamp', ''),
            'source_pdf': row.get('source_pdf', ''),
            'commodity': row.get('commodity', ''),
            'date': row.get('date', ''),
        }
        for field in ['pct_of_parity', 'original_pct']:
            try:
                corrections[key][field] = int(corrections[key][field])
            except (ValueError, TypeError):
                pass
        for field in ['parity_price', 'original_parity_price']:
            try:
                val = corrections[key][field]
                corrections[key][field] = float(val) if val else None
            except (ValueError, TypeError):
                corrections[key][field] = None
    return corrections


def save_corrections_to_gsheet(ws, corrections, reviewer=''):
    """Write corrections to Google Sheet. Updates existing rows, appends new ones."""
    existing = ws.col_values(1)
    key_to_row = {k: i + 1 for i, k in enumerate(existing) if k}

    updates = []
    appends = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for key, c in corrections.items():
        if c.get('status', 'unreviewed') == 'unreviewed':
            continue
        row_data = [
            key,
            c.get('source_pdf', ''),
            c.get('commodity', ''),
            c.get('date', ''),
            c.get('pct_of_parity', ''),
            c.get('original_pct', ''),
            c.get('parity_price', '') if c.get('parity_price') is not None else '',
            c.get('original_parity_price', '') if c.get('original_parity_price') is not None else '',
            c.get('status', ''),
            c.get('note', ''),
            reviewer or c.get('reviewer', ''),
            timestamp,
        ]
        if key in key_to_row:
            row_num = key_to_row[key]
            updates.append((row_num, row_data))
        else:
            appends.append(row_data)

    if updates:
        batch = []
        for row_num, row_data in updates:
            batch.append({
                'range': f'A{row_num}:L{row_num}',
                'values': [row_data],
            })
        ws.batch_update(batch)

    if appends:
        ws.append_rows(appends, value_input_option='RAW')

    return len(updates), len(appends)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_data(_cache_buster=None):
    df = pd.read_csv(EXTRACTED_CSV)
    df['pct_of_parity'] = pd.to_numeric(df['pct_of_parity'], errors='coerce')
    df['source_page'] = pd.to_numeric(df['source_page'], errors='coerce')
    df['parity_price_ocr'] = pd.to_numeric(df['parity_price_ocr'], errors='coerce')
    for col in ['pct_footnote', 'parity_footnote', 'bbox_left', 'bbox_top',
                'bbox_right', 'bbox_bottom', 'bbox_dpi']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def load_corrections():
    """Load corrections from Google Sheet."""
    ws = get_gsheet_connection()
    if ws:
        try:
            return load_corrections_from_gsheet(ws)
        except Exception as e:
            st.sidebar.warning(f"Sheet read failed: {e}")
    return {}


@st.cache_data
def load_column_positions():
    if COLUMN_POSITIONS_FILE.exists():
        with open(COLUMN_POSITIONS_FILE) as f:
            return json.load(f)
    return {}


def save_corrections(corrections):
    """Save corrections to Google Sheet."""
    ws = get_gsheet_connection()
    if ws:
        try:
            reviewer = st.session_state.get('reviewer_name', '')
            n_updated, n_appended = save_corrections_to_gsheet(ws, corrections, reviewer)
            return n_updated, n_appended
        except Exception as e:
            st.sidebar.warning(f"Sheet write failed: {e}")
    return 0, 0


def make_key(row):
    """Unique key for a row: pdf + commodity + date."""
    return f"{row['source_pdf']}|{row['commodity']}|{row['date']}"


# ---------------------------------------------------------------------------
# Image handling (from GCS)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def fetch_image_from_gcs(image_name):
    """Fetch a page image from Google Cloud Storage. Returns PIL Image or None."""
    url = f"{GCS_IMAGE_BASE}/{image_name}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return Image.open(io.BytesIO(resp.content))
    except Exception:
        pass
    return None


def get_page_image(pdf_filename, page_number):
    """Return PIL Image for a page, fetched from GCS."""
    image_name = f"{pdf_filename}_p{int(page_number)}.jpg"
    return fetch_image_from_gcs(image_name)


def get_highlighted_image(page_img, rows_df):
    """Return a PIL Image with bounding boxes drawn around extracted rows."""
    img = page_img.copy()
    draw = ImageDraw.Draw(img)

    colors = {
        'wheat': '#e63946', 'corn': '#457b9d', 'cotton': '#2a9d8f',
        'rice': '#e9c46a', 'tobacco': '#f4a261', 'milk': '#264653',
        'soybeans': '#6a4c93', 'hogs': '#d62828',
    }

    has_bbox = 'bbox_left' in rows_df.columns and rows_df['bbox_left'].notna().any()
    if not has_bbox:
        return img

    for _, row in rows_df.iterrows():
        if pd.isna(row.get('bbox_left')):
            continue
        extraction_dpi = row.get('bbox_dpi', 400)
        scale = 200.0 / extraction_dpi
        left = int(row['bbox_left'] * scale)
        top = int(row['bbox_top'] * scale) - 3
        right = int(row['bbox_right'] * scale)
        bottom = int(row['bbox_bottom'] * scale) + 3
        color = colors.get(row['commodity'], '#ff0000')
        draw.rectangle([left, top, right, bottom], outline=color, width=2)

    return img


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(layout="wide", page_title="Parity Price OCR Review")
    st.title("Parity Price OCR Review")

    csv_mtime = EXTRACTED_CSV.stat().st_mtime if EXTRACTED_CSV.exists() else 0
    df = load_data(_cache_buster=csv_mtime)

    if 'corrections' not in st.session_state:
        st.session_state.corrections = load_corrections()
    corrections = st.session_state.corrections

    # ---- Sidebar ----
    ws = get_gsheet_connection()
    if ws:
        st.sidebar.success("Connected to Google Sheets")
    else:
        st.sidebar.error("Google Sheets not connected")
    st.session_state.reviewer_name = st.sidebar.text_input(
        "Your name (for attribution)",
        value=st.session_state.get('reviewer_name', ''),
    )

    st.sidebar.header("Filters")

    view_mode = st.sidebar.radio(
        "View mode",
        ["By PDF (all commodities per report)", "By commodity (time series)"],
    )

    conf_options = sorted(df['confidence'].unique())
    conf_filter = st.sidebar.multiselect(
        "Confidence", conf_options, default=conf_options
    )

    commodity_options = sorted(df['commodity'].unique())
    commodity_filter = st.sidebar.multiselect(
        "Commodity", commodity_options, default=commodity_options
    )

    report_month_only = st.sidebar.checkbox(
        "Report-month rows only", value=True,
        help="Show only the current-month value from each report (most reliable)"
    )

    review_status = st.sidebar.radio(
        "Show", ["Unreviewed only", "All", "Reviewed only"]
    )

    # ---- Apply filters ----
    mask = df['confidence'].isin(conf_filter) & df['commodity'].isin(commodity_filter)
    if report_month_only:
        mask = mask & (df['is_report_month'] == True)

    reviewed_keys = {k for k, v in corrections.items()
                     if v.get('status') in ('confirmed', 'corrected', 'rejected', 'flagged')}
    if review_status == "Unreviewed only":
        mask = mask & ~df.apply(lambda r: make_key(r) in reviewed_keys, axis=1)
    elif review_status == "Reviewed only":
        mask = mask & df.apply(lambda r: make_key(r) in reviewed_keys, axis=1)

    filtered = df[mask].copy()

    # ---- Stats ----
    total = len(df)
    n_reviewed = len(reviewed_keys)
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Progress:** {n_reviewed} / {total} reviewed")
    st.sidebar.progress(min(n_reviewed / max(total, 1), 1.0))
    st.sidebar.markdown(f"**Showing:** {len(filtered)} rows")

    # ---- Download ----
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Export**")
    corrections_json = json.dumps(corrections, indent=2)
    st.sidebar.download_button(
        "Download corrections JSON",
        data=corrections_json,
        file_name="corrections.json",
        mime="application/json",
    )

    # ---- Summary stats ----
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Review breakdown:**")
    status_counts = {}
    for v in corrections.values():
        s = v.get('status', 'unreviewed')
        status_counts[s] = status_counts.get(s, 0) + 1
    for s in ['confirmed', 'corrected', 'flagged', 'rejected']:
        if s in status_counts:
            st.sidebar.markdown(f"- {s}: {status_counts[s]}")

    # ---- Main content ----
    if len(filtered) == 0:
        st.info("No items match current filters.")
        return

    if view_mode.startswith("By PDF"):
        render_by_pdf(filtered, corrections)
    else:
        render_by_commodity(filtered, corrections)


def render_by_pdf(filtered, corrections):
    """Show one PDF at a time with all its commodity extractions."""
    pdfs = sorted(filtered['source_pdf'].unique())

    col_nav1, col_nav2, col_nav3 = st.columns([1, 3, 1])
    if 'pdf_idx' not in st.session_state:
        st.session_state.pdf_idx = 0
    with col_nav1:
        if st.button("← Previous") and st.session_state.pdf_idx > 0:
            st.session_state.pdf_idx -= 1
            st.rerun()
    with col_nav3:
        if st.button("Next →") and st.session_state.pdf_idx < len(pdfs) - 1:
            st.session_state.pdf_idx += 1
            st.rerun()
    with col_nav2:
        idx = st.selectbox(
            "PDF",
            range(len(pdfs)),
            index=min(st.session_state.pdf_idx, len(pdfs) - 1),
            format_func=lambda i: f"[{i+1}/{len(pdfs)}] {pdfs[i]}",
            label_visibility="collapsed",
        )
        if idx != st.session_state.pdf_idx:
            st.session_state.pdf_idx = idx
            st.rerun()

    current_pdf = pdfs[st.session_state.pdf_idx]
    pdf_data = filtered[filtered['source_pdf'] == current_pdf]
    if 'bbox_top' in pdf_data.columns and pdf_data['bbox_top'].notna().any():
        pdf_data = pdf_data.sort_values('bbox_top')
    else:
        pdf_data = pdf_data.sort_values('commodity')
    page_num = pdf_data['source_page'].iloc[0]

    # Fetch page image from GCS
    page_img = get_page_image(current_pdf, page_num)

    st.caption(f"{len(pdf_data)} commodities extracted")

    all_col_positions = load_column_positions()
    col_positions = all_col_positions.get(current_pdf)

    render_commodity_forms(pdf_data, corrections, prefix="pdf",
                           page_img=page_img, col_positions=col_positions)

    # Bulk save buttons
    st.divider()
    col_bulk1, col_bulk2 = st.columns(2)

    def _apply_pending_and_save(advance=False):
        pending = st.session_state.get('pending_edits', {})
        for k, edit in pending.items():
            corrections[k] = {
                'pct_of_parity': edit['pct_of_parity'],
                'original_pct': edit['original_pct'],
                'parity_price': edit['parity_price'],
                'original_parity_price': edit['original_parity_price'],
                'status': edit['status'],
                'source_pdf': edit['source_pdf'],
                'commodity': edit['commodity'],
                'date': edit['date'],
                'note': edit.get('note', ''),
            }
        save_corrections(corrections)
        st.session_state.pending_edits = {}
        st.session_state.corrections = load_corrections()
        if advance and st.session_state.pdf_idx < len(pdfs) - 1:
            st.session_state.pdf_idx += 1

    with col_bulk1:
        if st.button("Save this page", key="bulk_save"):
            _apply_pending_and_save()
            st.success(f"Saved {len(pdf_data)} values")
            st.rerun()
    with col_bulk2:
        if st.button("Save & advance →", key="bulk_save_next"):
            _apply_pending_and_save(advance=True)
            st.rerun()

    # Full page image at the bottom
    if page_img:
        st.subheader("Full page")
        highlighted = get_highlighted_image(page_img, pdf_data)
        st.image(highlighted, use_container_width=True)


def render_by_commodity(filtered, corrections):
    """Show all months for one commodity, sorted by date."""
    commodities = sorted(filtered['commodity'].unique())
    selected = st.selectbox("Commodity", commodities)

    comm_data = filtered[filtered['commodity'] == selected].sort_values('date')
    st.markdown(f"**{len(comm_data)} observations** for {selected}")

    if len(comm_data) > 1:
        chart_data = comm_data[['date', 'pct_of_parity']].set_index('date')
        st.line_chart(chart_data, height=200)

    render_commodity_forms(comm_data, corrections, prefix="comm")


def render_commodity_forms(data, corrections, prefix="", page_img=None,
                           col_positions=None):
    """Render editable forms for a set of rows."""
    if 'pending_edits' not in st.session_state:
        st.session_state.pending_edits = {}

    for row_idx, (_, row) in enumerate(data.iterrows()):
        key = make_key(row)
        existing = corrections.get(key, {})
        current_status = existing.get('status', 'unreviewed')

        status_icons = {
            'unreviewed': '⬜',
            'confirmed': '✅',
            'corrected': '🔧',
            'rejected': '❌',
            'flagged': '🟡',
        }
        icon = status_icons.get(current_status, '⬜')

        with st.container():
            st.markdown(
                f"**{icon} {row['commodity'].upper()}** — {row['date']} "
                f"| conf: `{row['confidence']}`"
            )

            # Show cropped section of the table with this row highlighted
            if page_img and 'bbox_left' in row.index:
                bbox_t = row.get('bbox_top')
                if pd.notna(bbox_t):
                    try:
                        scale = 200.0 / row['bbox_dpi']
                        row_top = int(bbox_t * scale)
                        row_bottom = int(row['bbox_bottom'] * scale)
                        row_h = row_bottom - row_top
                        ct = max(0, row_top - 250)
                        cb = min(page_img.height, row_bottom + 100)
                        crop = page_img.crop((0, ct, page_img.width, cb))
                        draw = ImageDraw.Draw(crop)
                        crop_h = cb - ct

                        if col_positions:
                            col_dpi = col_positions.get('dpi', 400)
                            col_scale = 200.0 / col_dpi
                            pct_ranges = col_positions.get('pct_x_ranges', [])
                            par_ranges = col_positions.get('parity_x_ranges', [])
                            if pct_ranges and par_ranges:
                                edges = [par_ranges[-1][0], par_ranges[-1][1],
                                         pct_ranges[-1][0], pct_ranges[-1][1]]
                                for x in edges:
                                    cx = int(x * col_scale)
                                    draw.line([(cx, 0), (cx, crop_h)],
                                              fill='#e63946', width=3)

                        hl_top = row_top - ct - 2
                        hl_bottom = row_bottom - ct + 2
                        draw.rectangle(
                            [0, hl_top, crop.width, hl_bottom],
                            outline='#e63946', width=3
                        )
                        st.image(crop)
                    except Exception as e:
                        st.warning(f"Crop error: {e}")

            orig_val = int(row['pct_of_parity']) if pd.notna(row['pct_of_parity']) else 0
            parity_ocr = row.get('parity_price_ocr', None)

            # Top row: [spacer] [Parity $] [% parity]
            c_spacer, c_par, c_pct = st.columns([5, 1, 1])

            with c_par:
                par_fn = row.get('parity_footnote', None)
                month_abbrs = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
                               7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
                try:
                    yr, mo = str(row['date']).split('-')
                    date_label = f"{month_abbrs[int(mo)]}. {yr}"
                except (ValueError, KeyError):
                    date_label = str(row['date'])
                par_label = f"Parity $ ({date_label})"
                if pd.notna(par_fn) if hasattr(pd, 'notna') else par_fn is not None:
                    par_label = f"Parity $ ({date_label}, fn {int(par_fn)}/)"
                par_display = f"{parity_ocr:.2f}" if pd.notna(parity_ocr) else ""
                par_display = existing.get('parity_price', par_display)
                par_str = st.text_input(
                    par_label,
                    value=str(par_display),
                    key=f"{prefix}_par_{key}_{row_idx}",
                )
                try:
                    new_par = float(par_str) if par_str.strip() else None
                except ValueError:
                    new_par = None

            with c_pct:
                display_val = existing.get('pct_of_parity', orig_val)
                pct_fn = row.get('pct_footnote', None)
                fn_label = f"% of parity ({date_label})"
                if pd.notna(pct_fn) if hasattr(pd, 'notna') else pct_fn is not None:
                    fn_label = f"% parity ({date_label}, fn {int(pct_fn)}/)"
                pct_str = st.text_input(
                    fn_label,
                    value=str(int(display_val)),
                    key=f"{prefix}_pct_{key}_{row_idx}",
                )
                try:
                    new_pct = int(pct_str)
                except ValueError:
                    new_pct = orig_val

            # Bottom row: [spacer] [Flag] [Note]
            c_spacer2, c_flag, c_note = st.columns([5, 1, 1])

            with c_flag:
                is_flagged = current_status == 'flagged'
                flagged = st.checkbox(
                    "Flag",
                    value=is_flagged,
                    key=f"{prefix}_flag_{key}_{row_idx}",
                )

            with c_note:
                existing_note = existing.get('note', '')
                has_note = st.checkbox(
                    "Note",
                    value=bool(existing_note),
                    key=f"{prefix}_hasnote_{key}_{row_idx}",
                )
                new_note = ''
                if has_note:
                    new_note = st.text_input(
                        "Note text",
                        value=existing_note,
                        key=f"{prefix}_note_{key}_{row_idx}",
                        label_visibility="collapsed",
                    )

            # Determine status from user actions
            value_changed = (new_pct != orig_val)
            if flagged:
                new_status = 'flagged'
            elif value_changed:
                new_status = 'corrected'
            elif current_status in ('confirmed', 'corrected'):
                new_status = current_status
            else:
                new_status = 'confirmed'

            orig_par = float(parity_ocr) if pd.notna(parity_ocr) else None
            st.session_state.pending_edits[key] = {
                'pct_of_parity': new_pct,
                'original_pct': orig_val,
                'parity_price': new_par,
                'original_parity_price': orig_par,
                'status': new_status,
                'source_pdf': row['source_pdf'],
                'commodity': row['commodity'],
                'date': row['date'],
                'note': new_note,
                'flagged': flagged,
            }

            st.divider()


if __name__ == '__main__':
    main()
