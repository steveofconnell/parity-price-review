#!/usr/bin/env python3
"""
Streamlit app for reviewing OCR-extracted parity price data.

Shows the scanned parity table page alongside extracted values.
User can confirm, correct, or flag each extraction.
Corrections are saved to a JSON file and can be applied to produce a corrected CSV.

Usage:
    streamlit run review_app.py

Pre-requisite: run generate_page_images.py first to create page images.
If images are missing, the app will generate them on-the-fly (slower).
"""

import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageDraw

SCRIPT_DIR = Path(__file__).parent
EXTRACTED_CSV = SCRIPT_DIR / "extracted_pct_of_parity.csv"
COLUMN_POSITIONS_FILE = SCRIPT_DIR / "extracted_pct_of_parity_column_positions.json"
CORRECTIONS_FILE = SCRIPT_DIR / "corrections.json"
CORRECTED_CSV = SCRIPT_DIR / "extracted_pct_of_parity_corrected.csv"

# Google Cloud Storage base URL for page images
GCS_IMAGE_BASE = "https://storage.googleapis.com/parity-price-review-images"

# Google Sheets config
GSHEET_CREDENTIALS = None  # use st.secrets in cloud
GSHEET_ID = '1KyWnEIeOqZDn394jWQB74bh2cI7A1moMZ3KGEtUjbz4'
GSHEET_HEADERS = ['key', 'source_pdf', 'commodity', 'date',
                  'pct_of_parity', 'original_pct', 'pct_footnote',
                  'parity_price', 'original_parity_price', 'parity_footnote',
                  'status', 'note', 'reviewer', 'timestamp']

# Lock expiry in minutes — if a reviewer hasn't saved within this window,
# the lock is treated as stale and other reviewers can claim the PDF.
LOCK_EXPIRY_MINUTES = 30


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
        if 'gcp_service_account' in st.secrets:
            creds = Credentials.from_service_account_info(
                dict(st.secrets['gcp_service_account']),
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        else:
            return None
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
        # Convert numeric fields
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


def save_pending_to_gsheet(ws, pending_edits, reviewer=''):
    """Write only the pending edits to Google Sheet. Does not touch other rows.

    Before writing, checks each row: if it already exists in the sheet
    and was reviewed by a DIFFERENT reviewer, that row is skipped to
    prevent overwriting another reviewer's work.
    """
    if not pending_edits:
        return 0, 0

    # Get all existing data to check for conflicts
    all_rows = ws.get_all_values()
    key_to_row = {}       # key -> row number (1-indexed)
    key_to_reviewer = {}  # key -> existing reviewer name
    for i, row_vals in enumerate(all_rows):
        if i == 0:
            continue  # skip header
        if row_vals:
            k = row_vals[0]
            key_to_row[k] = i + 1
            # Reviewer is column 13 (index 12)
            if len(row_vals) > 12:
                key_to_reviewer[k] = row_vals[12]

    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    updates = []
    appends = []
    skipped = 0

    for key, c in pending_edits.items():
        if c.get('status', 'unreviewed') == 'unreviewed':
            continue

        # Skip rows already reviewed by someone else
        if key in key_to_reviewer:
            existing_reviewer = key_to_reviewer[key]
            if existing_reviewer and existing_reviewer != reviewer:
                skipped += 1
                continue

        row_data = [
            key,
            c.get('source_pdf', ''),
            c.get('commodity', ''),
            c.get('date', ''),
            c.get('pct_of_parity', ''),
            c.get('original_pct', ''),
            c.get('pct_footnote', ''),
            c.get('parity_price', '') if c.get('parity_price') is not None else '',
            c.get('original_parity_price', '') if c.get('original_parity_price') is not None else '',
            c.get('parity_footnote', ''),
            c.get('status', ''),
            c.get('note', ''),
            reviewer,
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
                'range': f'A{row_num}:N{row_num}',
                'values': [row_data],
            })
        ws.batch_update(batch)

    if appends:
        ws.append_rows(appends, value_input_option='RAW')

    if skipped:
        st.sidebar.info(f"Skipped {skipped} row(s) already reviewed by others")

    return len(updates), len(appends)


# ---------------------------------------------------------------------------
# PDF locking — prevents two reviewers from working on the same page
# ---------------------------------------------------------------------------

def get_locks_worksheet():
    """Return the 'locks' worksheet, creating it if needed."""
    if 'locks_ws' in st.session_state:
        return st.session_state.locks_ws
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        if 'gcp_service_account' not in st.secrets:
            return None
        creds = Credentials.from_service_account_info(
            dict(st.secrets['gcp_service_account']),
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(GSHEET_ID)
        # Try to get existing 'locks' worksheet
        try:
            ws = sheet.worksheet('locks')
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title='locks', rows=1000, cols=3)
            ws.update('A1:C1', [['pdf_name', 'reviewer', 'timestamp']])
        st.session_state.locks_ws = ws
        return ws
    except Exception as e:
        st.sidebar.warning(f"Locks sheet unavailable: {e}")
        return None


def get_locked_pdfs(exclude_reviewer=''):
    """Return a set of PDF names currently locked by OTHER reviewers.

    Locks older than LOCK_EXPIRY_MINUTES are ignored (treated as stale).
    """
    ws = get_locks_worksheet()
    if not ws:
        return set()
    try:
        records = ws.get_all_records()
    except Exception:
        return set()
    now = datetime.now(timezone.utc)
    locked = set()
    for row in records:
        pdf = row.get('pdf_name', '')
        reviewer = row.get('reviewer', '')
        ts_str = row.get('timestamp', '')
        if not pdf or not ts_str:
            continue
        # Skip own locks
        if reviewer == exclude_reviewer:
            continue
        # Check expiry
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_minutes = (now - ts).total_seconds() / 60
            if age_minutes > LOCK_EXPIRY_MINUTES:
                continue  # stale lock
        except (ValueError, TypeError):
            continue
        locked.add(pdf)
    return locked


def acquire_lock(pdf_name, reviewer):
    """Write or refresh a lock for this PDF. Overwrites any existing lock
    for the same PDF by this reviewer."""
    ws = get_locks_worksheet()
    if not ws or not reviewer:
        return
    try:
        now_str = datetime.now(timezone.utc).isoformat()
        # Check if this reviewer already has a lock row for this PDF
        cell_list = ws.findall(pdf_name, in_column=1)
        for cell in cell_list:
            row_vals = ws.row_values(cell.row)
            if len(row_vals) >= 2 and row_vals[1] == reviewer:
                # Update timestamp
                ws.update_cell(cell.row, 3, now_str)
                return
        # No existing lock — append
        ws.append_row([pdf_name, reviewer, now_str], value_input_option='RAW')
    except Exception as e:
        st.sidebar.warning(f"Could not acquire lock: {e}")


def release_lock(pdf_name, reviewer):
    """Remove this reviewer's lock on the given PDF."""
    ws = get_locks_worksheet()
    if not ws or not reviewer:
        return
    try:
        cell_list = ws.findall(pdf_name, in_column=1)
        for cell in cell_list:
            row_vals = ws.row_values(cell.row)
            if len(row_vals) >= 2 and row_vals[1] == reviewer:
                ws.delete_rows(cell.row)
                return
    except Exception:
        pass


def cleanup_stale_locks():
    """Remove all locks older than LOCK_EXPIRY_MINUTES. Called periodically."""
    ws = get_locks_worksheet()
    if not ws:
        return 0
    try:
        records = ws.get_all_records()
    except Exception:
        return 0
    now = datetime.now(timezone.utc)
    rows_to_delete = []
    for i, row in enumerate(records):
        ts_str = row.get('timestamp', '')
        if not ts_str:
            rows_to_delete.append(i + 2)  # +2: 1-indexed + header
            continue
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if (now - ts).total_seconds() / 60 > LOCK_EXPIRY_MINUTES:
                rows_to_delete.append(i + 2)
        except (ValueError, TypeError):
            rows_to_delete.append(i + 2)
    # Delete from bottom up to preserve row indices
    for row_num in sorted(rows_to_delete, reverse=True):
        try:
            ws.delete_rows(row_num)
        except Exception:
            pass
    return len(rows_to_delete)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_data(_cache_buster=None):
    df = pd.read_csv(EXTRACTED_CSV)
    # Ensure types
    df['pct_of_parity'] = pd.to_numeric(df['pct_of_parity'], errors='coerce')
    df['source_page'] = pd.to_numeric(df['source_page'], errors='coerce')
    df['parity_price_ocr'] = pd.to_numeric(df['parity_price_ocr'], errors='coerce')
    # Optional columns (may not exist in older CSV versions)
    for col in ['pct_footnote', 'parity_footnote', 'bbox_left', 'bbox_top',
                'bbox_right', 'bbox_bottom', 'bbox_dpi']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def load_corrections():
    """Load corrections from Google Sheet if available, else local JSON."""
    ws = get_gsheet_connection()
    if ws:
        try:
            return load_corrections_from_gsheet(ws)
        except Exception as e:
            st.sidebar.warning(f"Sheet read failed, using local: {e}")
    if CORRECTIONS_FILE.exists():
        with open(CORRECTIONS_FILE) as f:
            return json.load(f)
    return {}


@st.cache_data
def load_column_positions():
    if COLUMN_POSITIONS_FILE.exists():
        with open(COLUMN_POSITIONS_FILE) as f:
            return json.load(f)
    return {}


def save_corrections(corrections, pending_edits=None):
    """Save pending edits to Google Sheet, then reload all corrections.

    Only the rows in pending_edits are written to the sheet, avoiding
    race conditions between concurrent reviewers. The full corrections
    dict is saved locally as a backup.
    """
    # Always save local backup of full state
    with open(CORRECTIONS_FILE, 'w') as f:
        json.dump(corrections, f, indent=2)

    # Write only pending edits to Google Sheet
    ws = get_gsheet_connection()
    if ws and pending_edits:
        try:
            reviewer = st.session_state.get('reviewer_name', '')
            n_updated, n_appended = save_pending_to_gsheet(
                ws, pending_edits, reviewer)
            return n_updated, n_appended
        except Exception as e:
            st.sidebar.warning(f"Sheet write failed (saved locally): {e}")
    return 0, 0


def make_key(row):
    """Unique key for a row: pdf + commodity + date."""
    return f"{row['source_pdf']}|{row['commodity']}|{row['date']}"


# ---------------------------------------------------------------------------
# Image handling
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

    # Color per commodity for visual distinction
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

        # Scale from extraction DPI (400) to image DPI (200)
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
# Export
# ---------------------------------------------------------------------------

def export_corrected_csv(df, corrections):
    """Apply corrections to the original data and write a corrected CSV."""
    corrected = df.copy()
    applied = 0
    for idx, row in corrected.iterrows():
        key = make_key(row)
        if key in corrections:
            c = corrections[key]
            if c.get('status') in ('confirmed', 'corrected'):
                corrected.at[idx, 'pct_of_parity'] = c['pct_of_parity']
                if c.get('parity_price') is not None:
                    corrected.at[idx, 'parity_price_ocr'] = c['parity_price']
                corrected.at[idx, 'confidence'] = 'reviewed'
                applied += 1
            elif c.get('status') == 'rejected':
                corrected.at[idx, 'confidence'] = 'rejected'
                applied += 1
    corrected.to_csv(CORRECTED_CSV, index=False)
    return applied


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(layout="wide", page_title="Parity Price OCR Review")
    st.title("Parity Price OCR Review")

    # Use CSV modification time as cache key to auto-reload when data changes
    csv_mtime = EXTRACTED_CSV.stat().st_mtime if EXTRACTED_CSV.exists() else 0
    df = load_data(_cache_buster=csv_mtime)

    # Use session state for corrections so edits persist across reruns
    if 'corrections' not in st.session_state:
        st.session_state.corrections = load_corrections()
    corrections = st.session_state.corrections

    # ---- Sidebar ----
    # ---- Reviewer name ----
    ws = get_gsheet_connection()
    if ws:
        st.sidebar.success("Connected to Google Sheets")
    else:
        st.sidebar.info("Using local storage (no Google Sheets)")
    st.session_state.reviewer_name = st.sidebar.text_input(
        "Your name (for attribution)",
        value=st.session_state.get('reviewer_name', ''),
    )
    if not st.session_state.get('reviewer_name'):
        st.sidebar.error("Enter your name above to begin reviewing.")

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
        "Show", ["Next available", "My reviewed"]
    )

    # ---- Apply filters ----
    reviewer_name = st.session_state.get('reviewer_name', '')
    mask = df['confidence'].isin(conf_filter) & df['commodity'].isin(commodity_filter)
    if report_month_only:
        mask = mask & (df['is_report_month'] == True)

    # Build sets for filtering
    reviewed_keys = {k for k, v in corrections.items()
                     if v.get('status') in ('confirmed', 'corrected', 'rejected', 'flagged')}
    my_reviewed_keys = {k for k, v in corrections.items()
                        if v.get('status') in ('confirmed', 'corrected', 'rejected', 'flagged')
                        and v.get('reviewer', '') == reviewer_name} if reviewer_name else set()

    if review_status == "Next available":
        # Exclude anything already reviewed by anyone
        mask = mask & ~df.apply(lambda r: make_key(r) in reviewed_keys, axis=1)
    elif review_status == "My reviewed":
        # Show only rows this reviewer has submitted — no lock filtering
        mask = mask & df.apply(lambda r: make_key(r) in my_reviewed_keys, axis=1)

    filtered = df[mask].copy()

    # ---- Exclude PDFs locked by other reviewers (only in "Next available") ----
    if review_status == "Next available":
        locked_pdfs = get_locked_pdfs(exclude_reviewer=reviewer_name)
        if locked_pdfs:
            pre_lock_count = len(filtered)
            filtered = filtered[~filtered['source_pdf'].isin(locked_pdfs)]
            n_skipped = pre_lock_count - len(filtered)
            if n_skipped > 0:
                st.sidebar.info(
                    f"Skipping {len(locked_pdfs)} PDF(s) locked by other reviewers"
                )

    # ---- Stats ----
    total = len(df)
    n_reviewed = len(reviewed_keys)
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Progress:** {n_reviewed} / {total} reviewed")
    st.sidebar.progress(min(n_reviewed / max(total, 1), 1.0))
    st.sidebar.markdown(f"**Showing:** {len(filtered)} rows")

    # ---- Export ----
    # ---- Save / Export / Download ----
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Save & Export**")

    if st.sidebar.button("Save corrections to disk"):
        save_corrections(corrections)
        st.sidebar.success("Saved!")

    if st.sidebar.button("Export corrected CSV"):
        n = export_corrected_csv(df, corrections)
        st.sidebar.success(f"Exported {n} corrections to\n{CORRECTED_CSV.name}")

    # Download corrections JSON
    corrections_json = json.dumps(corrections, indent=2)
    st.sidebar.download_button(
        "Download corrections JSON",
        data=corrections_json,
        file_name="corrections.json",
        mime="application/json",
    )

    # Download corrected CSV
    if st.sidebar.button("Generate & download corrected CSV"):
        n = export_corrected_csv(df, corrections)
        if CORRECTED_CSV.exists():
            with open(CORRECTED_CSV) as f:
                csv_data = f.read()
            st.sidebar.download_button(
                "Download CSV",
                data=csv_data,
                file_name="extracted_pct_of_parity_corrected.csv",
                mime="text/csv",
                key="dl_csv",
            )

    # Upload corrections (resume previous work)
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Load previous work**")
    uploaded = st.sidebar.file_uploader(
        "Upload corrections JSON",
        type="json",
        help="Upload a previously downloaded corrections.json to resume work"
    )
    if uploaded is not None:
        try:
            uploaded_corrections = json.loads(uploaded.read())
            # Merge: uploaded values override existing
            merged_count = 0
            for k, v in uploaded_corrections.items():
                if k not in corrections or v.get('status') != 'unreviewed':
                    corrections[k] = v
                    merged_count += 1
            save_corrections(corrections)
            st.sidebar.success(f"Loaded {merged_count} corrections")
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Error loading file: {e}")

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

    # ---- Lock status ----
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Active locks:**")
    all_locks = get_locked_pdfs(exclude_reviewer='')  # show all
    if all_locks:
        st.sidebar.markdown(f"{len(all_locks)} PDF(s) currently locked")
        if st.sidebar.button("Clean up stale locks"):
            n = cleanup_stale_locks()
            if n:
                st.sidebar.success(f"Removed {n} stale lock(s)")
                # Clear cached worksheet so fresh data is fetched
                st.session_state.pop('locks_ws', None)
                st.rerun()
            else:
                st.sidebar.info("No stale locks found")
    else:
        st.sidebar.markdown("None")

    # ---- Main content ----
    if not reviewer_name:
        st.info("Please enter your name in the sidebar to begin reviewing.")
        return

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

    # Navigation
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

    # Acquire lock on this PDF so other reviewers skip it
    reviewer_name = st.session_state.get('reviewer_name', '')
    if reviewer_name:
        # Release previous lock if we navigated away
        prev_pdf = st.session_state.get('locked_pdf', None)
        if prev_pdf and prev_pdf != current_pdf:
            release_lock(prev_pdf, reviewer_name)
        acquire_lock(current_pdf, reviewer_name)
        st.session_state.locked_pdf = current_pdf

    pdf_data = filtered[filtered['source_pdf'] == current_pdf]
    # Sort by vertical position on page (order they appear in the PDF)
    if 'bbox_top' in pdf_data.columns and pdf_data['bbox_top'].notna().any():
        pdf_data = pdf_data.sort_values('bbox_top')
    else:
        pdf_data = pdf_data.sort_values('commodity')
    page_num = pdf_data['source_page'].iloc[0]

    # Fetch page image from GCS
    page_img = get_page_image(current_pdf, page_num)

    st.caption(f"{len(pdf_data)} commodities extracted")

    # Load column positions for this PDF
    all_col_positions = load_column_positions()
    col_positions = all_col_positions.get(current_pdf)

    # Each commodity: crop at full width, then form below it
    render_commodity_forms(pdf_data, corrections, prefix="pdf",
                           page_img=page_img, col_positions=col_positions)

    # Save button
    st.divider()
    reviewer_name = st.session_state.get('reviewer_name', '')
    if not reviewer_name:
        st.warning("Enter your name in the sidebar before submitting.")
        st.button("Save & next →", key="bulk_save_next", type="primary",
                  disabled=True)
    elif st.button("Save & next →", key="bulk_save_next", type="primary"):
        pending = st.session_state.get('pending_edits', {})
        # Build correction entries from pending edits
        edits_to_save = {}
        for k, edit in pending.items():
            entry = {
                'pct_of_parity': edit['pct_of_parity'],
                'original_pct': edit['original_pct'],
                'pct_footnote': edit.get('pct_footnote', ''),
                'parity_price': edit['parity_price'],
                'original_parity_price': edit['original_parity_price'],
                'parity_footnote': edit.get('parity_footnote', ''),
                'status': edit['status'],
                'source_pdf': edit['source_pdf'],
                'commodity': edit['commodity'],
                'date': edit['date'],
                'note': edit.get('note', ''),
            }
            corrections[k] = entry
            edits_to_save[k] = entry
        # Write only these edits to the sheet (not the full dict)
        save_corrections(corrections, pending_edits=edits_to_save)
        # Release lock on completed PDF
        if reviewer_name:
            release_lock(current_pdf, reviewer_name)
            st.session_state.locked_pdf = None
        st.session_state.pending_edits = {}
        # Reload from sheet to pick up other reviewers' work
        st.session_state.corrections = load_corrections()
        if st.session_state.pdf_idx < len(pdfs) - 1:
            st.session_state.pdf_idx += 1
        st.rerun()

    # Full page image at the bottom for reference
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

    # Quick time-series view
    if len(comm_data) > 1:
        chart_data = comm_data[['date', 'pct_of_parity']].set_index('date')
        st.line_chart(chart_data, height=200)

    # Editable table
    render_commodity_forms(comm_data, corrections, prefix="comm")


def render_commodity_forms(data, corrections, prefix="", page_img=None,
                           col_positions=None):
    """Render editable forms for a set of rows.

    Layout per commodity (optimized for keyboard tabbing):
        [Flag checkbox] [Note checkbox → text if checked] [Parity $] [% of parity]

    No individual save buttons — all edits collected and saved via
    bulk buttons at the bottom of the page.
    """
    # Collect edits in session state so bulk save can access them
    if 'pending_edits' not in st.session_state:
        st.session_state.pending_edits = {}

    for row_idx, (_, row) in enumerate(data.iterrows()):
        key = make_key(row)
        existing = corrections.get(key, {})
        current_status = existing.get('status', 'unreviewed')

        # Status indicator
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
            c_spacer, c_par, c_pct = st.columns([33, 7, 7])

            # Format date as "Jan. 1971" etc.
            month_abbrs = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
                           7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
            try:
                yr, mo = str(row['date']).split('-')
                date_label = f"{month_abbrs[int(mo)]}. {yr}"
            except (ValueError, KeyError):
                date_label = str(row['date'])

            par_fn = row.get('parity_footnote', None)
            pct_fn = row.get('pct_footnote', None)

            with c_par:
                par_display = f"{parity_ocr:.2f}" if pd.notna(parity_ocr) else ""
                par_display = existing.get('parity_price', par_display)
                par_str = st.text_input(
                    f"Parity $ ({date_label})",
                    value=str(par_display),
                    key=f"{prefix}_par_{key}_{row_idx}",
                )
                if pd.notna(par_fn):
                    st.caption(f"footnote: {int(par_fn)}/")
                try:
                    new_par = float(par_str) if par_str.strip() else None
                except ValueError:
                    new_par = None

            with c_pct:
                display_val = existing.get('pct_of_parity', orig_val)
                pct_str = st.text_input(
                    f"% of parity ({date_label})",
                    value=str(int(display_val)),
                    key=f"{prefix}_pct_{key}_{row_idx}",
                )
                if pd.notna(pct_fn):
                    st.caption(f"footnote: {int(pct_fn)}/")
                try:
                    new_pct = int(pct_str)
                except ValueError:
                    new_pct = orig_val

            # Bottom row: [spacer] [Flag] [Note]
            c_spacer2, c_flag, c_note = st.columns([33, 7, 7])

            with c_flag:
                is_flagged = current_status == 'flagged'
                flagged = st.checkbox(
                    "Flag — unsure / needs further review",
                    value=is_flagged,
                    key=f"{prefix}_flag_{key}_{row_idx}",
                )

            with c_note:
                existing_note = existing.get('note', '')
                has_note = st.checkbox(
                    "Note (if corrected: describe OCR error)",
                    value=bool(existing_note),
                    key=f"{prefix}_hasnote_{key}_{row_idx}",
                )
                new_note = ''
                if has_note:
                    new_note = st.text_input(
                        "e.g., 'true . detected as 3'",
                        value=existing_note,
                        key=f"{prefix}_note_{key}_{row_idx}",
                    )

            # Determine status from user actions
            value_changed = (new_pct != orig_val)
            if flagged:
                new_status = 'flagged'
            elif value_changed:
                new_status = 'corrected'
            elif current_status in ('confirmed', 'corrected'):
                new_status = current_status  # preserve existing review
            else:
                new_status = 'confirmed'  # will be set on bulk save

            # Store pending edit (used by bulk save)
            orig_par = float(parity_ocr) if pd.notna(parity_ocr) else None
            st.session_state.pending_edits[key] = {
                'pct_of_parity': new_pct,
                'original_pct': orig_val,
                'pct_footnote': int(pct_fn) if pd.notna(pct_fn) else '',
                'parity_price': new_par,
                'original_parity_price': orig_par,
                'parity_footnote': int(par_fn) if pd.notna(par_fn) else '',
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
