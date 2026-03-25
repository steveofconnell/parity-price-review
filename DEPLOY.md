# Parity Price Review App — Deployment Notes

## Components

1. **Streamlit app**: `app.py` — deployed on Streamlit Community Cloud
   - URL: https://parity-price-review-e628ur2yqvnsqhidxspgxd.streamlit.app/
   - Connects to Streamlit Community Cloud via this GitHub repo

2. **GitHub repo**: https://github.com/steveofconnell/parity-price-review
   - Push to `main` triggers automatic redeploy on Streamlit Cloud

3. **Google Sheets backend**: stores review state (confirmations, corrections)
   - Sheet ID: `1KyWnEIeOqZDn394jWQB74bh2cI7A1moMZ3KGEtUjbz4`
   - Service account credentials: `~/Dropbox/Personal/tokens/digitizeparitypriceseries-36b3d2770be6.json`

4. **Google Cloud Storage**: hosts page images (too large for GitHub)
   - Bucket: `gs://parity-price-review-images`
   - Public URL pattern: `https://storage.googleapis.com/parity-price-review-images/{image_name}`
   - Image naming: `{pdf_filename_without_ext}_p{page_number}.jpg` (200 DPI)
   - GCS project: linked to `soconnell.work@gmail.com`

## Data Files (in this repo)

- `extracted_pct_of_parity.csv` — merged ESMIS (7,049 rows) + HathiTrust (845 rows) = 7,894 total
- `extracted_pct_of_parity_column_positions.json` — per-PDF column x-positions for overlay guides

## How to Update

### Update extraction data
1. Re-run OCR extraction scripts (07c for ESMIS, 07d for HathiTrust)
2. Merge CSVs: `head -1 esmis.csv > merged.csv && tail -n +2 esmis.csv >> merged.csv && tail -n +2 hathi.csv >> merged.csv`
3. Copy merged CSV to this repo as `extracted_pct_of_parity.csv`
4. Commit and push to GitHub — Streamlit Cloud auto-redeploys

### Upload new page images
```bash
gcloud auth login  # if not already authenticated
cd /path/to/review_images/
gsutil -m cp NEW_IMAGES*.jpg gs://parity-price-review-images/
```

### Refresh the app
After pushing to GitHub, Streamlit Cloud auto-redeploys within ~1 minute.
To force: go to https://share.streamlit.io/ → Manage app → Reboot.

## History
- 2026-03-14: Initial deployment with ESMIS data (7,049 rows), 404 page images
- 2026-03-25: Added HathiTrust data (845 rows, 78 new page images), reduced overlay opacity
