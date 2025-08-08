# Ticket CSV Downloader from .eml Emails

A secure, standalone Python script that:
- Parses `.eml` email files
- Extracts hyperlinks to `.csv` files
- Downloads and aggregates them into a single CSV
- Logs all activity for audit and debugging


## 📦 Features

- Parses `.eml` files and extracts `.csv` hyperlinks
- Downloads files with retry and timeout
- Aggregates all CSVs into one (single header)
- Creates timestamped output folder: `xyz.<name>_<timestamp>/`
- Logs downloads and aggregation
- No external dependencies (uses only Python standard library)
- Configurable via command line

---

## 🧰 Requirements

- Python 3.6 or higher
- No `pip install` required

---

## 📁 Folder Structure

After setup:

project/
├── download_tickets.py ← Save script below
├── emails/ ← Place your .eml file here
│ └── myEmail.eml
└── output/ ← Auto-created (do not add)
└── xyz.MyData_20250405_153000/
├── raw_downloads/
├── logs/
└── combined.csv

After setup:
How to Run
python download_tickets.py --email emails/myEmail.eml --name FinalOutput
