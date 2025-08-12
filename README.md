# NSE Corporate Annual Reports Scraper

📄 A Python-based web scraper to extract corporate annual report filings from the [NSE India website](https://www.nseindia.com/companies-listing/corporate-filings-annual-reports), organize them by company ticker and financial year, and store both the PDF documents and their metadata.

---

## 🚀 Features

- Scrapes annual report filings for **all listed companies** on NSE
- Downloads both **PDF documents** and associated **metadata**
- Organizes output in a structured folder hierarchy:  
  `TICKER/FINANCIAL_YEAR/document.pdf`  
  `TICKER/FINANCIAL_YEAR/document_meta.json`
- Metadata includes:
  - Filing date
  - Ticker symbol
  - Financial year
  - Document URL
  - Any additional relevant details
- Robust error handling with retries and missing document detection

---

## 🧰 Requirements

- Python 3.8+
- Required libraries (install via `pip install -r requirements.txt`):
  - `requests`
  - `beautifulsoup4`
  - `pandas`
  - `selenium`

---

## 📦 Folder Structure

Example output:
```
NSE_Annual_Reports/
├── INFY/
│   ├── 2022-2023/
│   │   ├── document.pdf
│   │   └── document_meta.json
│   └── 2021-2022/
│       ├── document.pdf
│       └── document_meta.json
├── RELIANCE/
│   └── 2022-2023/
│       ├── document.pdf
│       └── document_meta.json
```

---

## 🛠️ How to Run

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/nse-annual-reports-scraper.git
   cd nse-annual-reports-scraper

2. **Install dependencies**

    ```bash
    pip install -r requirements.txt
3. **Run the scraper**

    ```bash
    python nse.py
3. **Output**

    All downloaded reports and metadata will be saved in the NSE_Annual_Reports/ directory.