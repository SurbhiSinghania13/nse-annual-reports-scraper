# NSE Annual Reports Scraper

A Python-based scraper to extract all corporate annual report filings from the NSE (National Stock Exchange) website and organize them in a structured folder format with both PDF documents and associated metadata.

## Features

- Scrapes annual reports for all NSE-listed companies
- Extracts PDF documents and metadata
- Organizes data in structured folder hierarchy
- Handles ZIP file extraction automatically
- Robust error handling and retry logic
- Comprehensive logging

## Requirements

### System Dependencies
- Python 3.7 or higher
- Chrome browser (for Selenium WebDriver)
- ChromeDriver (automatically managed by Selenium)

### Python Dependencies

**requirements.txt:**
```
requests>=2.28.0
beautifulsoup4>=4.11.0
selenium>=4.15.0
pandas>=1.5.0
lxml>=4.9.0
```

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd nse-annual-reports-scraper
   ```

2. **(Recommended) Create a virtual environment:**

   ```bash
   python3 -m venv venv
   ```
3. **Activate the virtual environment:**
   - macOS/Linux:

      ```bash
      source venv/bin/activate
      ``````
   - Windows (Command Prompt):

      ```bash
      venv\Scripts\activate
      ```
   - Windows (PowerShell):

      ```bash
      venv\Scripts\Activate.ps1
      ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify Chrome installation:**
   - Ensure Google Chrome is installed on your system
   - ChromeDriver will be automatically managed by Selenium

## Usage

### Basic Usage

Run the scraper with default settings (processes all companies):

```bash
python nse_scraper.py
```

### Command Line Options

```bash
# Limit number of companies (useful for testing)
python nse_scraper.py --max-companies 10

# Specify custom output directory
python nse_scraper.py --output-dir my_custom_reports

# Combine options
python nse_scraper.py --max-companies 5 --output-dir test_run
```

### Available Arguments

| Argument | Description | Default | Example |
|----------|-------------|---------|---------|
| `--output-dir` | Output directory for scraped data | `nse_annual_reports` | `--output-dir reports_2024` |
| `--max-companies` | Maximum number of companies to process | All companies | `--max-companies 50` |

## Output Structure

The scraper creates the following folder structure:

```
NSE_Annual_Reports/
├── companies_list.json          # Complete list of NSE companies
├── enhanced_scraper.log         # Detailed execution log
├── TCS/                         # Company ticker symbol
│   ├── 2023-24/                # Financial year
│   │   ├── document.pdf        # Annual report PDF
│   │   └── document_meta.json  # Report metadata
│   ├── 2022-23/
│   │   ├── document.pdf
│   │   └── document_meta.json
│   └── 2021-22/
│       ├── document.pdf
│       └── document_meta.json
├── RELIANCE/
│   ├── 2023-24/
│   │   ├── document.pdf
│   │   └── document_meta.json
│   └── 2022-23/
│       ├── document.pdf
│       └── document_meta.json
└── INFY/
    ├── 2023-24/
    │   ├── document.pdf
    │   └── document_meta.json
    └── 2022-23/
        ├── document.pdf
        └── document_meta.json
```

## Example Files

### Example document_meta.json

```json
{
  "date": "25-JUL-2024 18:30:15",
  "ticker": "TCS",
  "year": "2023-24",
  "url": "https://nsearchives.nseindia.com/corporate/TCS_25072024183015_Annual_Report_2023-24.zip",
  "subject": "Annual Report",
  "company_name": "Tata Consultancy Services Limited",
  "isin_number": "INE467B01029",
  "date_of_listing": "25-AUG-2004"
}
```

### Example companies_list.json (excerpt)

```json
[
  {
    "ticker": "20MICRONS",
    "company_name": "20 Microns Limited",
    "isin_number": "INE144J01027",
    "date_of_listing": "06-OCT-2008",
    "face_value": "5",
    "series": "EQ",
    "paid_up_value": "5",
    "market_lot": "1"
  },
  {
    "ticker": "21STCENMGM",
    "company_name": "21st Century Management Services Limited",
    "isin_number": "INE253B01015",
    "date_of_listing": "03-MAY-1995",
    "face_value": "10",
    "series": "EQ",
    "paid_up_value": "10",
    "market_lot": "1"
  },
  {
    "ticker": "360ONE",
    "company_name": "360 ONE WAM LIMITED",
    "isin_number": "INE466L01038",
    "date_of_listing": "19-SEP-2019",
    "face_value": "1",
    "series": "EQ",
    "paid_up_value": "1",
    "market_lot": "1"
  }
]
```

### Example Log Output

```
2024-08-13 10:30:15 - INFO - Starting NSE Annual Reports Scraper
2024-08-13 10:30:16 - INFO - Downloading NSE securities CSV...
2024-08-13 10:30:18 - INFO - Successfully extracted 2156 companies from CSV
2024-08-13 10:30:18 - INFO - Processing 1/2156: TCS
2024-08-13 10:30:19 - INFO - Loading page for TCS (attempt 1)
2024-08-13 10:30:25 - INFO - Extracting reports for TCS
2024-08-13 10:30:25 - INFO - Found report: Annual Report for 2023-24
2024-08-13 10:30:25 - INFO - Found report: Annual Report for 2022-23
2024-08-13 10:30:26 - INFO - Downloading https://nsearchives.nseindia.com/corporate/TCS_25072024183015_Annual_Report_2023-24.zip (attempt 1)
2024-08-13 10:30:28 - INFO - Successfully downloaded: nse_annual_reports/TCS/2023-24/document.pdf
```

## Configuration

### Timing Configuration
The scraper includes built-in delays to be respectful to the NSE servers:
- **Request delay**: 1 second between requests
- **Page load timeout**: 20 seconds
- **Retry attempts**: 3 attempts for failed operations

### File Validation
- **Minimum PDF size**: 10KB (to filter out error pages)
- **Content type validation**: Automatic detection of PDF vs ZIP files
- **ZIP extraction**: Automatic extraction of PDFs from ZIP archives

## Troubleshooting

### Common Issues

1. **ChromeDriver Issues**
   ```bash
   # Update Chrome and try again
   # Or install specific ChromeDriver version
   ```

2. **Network Timeouts**
   ```bash
   # Run with fewer companies to test connectivity
   python nse_scraper.py --max-companies 5
   ```

3. **Permission Errors**
   ```bash
   # Ensure output directory is writable
   mkdir nse_annual_reports
   chmod 755 nse_annual_reports
   ```

4. **Memory Issues with Large Datasets**
   ```bash
   # Process in smaller batches
   python nse_scraper.py --max-companies 100
   ```

### Debug Mode

For detailed debugging, check the log file:
```bash
tail -f nse_annual_reports/enhanced_scraper.log
```

### Recovery from Interruptions

The scraper automatically skips already downloaded files, so you can safely restart it:
```bash
# Will skip existing files and continue where it left off
python nse_scraper.py
```

## Performance

### Expected Runtime
- **Small test (10 companies)**: 5-10 minutes
- **Medium batch (100 companies)**: 1-2 hours
- **Full dataset (2000+ companies)**: 8-12 hours

### Resource Usage
- **Memory**: ~200-500 MB
- **Disk space**: Varies by number of reports (typically 1-10 MB per report)
- **Network**: Respectful rate limiting (1 request per second)

## Data Quality

### Metadata Fields
Each `document_meta.json` contains:
- **date**: Filing date (when available) or broadcast date
- **ticker**: Company stock symbol
- **year**: Financial year (e.g., "2023-24")
- **url**: Original download URL
- **subject**: Report title/type
- **company_name**: Full company name
- **isin_number**: International Securities Identification Number
- **date_of_listing**: Company's listing date on NSE

### File Integrity
- All PDFs are validated for minimum size
- ZIP files are properly extracted
- Corrupted downloads are automatically retried
- Failed downloads are logged for manual review

## License

This project is for educational and research purposes. Please respect NSE's terms of service and use responsibly.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
1. Check the log file for detailed error messages
2. Review this README for troubleshooting steps
3. Open an issue in the repository with:
   - Your operating system
   - Python version
   - Error messages from the log
   - Steps to reproduce the issue