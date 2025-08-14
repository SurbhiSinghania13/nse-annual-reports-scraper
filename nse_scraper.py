#!/usr/bin/env python3

import os
import json
import time
import requests
import logging
import csv
import zipfile
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import re
import io

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# BeautifulSoup for parsing
from bs4 import BeautifulSoup, Tag


class UltimateNSEScraper:
    """
    Ultimate NSE Scraper combining the best of both approaches with enhanced metadata
    """
    
    def __init__(self, base_output_dir: str = "NSE_Annual_Reports"):
        self.base_url = "https://www.nseindia.com"
        self.annual_reports_url = f"{self.base_url}/companies-listing/corporate-filings-annual-reports"
        self.securities_csv_url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(exist_ok=True)
        
        # WORKING SETTINGS from test.py (PRIMARY METHOD)
        self.working_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        # Setup logging
        self.logger = self._setup_logging()
        
        # Setup session for downloads
        self.session = requests.Session()
        self._setup_session()
        
        # PRIMARY METHOD SETTINGS (from test.py)
        self.primary_page_load_timeout = 30
        self.primary_wait_for_table = 20
        self.primary_table_stabilize = 5

        # FALLBACK METHOD SETTINGS
        self.fallback_retry_attempts = 4
        self.fallback_retry_delay = 7
        self.initial_wait = 6
        self.content_wait = 5
        self.extraction_wait = 3
        
        # DOWNLOAD SETTINGS
        self.download_retry_attempts = 3
        self.download_retry_delay = 4
        self.min_pdf_size = 10000
        
        # PROCESSING CONTROL
        self.request_delay = 2.0
        self.companies_cache = None
        
        # FAILURE TRACKING
        self.failed_companies = []
        self.retry_companies = []
        
        # STATISTICS
        self.stats = {
            'companies_processed': 0,
            'companies_successful': 0,
            'reports_found': 0,
            'pdfs_downloaded': 0,
            'download_failures': 0,
            'primary_method_success': 0,
            'fallback_method_success': 0,
            'total_failures': 0,
            'corrupted_zips': 0,
            'misnamed_pdfs': 0,
            'server_errors': 0,
            'errors': []
        }
        
    def _setup_logging(self):
        """Setup comprehensive logging."""
        logger = logging.getLogger('NSE_Annual_Reports_Scraper')
        logger.setLevel(logging.INFO)
        
        if logger.handlers:
            logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # File handler
        log_file = self.base_output_dir / 'enhanced_scraper.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.propagate = False
        
        print(f"📝 Log file: {log_file.absolute()}")
        return logger
    
    def _setup_session(self):
        """Setup HTTP session for downloads."""
        headers = {
            'User-Agent': self.working_user_agent,
            'Accept': 'application/zip, application/pdf, application/octet-stream, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        }
        self.session.headers.update(headers)
    
    def extract_companies_from_csv(self) -> List[Dict[str, str]]:
        """Extract company list from NSE's securities CSV with all available fields."""
        if self.companies_cache:
            self.logger.info(f"Using cached companies: {len(self.companies_cache)} companies")
            return self.companies_cache
        
        companies = []
        
        try:
            self.logger.info("📥 Downloading NSE securities CSV...")
            
            response = self.session.get(self.securities_csv_url, timeout=30)
            response.raise_for_status()
            
            csv_content = response.text
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            # Log available columns for debugging
            fieldnames = csv_reader.fieldnames if csv_reader.fieldnames else []
            self.logger.info(f"📋 Available CSV columns: {fieldnames}")
            
            for row in csv_reader:
                symbol = row.get('SYMBOL', '').strip()
                company_name = row.get('NAME OF COMPANY', '').strip()
                
                if symbol and company_name:
                    # Extract all available fields from CSV with exact column names
                    company_data = {
                        'ticker': symbol,
                        'company_name': company_name
                        
                    }
                    
                    # Map common NSE CSV column variations
                    column_mappings = {
                        'isin_number': [' ISIN NUMBER', 'ISIN_NUMBER', 'ISIN'],
                        'date_of_listing': [' DATE OF LISTING', 'DATE_OF_LISTING', 'LISTING_DATE'],
                        'face_value': [' FACE VALUE', 'FACE_VALUE', 'FACEVALUE'],
                        'series': [' SERIES'],
                        'paid_up_value': [' PAID UP VALUE', 'PAID_UP_VALUE', 'PAIDUP_VALUE'],
                        'market_lot': [' MARKET LOT', 'MARKET_LOT', 'LOT_SIZE']
                    }
                    
                    # Try to find and extract each field
                    for field_name, possible_columns in column_mappings.items():
                        for col_name in possible_columns:
                            if col_name in row and row[col_name].strip():
                                company_data[field_name] = row[col_name].strip()
                                break
                    
                    # Clean up any empty strings to avoid cluttering metadata
                    company_data = {k: v for k, v in company_data.items() if v and v != '-'}
                    companies.append(company_data)
            
            self.logger.info(f"✅ Extracted {len(companies)} companies from CSV with enhanced metadata")
            
            # Show sample company data for verification
            if companies:
                sample = companies[0]
                self.logger.info(f"📋 Sample company data: {sample}")
            
            # Cache and save with all fields
            self.companies_cache = companies
            companies_file = self.base_output_dir / 'companies_list.json'
            with open(companies_file, 'w', encoding='utf-8') as f:
                json.dump(companies, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Saved complete companies list to: {companies_file}")
            
            return companies
            
        except Exception as e:
            self.logger.error(f"❌ Failed to extract companies from CSV: {e}")
            return self._get_fallback_company_list()
    
    def _get_fallback_company_list(self) -> List[Dict[str, str]]:
        """Fallback list of major companies with enhanced metadata."""
        return [
            {
                'ticker': 'TCS', 
                'company_name': 'Tata Consultancy Services Limited', 
                'value': 'TCS',
                'isin_number': 'INE467B01029',
                'date_of_listing': '25-AUG-2004',
                'face_value': '1',
                'series': 'EQ'
            },
            {
                'ticker': 'RELIANCE', 
                'company_name': 'Reliance Industries Limited', 
                'value': 'RELIANCE',
                'isin_number': 'INE002A01018',
                'date_of_listing': '29-NOV-1977',
                'face_value': '10',
                'series': 'EQ'
            },
            {
                'ticker': 'INFY', 
                'company_name': 'Infosys Limited', 
                'value': 'INFY',
                'isin_number': 'INE009A01021',
                'date_of_listing': '08-FEB-1995',
                'face_value': '5',
                'series': 'EQ'
            },
            {
                'ticker': 'HDFCBANK', 
                'company_name': 'HDFC Bank Limited', 
                'value': 'HDFCBANK',
                'isin_number': 'INE040A01034',
                'date_of_listing': '08-NOV-1995',
                'face_value': '1',
                'series': 'EQ'
            }
        ]
    
    def _get_company_data(self, ticker: str) -> Dict:
        """Get company data with enhanced metadata from CSV."""
        if not self.companies_cache:
            self.extract_companies_from_csv()
        
        # Find company in cached data
        for company in self.companies_cache or []:
            if company['ticker'].upper() == ticker.upper():
                return company
        
        # Fallback company data
        return {
            'ticker': ticker,
            'company_name': f'{ticker} Limited',
            'value': ticker,
            'isin_number': '',
            'date_of_listing': '',
            'face_value': '',
            'series': 'EQ'
        }
    
    def process_company_ultimate(self, ticker: str) -> dict:
        """Ultimate company processing with dual-method approach and enhanced metadata."""
        self.logger.info(f"🚀 Processing {ticker} with ULTIMATE dual-method approach...")
        
        # Get company data with enhanced metadata
        company_data = self._get_company_data(ticker)
        
        result = {
            'ticker': ticker,
            'success': False,
            'reports': [],
            'downloads': [],
            'errors': [],
            'method_used': 'none',
            'attempts': [],
            'company_data': company_data
        }
        
        # METHOD 1: PRIMARY 
        self.logger.info(f"🥇 Trying PRIMARY method for {ticker}...")
        primary_result = self._try_primary_method(ticker, company_data)
        result['attempts'].append({'method': 'primary', 'success': primary_result['success']})
        
        if primary_result['success'] and len(primary_result['reports']) > 0:
            self.logger.info(f"✅ PRIMARY method successful for {ticker}: {len(primary_result['reports'])} reports")
            result.update(primary_result)
            result['method_used'] = 'primary'
            self.stats['primary_method_success'] += 1
            
            # Download PDFs using primary method
            if primary_result['reports']:
                downloads = self._download_reports_primary(primary_result['reports'], company_data)
                result['downloads'] = downloads
            
            return result
        
        self.logger.warning(f"⚠️ PRIMARY method failed for {ticker}, trying FALLBACK method...")
        
        # METHOD 2: FALLBACK - Use enhanced logic 
        self.logger.info(f"🥈 Trying FALLBACK method for {ticker}...")
        fallback_result = self._try_fallback_method(ticker, company_data)
        result['attempts'].append({'method': 'fallback', 'success': fallback_result['success']})
        
        if fallback_result['success'] and len(fallback_result['reports']) > 0:
            self.logger.info(f"✅ FALLBACK method successful for {ticker}: {len(fallback_result['reports'])} reports")
            result.update(fallback_result)
            result['method_used'] = 'fallback'
            self.stats['fallback_method_success'] += 1
            
            # Download PDFs using enhanced method
            if fallback_result['reports']:
                downloads = self._download_reports_enhanced(fallback_result['reports'], company_data)
                result['downloads'] = downloads
            
            return result
        
        # BOTH METHODS FAILED - Log for retry
        self.logger.error(f"❌ BOTH methods failed for {ticker} - adding to retry list")
        result['errors'].append("Both primary and fallback methods failed")
        result['method_used'] = 'failed'
        self.stats['total_failures'] += 1
        
        # Add to failed companies for retry
        self.failed_companies.append({
            'ticker': ticker,
            'company_data': company_data,
            'primary_error': primary_result.get('errors', []),
            'fallback_error': fallback_result.get('errors', []),
            'timestamp': datetime.now().isoformat()
        })
        
        return result
    
    def _try_primary_method(self, ticker: str, company_data: Dict) -> dict:
        """Try the PRIMARY method using test.py working logic."""
        result = {
            'ticker': ticker,
            'success': False,
            'reports': [],
            'errors': []
        }
        
        driver = None
        try:
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # working user agent
            chrome_options.add_argument(f'--user-agent={self.working_user_agent}')
            
            # Additional settings to avoid detection
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(self.primary_page_load_timeout)
            
            # Execute script to hide automation indicators
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Load page
            url = f"{self.annual_reports_url}?symbol={ticker}"
            self.logger.debug(f"🌐 Loading with PRIMARY method: {url}")
            driver.get(url)
            
            # Wait for table 
            if self._wait_for_table_primary_method(driver, ticker):
                # Extract reports using  method with enhanced metadata
                reports = self._extract_reports_primary_method(driver, ticker, company_data)
                result['reports'] = reports
                result['success'] = len(reports) > 0
                
                self.logger.debug(f"PRIMARY method found {len(reports)} reports for {ticker}")
                
            else:
                result['errors'].append("Table not found with primary method")
                self.logger.debug(f"PRIMARY method: Table not found for {ticker}")
            
        except Exception as e:
            error_msg = f"PRIMARY method error for {ticker}: {str(e)}"
            result['errors'].append(error_msg)
            self.logger.debug(error_msg)
            
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        
        return result
    
    def _wait_for_table_primary_method(self, driver, ticker: str) -> bool:
        """Wait for table using the proven working method from test.py."""
        wait = WebDriverWait(driver, self.primary_wait_for_table)
        
        try:
            # Wait for the specific table 
            table_element = wait.until(EC.presence_of_element_located((By.ID, "CFannualreportEquityTable")))
            
            # Wait for table to have data
            for attempt in range(10):
                time.sleep(1)
                
                try:
                    tbody = table_element.find_element(By.TAG_NAME, "tbody")
                    data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                    
                    if len(data_rows) > 0:
                        first_row_text = data_rows[0].text.strip()
                        if first_row_text and "loading" not in first_row_text.lower():
                            time.sleep(self.primary_table_stabilize)
                            return True
                        
                except:
                    all_rows = table_element.find_elements(By.TAG_NAME, "tr")
                    data_rows = [row for row in all_rows if not row.find_elements(By.TAG_NAME, "th")]
                    
                    if len(data_rows) > 0:
                        first_row_text = data_rows[0].text.strip()
                        if first_row_text and "loading" not in first_row_text.lower():
                            time.sleep(self.primary_table_stabilize)
                            return True
            
            return False
            
        except TimeoutException:
            return False
    
    def _extract_reports_primary_method(self, driver, ticker: str, company_data: Dict) -> list:
        """Extract reports using the proven method from test.py with enhanced metadata."""
        reports = []
        
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            annual_table = soup.find('table', id='CFannualreportEquityTable')
            
            if not annual_table:
                return reports
            
            # Get data rows
            tbody = annual_table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
            else:
                all_rows = annual_table.find_all('tr')
                rows = [row for row in all_rows if not row.find('th')]
            
            # Extract data using  method with enhanced metadata
            for i, row in enumerate(rows, 1):
                try:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 6:
                        company_name = cells[0].get_text().strip()
                        from_year = cells[1].get_text().strip()
                        to_year = cells[2].get_text().strip()
                        attachment_cell = cells[3]
                        submission_type = cells[4].get_text().strip()
                        broadcast_date = cells[5].get_text().strip()
                        
                        # Extract download link
                        attachment_link = attachment_cell.find('a', href=True)
                        if attachment_link:
                            href = attachment_link.get('href', '')
                            
                            if href.startswith('http'):
                                download_url = href
                            else:
                                download_url = f"{self.base_url}{href}" if href.startswith('/') else f"{self.base_url}/{href}"
                            
                            financial_year = f"{from_year}-{to_year[-2:] if len(to_year) >= 2 else to_year}" if from_year and to_year else "unknown"
                            file_type = "pdf" if download_url.lower().endswith('.pdf') else "zip"
                            
                            # Enhanced metadata structure
                            report = {
                                'date': self._clean_broadcast_date(broadcast_date),
                                'ticker': ticker,
                                'year': financial_year,
                                'url': download_url,
                                'subject': submission_type if submission_type.strip() and submission_type.strip() != '-' else f"Annual Report {financial_year}",
                                'company_name': company_data.get('company_name', company_name),
                                'isin_number': company_data.get('isin_number', ''),
                                'date_of_listing': company_data.get('date_of_listing', '')
                                # Additional fields for internal use
                                
                            }
                            
                            reports.append(report)
                            
                except Exception as e:
                    self.logger.debug(f"Error processing row {i} in primary method: {e}")
            
        except Exception as e:
            self.logger.debug(f"Error in primary extraction: {e}")
        
        return reports
    
    def _try_fallback_method(self, ticker: str, company_data: Dict) -> dict:
        """Try the FALLBACK method using enhanced logic."""
        result = {
            'ticker': ticker,
            'success': False,
            'reports': [],
            'errors': []
        }
        
        try:
            # Use enhanced multi-attempt approach
            soup = self._get_company_page_enhanced_patience(ticker)
            
            if soup:
                # Extract reports using enhanced method with company data
                reports = self._extract_annual_reports_enhanced(soup, company_data)
                result['reports'] = reports
                result['success'] = len(reports) > 0
                
                self.logger.debug(f"FALLBACK method found {len(reports)} reports for {ticker}")
            else:
                result['errors'].append("Could not load page with fallback method")
                
        except Exception as e:
            error_msg = f"FALLBACK method error for {ticker}: {str(e)}"
            result['errors'].append(error_msg)
            self.logger.debug(error_msg)
        
        return result
    
    def _get_company_page_enhanced_patience(self, ticker: str) -> Optional[BeautifulSoup]:
        """Load company page with enhanced patience."""
        for attempt in range(self.fallback_retry_attempts):
            soup = self._load_single_attempt_enhanced(ticker, attempt)
            
            if soup and self._validate_company_page_content(soup, ticker):
                time.sleep(self.extraction_wait)
                return soup
            
            if attempt < self.fallback_retry_attempts - 1:
                time.sleep(self.fallback_retry_delay)
        
        return None
    
    def _load_single_attempt_enhanced(self, ticker: str, attempt_num: int) -> Optional[BeautifulSoup]:
        """Load company page - single attempt with enhanced patience."""
        driver = None
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument(f'--user-agent={self.working_user_agent}')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(30)
            
            url = f"{self.annual_reports_url}?symbol={ticker}"
            driver.get(url)
            
            # Progressive wait time
            wait_time = self.initial_wait + (attempt_num * 3)
            time.sleep(wait_time)
            
            # Enhanced scrolling and waiting
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            additional_wait = 5 + (attempt_num * 2)
            time.sleep(additional_wait)
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            return soup
                
        except Exception as e:
            self.logger.debug(f"Enhanced loading error for {ticker} (attempt {attempt_num + 1}): {e}")
            return None
            
        finally:
            if driver:
                driver.quit()
    
    def _validate_company_page_content(self, soup: BeautifulSoup, ticker: str) -> bool:
        """Validate page content."""
        tables = soup.find_all('table')
        page_text = soup.get_text().lower()
        
        has_tables = len(tables) > 0
        has_content = any(word in page_text for word in ['annual', 'report', 'attachment', 'filing'])
        
        return has_tables and has_content
    
    def _extract_annual_reports_enhanced(self, soup: BeautifulSoup, company_data: Dict) -> List[Dict]:
        """Extract annual reports using enhanced method with company metadata."""
        reports = []
        ticker = company_data['ticker']
        
        # Method 1: Corporate tables
        reports.extend(self._extract_from_corporate_tables_enhanced(soup, company_data))
        
        # Method 2: Any links if no table reports
        if not reports:
            reports.extend(self._extract_from_any_links_enhanced(soup, company_data))
        
        return reports
    
    def _extract_from_corporate_tables_enhanced(self, soup: BeautifulSoup, company_data: Dict) -> List[Dict]:
        """Enhanced table extraction with enhanced metadata."""
        reports = []
        ticker = company_data['ticker']
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
                
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) == 0:
                    continue
                
                # Find links in the row
                pdf_links = []
                for cell in cells:
                    links = cell.find_all('a', href=True)
                    pdf_links.extend(links)
                
                # Process each link
                for link in pdf_links:
                    href = link.get('href', '')
                    link_text = link.get_text().strip()
                    
                    if self._is_valid_document_link(href, link_text):
                        if href.startswith('http'):
                            pdf_url = href
                        else:
                            pdf_url = urljoin(self.base_url, href)
                        
                        row_text = ' '.join([cell.get_text().strip() for cell in cells])
                        combined_text = f"{link_text} {row_text}".lower()
                        
                        if self._is_likely_annual_report(combined_text, pdf_url):
                            financial_year = self._extract_year_from_text(combined_text)
                            
                            # Enhanced metadata structure for fallback method
                            report = {
                                'date': self._extract_date_from_text(row_text),
                                'ticker': ticker,
                                'year': financial_year,
                                'url': pdf_url,
                                'subject': link_text if link_text else f"Annual Report {financial_year}",
                                'company_name': company_data.get('company_name', f'{ticker} Limited'),
                                'isin_number': company_data.get('isin_number', ''),
                                'date_of_listing': company_data.get('date_of_listing', ''),
                                'extraction_method': 'enhanced_table'
                            }
                            reports.append(report)
        
        return reports
    
    def _extract_from_any_links_enhanced(self, soup: BeautifulSoup, company_data: Dict) -> List[Dict]:
        """Enhanced link extraction with enhanced metadata."""
        reports = []
        ticker = company_data['ticker']
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            link_text = link.get_text().strip()
            
            if self._is_valid_document_link(href, link_text):
                combined_text = link_text.lower()
                
                if href.startswith('http'):
                    pdf_url = href
                else:
                    pdf_url = urljoin(self.base_url, href)
                
                if self._is_likely_annual_report(combined_text, pdf_url):
                    financial_year = self._extract_year_from_text(combined_text)
                    
                    # Enhanced metadata structure for link extraction
                    report = {
                        'date': "unknown",
                        'ticker': ticker,
                        'year': financial_year,
                        'url': pdf_url,
                        'subject': link_text if link_text else "Annual Report",
                        'company_name': company_data.get('company_name', f'{ticker} Limited'),
                        'isin_number': company_data.get('isin_number', ''),
                        'date_of_listing': company_data.get('date_of_listing', ''),
                        'extraction_method': 'enhanced_links'
                    }
                    reports.append(report)
        
        return reports
    
    def _extract_date_from_text(self, text: str) -> str:
        """Extract date from text with various formats."""
        if not text:
            return "unknown"
        
        # Common date patterns that match NSE format
        date_patterns = [
            r'(\d{1,2}[-/]\w{3}[-/]\d{4}\s+\d{1,2}:\d{2}:\d{2})',  # 25-JUL-2024 18:30:15
            r'(\d{1,2}[-/]\w{3}[-/]\d{4})',  # 25-JUL-2024
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{4})',  # 25/07/2024
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',  # 2024/07/25
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                found_date = match.group(1)
                # Format it properly
                return self._format_date_for_metadata(found_date)
        
        return "unknown"
    
    def _download_reports_primary(self, reports: List[Dict], company_data: Dict) -> List[Dict]:
        """Download reports using primary method with enhanced metadata."""
        downloads = []
        
        for i, report in enumerate(reports, 1):
            try:
                ticker = report['ticker']
                year = report['year']
                url = report['url']
                
                # Create folder structure
                folder_path = self.base_output_dir / ticker / year
                folder_path.mkdir(parents=True, exist_ok=True)
                
                pdf_path = folder_path / "document.pdf"
                metadata_path = folder_path / "document_meta.json"
                
                # Download file
                download_success = self._download_file_with_handling(url, pdf_path)
                
                download_result = {
                    'ticker': ticker,
                    'year': year,
                    'url': url,
                    'file_path': str(pdf_path),
                    'success': download_success,
                    'file_size': pdf_path.stat().st_size if pdf_path.exists() else 0
                }
                
                if download_success:
                    self.stats['pdfs_downloaded'] += 1
                else:
                    self.stats['download_failures'] += 1
                
                downloads.append(download_result)
                
                # Save enhanced metadata
                metadata = self._create_enhanced_metadata(report, download_success, download_result['file_size'])
                
                self._save_metadata(metadata, metadata_path)
                
                if i < len(reports):
                    time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error downloading {report.get('year', 'unknown')}: {e}")
                downloads.append({
                    'ticker': report.get('ticker', 'unknown'),
                    'year': report.get('year', 'unknown'),
                    'url': report.get('url', ''),
                    'success': False,
                    'error': str(e)
                })
        
        return downloads
    
    def _download_reports_enhanced(self, reports: List[Dict], company_data: Dict) -> List[Dict]:
        """Download reports using enhanced method with enhanced metadata."""
        downloads = []
        
        for i, report in enumerate(reports, 1):
            try:
                ticker = report['ticker']
                year = report['year']
                url = report['url']
                
                folder_path = self.base_output_dir / ticker / year
                folder_path.mkdir(parents=True, exist_ok=True)
                
                pdf_path = folder_path / "document.pdf"
                metadata_path = folder_path / "document_meta.json"
                
                # Enhanced download with corruption detection
                download_success = self._download_file_enhanced_handling(url, pdf_path)
                
                download_result = {
                    'ticker': ticker,
                    'year': year,
                    'url': url,
                    'file_path': str(pdf_path),
                    'success': download_success,
                    'file_size': pdf_path.stat().st_size if pdf_path.exists() else 0
                }
                
                if download_success:
                    self.stats['pdfs_downloaded'] += 1
                else:
                    self.stats['download_failures'] += 1
                
                downloads.append(download_result)
                
                # Save enhanced metadata
                metadata = self._create_enhanced_metadata(report, download_success, download_result['file_size'])
                
                self._save_metadata(metadata, metadata_path)
                
                if i < len(reports):
                    time.sleep(1)
                
            except Exception as e:
                downloads.append({
                    'ticker': report.get('ticker', 'unknown'),
                    'year': report.get('year', 'unknown'),
                    'url': report.get('url', ''),
                    'success': False,
                    'error': str(e)
                })
        
        return downloads
    
    def _create_enhanced_metadata(self, report: Dict, download_success: bool, file_size: int) -> Dict:
        """Create enhanced metadata structure matching the required format."""
        
        # Get the date from report, clean it up
        report_date = report.get('date', 'unknown')
        if report_date and report_date != 'unknown':
            # Clean up the date format
            report_date = self._format_date_for_metadata(report_date)
        
        # Create the exact metadata structure you specified
        metadata = {
            'date': report_date,
            'ticker': report.get('ticker', ''),
            'year': report.get('year', 'unknown'),
            'url': report.get('url', ''),
            'subject': report.get('subject', 'Annual Report'),
            'company_name': report.get('company_name', ''),
            'isin_number': report.get('isin_number', ''),
            'date_of_listing': report.get('date_of_listing', '')
        }
        
        # Only remove completely empty fields, keep fields with meaningful defaults
        clean_metadata = {}
        for key, value in metadata.items():
            if key in ['date', 'ticker', 'year', 'url', 'subject', 'company_name']:
                # Always include these core fields
                clean_metadata[key] = value
            elif key in ['isin_number', 'date_of_listing']:
                # Include these even if empty, but show empty string instead of removing
                clean_metadata[key] = value if value else ''
            elif value:  # Only include other fields if they have actual values
                clean_metadata[key] = value
        
        return clean_metadata
    
    def _format_date_for_metadata(self, raw_date: str) -> str:
        """Format date to match the required format like '25-JUL-2024 18:30:15'."""
        if not raw_date or raw_date in ['unknown', '', '-']:
            return 'unknown'
        
        # If it already looks like the right format, keep it
        if re.match(r'\d{1,2}-[A-Z]{3}-\d{4}', raw_date):
            return raw_date
        
        # Try to parse and format different date patterns
        try:
            # Handle various input formats
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', raw_date):  # DD/MM/YYYY
                parts = raw_date.split('/')
                if len(parts) == 3:
                    day, month, year = parts
                    month_name = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                                  'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'][int(month)]
                    return f"{int(day):02d}-{month_name}-{year}"
            
            elif re.match(r'\d{4}-\d{1,2}-\d{1,2}', raw_date):  # YYYY-MM-DD
                parts = raw_date.split('-')
                if len(parts) == 3:
                    year, month, day = parts
                    month_name = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                                  'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'][int(month)]
                    return f"{int(day):02d}-{month_name}-{year}"
        
        except (ValueError, IndexError):
            pass
        
        # If we can't parse it, return as-is
        return raw_date
    
    def _download_file_with_handling(self, url: str, file_path: Path) -> bool:
        """Download file with retry logic (primary method)."""
        if file_path.exists():
            existing_size = file_path.stat().st_size
            if existing_size >= self.min_pdf_size:
                return True
            else:
                file_path.unlink()
        
        for attempt in range(self.download_retry_attempts):
            try:
                if attempt > 0:
                    time.sleep(self.download_retry_delay + (attempt * 2))
                else:
                    time.sleep(self.request_delay)
                
                response = self.session.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                content_type = response.headers.get('content-type', '').lower()
                if 'zip' in content_type or url.lower().endswith('.zip'):
                    return self._handle_zip_download(response, file_path)
                else:
                    return self._handle_direct_download(response, file_path)
                
            except Exception as e:
                if attempt == self.download_retry_attempts - 1:
                    self.logger.debug(f"Download failed after {self.download_retry_attempts} attempts: {e}")
                    return False
        
        return False
    
    def _download_file_enhanced_handling(self, url: str, file_path: Path) -> bool:
        """Download file with enhanced corruption detection."""
        if file_path.exists():
            existing_size = file_path.stat().st_size
            if existing_size >= self.min_pdf_size:
                return True
            else:
                file_path.unlink()
        
        file_path.parent.mkdir(parents=True, exist_ok=True)
        time.sleep(2)  # Wait before download
        
        for attempt in range(self.download_retry_attempts):
            try:
                if attempt > 0:
                    delay = self.download_retry_delay + (attempt * 2)
                    time.sleep(delay)
                else:
                    time.sleep(self.request_delay)
                
                headers = {
                    'Accept': 'application/zip, application/pdf, application/octet-stream, */*',
                    'User-Agent': self.working_user_agent
                }
                
                response = self.session.get(url, stream=True, timeout=35, headers=headers)
                response.raise_for_status()
                
                content_type = response.headers.get('content-type', '').lower()
                
                if 'zip' in content_type or url.lower().endswith('.zip'):
                    success = self._extract_pdf_from_zip_enhanced(response, file_path, attempt)
                else:
                    success = self._save_direct_pdf_enhanced(response, file_path, attempt)
                
                if success and file_path.exists() and file_path.stat().st_size >= self.min_pdf_size:
                    return True
                else:
                    if file_path.exists():
                        file_path.unlink()
                
            except Exception as e:
                if attempt == self.download_retry_attempts - 1:
                    self.logger.debug(f"Enhanced download failed after {self.download_retry_attempts} attempts: {e}")
                    return False
        
        return False
    
    def _handle_zip_download(self, response, file_path: Path) -> bool:
        """Handle ZIP file download and extraction (primary method)."""
        temp_zip_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_zip.write(chunk)
                temp_zip_path = temp_zip.name
            
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
                
                if not pdf_files:
                    return False
                
                pdf_file = pdf_files[0]
                with zip_ref.open(pdf_file) as pdf_data:
                    pdf_content = pdf_data.read()
                    
                    if len(pdf_content) < self.min_pdf_size:
                        return False
                    
                    with open(file_path, 'wb') as output_file:
                        output_file.write(pdf_content)
                
                return file_path.exists() and file_path.stat().st_size >= self.min_pdf_size
                
        except Exception:
            return False
        finally:
            if temp_zip_path and os.path.exists(temp_zip_path):
                try:
                    os.unlink(temp_zip_path)
                except:
                    pass
    
    def _handle_direct_download(self, response, file_path: Path) -> bool:
        """Handle direct file download (primary method)."""
        try:
            content = b''
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
            
            if len(content) < self.min_pdf_size:
                return False
            
            with open(file_path, 'wb') as f:
                f.write(content)
            
            return file_path.exists() and file_path.stat().st_size >= self.min_pdf_size
            
        except Exception:
            return False
    
    def _extract_pdf_from_zip_enhanced(self, response, file_path: Path, attempt: int) -> bool:
        """Enhanced ZIP extraction with corruption detection."""
        temp_zip_path = None
        try:
            # Download ZIP to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                total_size = 0
                content_sample = b''
                
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        temp_zip.write(chunk)
                        total_size += len(chunk)
                        
                        if len(content_sample) < 1024:
                            content_sample += chunk[:1024 - len(content_sample)]
                
                temp_zip_path = temp_zip.name
            
            # Validate ZIP file size
            if total_size < 1000:
                self.logger.debug(f"ZIP too small ({total_size} bytes) on attempt {attempt + 1}")
                return False
            
            # Check if it's actually a PDF misnamed as ZIP
            if not content_sample.startswith(b'PK'):
                if content_sample.startswith(b'%PDF'):
                    self.logger.debug(f"Misnamed PDF detected, saving directly")
                    self.stats['misnamed_pdfs'] += 1
                    return self._save_misnamed_pdf_enhanced(temp_zip_path, file_path)
                elif b'<html' in content_sample.lower():
                    self.logger.debug(f"Server returned HTML error page")
                    self.stats['server_errors'] += 1
                    return False
                else:
                    self.logger.debug(f"Corrupted ZIP detected")
                    self.stats['corrupted_zips'] += 1
                    return False
            
            # Validate ZIP integrity
            try:
                with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                    bad_file = zip_ref.testzip()
                    if bad_file:
                        self.stats['corrupted_zips'] += 1
                        return False
                    
                    file_list = zip_ref.namelist()
                    pdf_files = [f for f in file_list if f.lower().endswith('.pdf')]
                    
                    if not pdf_files:
                        return False
                    
                    # Extract first PDF
                    pdf_file = pdf_files[0]
                    with zip_ref.open(pdf_file) as pdf_data:
                        pdf_content = pdf_data.read()
                        
                        if len(pdf_content) < self.min_pdf_size:
                            return False
                        
                        if not pdf_content.startswith(b'%PDF'):
                            return False
                        
                        with open(file_path, 'wb') as output_file:
                            output_file.write(pdf_content)
                    
                    return file_path.exists() and file_path.stat().st_size >= self.min_pdf_size
                        
            except zipfile.BadZipFile:
                self.stats['corrupted_zips'] += 1
                return False
            
        except Exception:
            return False
        finally:
            if temp_zip_path and os.path.exists(temp_zip_path):
                try:
                    os.unlink(temp_zip_path)
                except:
                    pass
    
    def _save_misnamed_pdf_enhanced(self, source_path: str, target_path: Path) -> bool:
        """Save misnamed PDF file."""
        try:
            with open(source_path, 'rb') as source_file:
                content = source_file.read()
                
            if len(content) >= self.min_pdf_size and content.startswith(b'%PDF'):
                with open(target_path, 'wb') as target_file:
                    target_file.write(content)
                return True
            
            return False
        except Exception:
            return False
    
    def _save_direct_pdf_enhanced(self, response, file_path: Path, attempt: int) -> bool:
        """Save PDF directly with enhanced validation."""
        try:
            content = b''
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
            
            if len(content) < self.min_pdf_size:
                return False
            
            # Validate content
            if content.startswith(b'%PDF') or len(content) > 50000:
                with open(file_path, 'wb') as f:
                    f.write(content)
                
                return file_path.exists() and file_path.stat().st_size >= self.min_pdf_size
            
            return False
        except Exception:
            return False
    
    # Helper methods from both scripts
    def _clean_broadcast_date(self, raw_date: str) -> str:
        """Clean broadcast date from table content."""
        if not raw_date:
            return ""
        
        parts = raw_date.split('Exchange')
        if parts:
            main_date = parts[0].strip()
            cleaned = ' '.join(main_date.split())
            return cleaned if cleaned != '-' else ""
        
        return raw_date.strip()
    
    def _is_valid_document_link(self, href: str, link_text: str) -> bool:
        """Check if a link could be a valid document."""
        if not href:
            return False
        
        href_lower = href.lower()
        text_lower = link_text.lower()
        
        if 'rss' in href_lower or 'xml' in href_lower:
            return False
        
        document_indicators = [
            '.pdf', '.zip', '.doc', '.docx',
            'download', 'attachment', 'document',
            'nsearchives.nseindia.com'
        ]
        
        return any(indicator in href_lower or indicator in text_lower for indicator in document_indicators)
    
    def _is_likely_annual_report(self, text: str, url: str = "") -> bool:
        """Enhanced annual report detection."""
        text_lower = text.lower()
        url_lower = url.lower()
        
        strong_indicators = ['annual report', 'yearly report', 'audited annual results']
        medium_indicators = ['annual', 'yearly', 'year ended', 'financial year', 'fy']
        url_indicators = ['annual_report', 'annual-report', 'nsearchives.nseindia.com']
        
        # Strong indicators
        if any(indicator in text_lower for indicator in strong_indicators):
            return True
        
        # URL indicators
        if any(indicator in url_lower for indicator in url_indicators):
            return True
        
        # Year range pattern
        if re.search(r'\b(20\d{2})\s*[-–]\s*(20\d{2}|\d{2})\b', text):
            return True
        
        # Medium + document combination
        has_medium = any(indicator in text_lower for indicator in medium_indicators)
        is_document = any(ext in url_lower for ext in ['.pdf', '.zip'])
        
        return has_medium and is_document
    
    def _extract_year_from_text(self, text: str) -> str:
        """Extract financial year from text."""
        patterns = [
            r'(\d{4})-(\d{2,4})',
            r'FY\s*(\d{4})-?(\d{2,4})?',
            r'year\s+ended?\s+.*?(\d{4})',
            r'for\s+the\s+year\s+(\d{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) >= 2 and groups[1]:
                    year1, year2 = groups[0], groups[1]
                    if len(year2) == 2:
                        year2 = year1[:2] + year2
                    return f"{year1}-{year2[-2:]}"
                elif len(groups) >= 1:
                    year = int(groups[0])
                    return f"{year-1}-{str(year)[-2:]}"
        
        return "unknown"
    
    def _save_metadata(self, metadata: Dict, file_path: Path) -> bool:
        """Save metadata to JSON file."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            self.logger.debug(f"Error saving metadata: {e}")
            return False
    
    def process_all_companies(self, max_companies: int = None, start_from: int = 0) -> Dict:
        """Process all companies with ultimate dual-method approach."""
        start_time = datetime.now()
        
        self.logger.info("🚀 Starting ULTIMATE NSE Scraper - Processing ALL companies")
        self.logger.info("🥇 PRIMARY: Proven working method from test.py")
        self.logger.info("🥈 FALLBACK: Enhanced multi-attempt method from nse.py")
        self.logger.info("📝 FAILURE TRACKING: Creates retry log for failed companies")
        
        try:
            # Extract companies
            companies = self.extract_companies_from_csv()
            
            if not companies:
                self.logger.error("No companies found")
                return {'error': 'No companies found'}
            
            # Apply limits and starting point
            if start_from > 0:
                companies = companies[start_from:]
                self.logger.info(f"Starting from company {start_from + 1}")
            
            if max_companies:
                companies = companies[:max_companies]
                self.logger.info(f"Processing limited set: {len(companies)} companies")
            else:
                self.logger.info(f"Processing ALL {len(companies)} companies")
            
            # Process each company
            results = {}
            for i, company_data in enumerate(companies, 1):
                ticker = company_data['ticker']
                actual_index = start_from + i
                
                self.logger.info(f"📊 Processing {i}/{len(companies)} (#{actual_index}): {ticker}")
                
                # Process with ultimate method
                result = self.process_company_ultimate(ticker)
                results[ticker] = result
                
                # Update statistics
                self.stats['companies_processed'] += 1
                if result['success']:
                    self.stats['companies_successful'] += 1
                    self.stats['reports_found'] += len(result['reports'])
                
                # Progress update
                if result['success']:
                    method = result['method_used']
                    reports_count = len(result['reports'])
                    downloads_count = len([d for d in result['downloads'] if d.get('success', False)])
                    self.logger.info(f"  ✅ Success with {method.upper()} method: {reports_count} reports, {downloads_count} downloads")
                else:
                    self.logger.warning(f"  ❌ Failed with both methods: {result.get('errors', [])}")
                
                # Progress summary every 10 companies
                if i % 10 == 0:
                    self._log_progress_summary(i, len(companies))
                
                # Polite delay
                if i < len(companies):
                    time.sleep(self.request_delay)
            
            # Final processing
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Save failure log
            self._save_failure_log()
            
            # Create summary
            summary = {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'companies_discovered': len(companies),
                'companies_processed': self.stats['companies_processed'],
                'companies_successful': self.stats['companies_successful'],
                'reports_found': self.stats['reports_found'],
                'pdfs_downloaded': self.stats['pdfs_downloaded'],
                'download_failures': self.stats['download_failures'],
                'primary_method_success': self.stats['primary_method_success'],
                'fallback_method_success': self.stats['fallback_method_success'],
                'total_failures': self.stats['total_failures'],
                'corrupted_zips': self.stats['corrupted_zips'],
                'misnamed_pdfs': self.stats['misnamed_pdfs'],
                'server_errors': self.stats['server_errors'],
                'results': results,
                'failed_companies_count': len(self.failed_companies)
            }
            
            self.logger.info("🎉 ULTIMATE scraping completed!")
            return summary
            
        except Exception as e:
            self.logger.error(f"Critical error in ultimate processing: {e}")
            return {'error': str(e)}
    
    def _log_progress_summary(self, current: int, total: int):
        """Log progress summary."""
        success_rate = (self.stats['companies_successful'] / self.stats['companies_processed']) * 100 if self.stats['companies_processed'] > 0 else 0
        
        self.logger.info(f"📈 Progress Summary ({current}/{total}):")
        self.logger.info(f"  ✅ Success rate: {success_rate:.1f}% ({self.stats['companies_successful']}/{self.stats['companies_processed']})")
        self.logger.info(f"  🥇 Primary method: {self.stats['primary_method_success']} successes")
        self.logger.info(f"  🥈 Fallback method: {self.stats['fallback_method_success']} successes")
        self.logger.info(f"  📄 Reports found: {self.stats['reports_found']}")
        self.logger.info(f"  💾 PDFs downloaded: {self.stats['pdfs_downloaded']}")
        self.logger.info(f"  ❌ Total failures: {self.stats['total_failures']}")
    
    def _save_failure_log(self):
        """Save failed companies to retry log."""
        if not self.failed_companies:
            self.logger.info("🎉 No companies failed - perfect run!")
            return
        
        retry_log_path = self.base_output_dir / 'failed_companies_retry.json'
        
        retry_data = {
            'created_at': datetime.now().isoformat(),
            'total_failed': len(self.failed_companies),
            'failed_companies': self.failed_companies,
            'retry_instructions': {
                'command': 'python Optimized_nse.py --retry-failed',
                'description': 'Run this command to retry only the failed companies'
            }
        }
        
        try:
            with open(retry_log_path, 'w', encoding='utf-8') as f:
                json.dump(retry_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"📝 Saved {len(self.failed_companies)} failed companies to: {retry_log_path}")
            self.logger.info(f"🔄 To retry failed companies, run: python Optimized_nse.py --retry-failed")
            
        except Exception as e:
            self.logger.error(f"Error saving failure log: {e}")
    
    def retry_failed_companies(self) -> Dict:
        """Retry companies that failed in previous run."""
        retry_log_path = self.base_output_dir / 'failed_companies_retry.json'
        
        if not retry_log_path.exists():
            self.logger.error("No failed companies log found")
            return {'error': 'No failed companies log found'}
        
        try:
            with open(retry_log_path, 'r', encoding='utf-8') as f:
                retry_data = json.load(f)
            
            failed_companies = retry_data.get('failed_companies', [])
            
            if not failed_companies:
                self.logger.info("No failed companies to retry")
                return {'message': 'No failed companies to retry'}
            
            self.logger.info(f"🔄 Retrying {len(failed_companies)} failed companies...")
            
            # Reset stats for retry
            self.stats = {
                'companies_processed': 0,
                'companies_successful': 0,
                'reports_found': 0,
                'pdfs_downloaded': 0,
                'download_failures': 0,
                'primary_method_success': 0,
                'fallback_method_success': 0,
                'total_failures': 0,
                'corrupted_zips': 0,
                'misnamed_pdfs': 0,
                'server_errors': 0,
                'errors': []
            }
            self.failed_companies = []
            
            # Process failed companies
            results = {}
            for i, failed_company in enumerate(failed_companies, 1):
                ticker = failed_company['ticker']
                company_data = failed_company.get('company_data', {})
                
                self.logger.info(f"🔄 Retrying {i}/{len(failed_companies)}: {ticker}")
                
                result = self.process_company_ultimate(ticker)
                results[ticker] = result
                
                # Update statistics
                self.stats['companies_processed'] += 1
                if result['success']:
                    self.stats['companies_successful'] += 1
                    self.stats['reports_found'] += len(result['reports'])
                
                if result['success']:
                    method = result['method_used']
                    self.logger.info(f"  ✅ Retry successful with {method.upper()} method!")
                else:
                    self.logger.warning(f"  ❌ Retry still failed")
                
                time.sleep(self.request_delay)
            
            # Save new failure log if there are still failures
            if self.failed_companies:
                self._save_failure_log()
            else:
                # Archive the old failure log
                archive_path = self.base_output_dir / f'failed_companies_retry_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                retry_log_path.rename(archive_path)
                self.logger.info(f"🎉 All retries successful! Archived old log to: {archive_path}")
            
            return {
                'retried_companies': len(failed_companies),
                'newly_successful': self.stats['companies_successful'],
                'still_failed': len(self.failed_companies),
                'results': results
            }
            
        except Exception as e:
            self.logger.error(f"Error during retry: {e}")
            return {'error': str(e)}
    
    def print_ultimate_summary(self, results: Dict):
        """Print comprehensive summary of ultimate scraping."""
        print(f"\n{'='*80}")
        print("🚀 ULTIMATE NSE SCRAPER - FINAL SUMMARY")
        print(f"{'='*80}")
        
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
            return
        
        # Basic statistics
        print(f"📊 PROCESSING STATISTICS:")
        print(f"  🏢 Companies discovered: {results.get('companies_discovered', 0)}")
        print(f"  ✅ Companies processed: {results.get('companies_processed', 0)}")
        print(f"  🎯 Companies successful: {results.get('companies_successful', 0)}")
        print(f"  📄 Reports found: {results.get('reports_found', 0)}")
        print(f"  💾 PDFs downloaded: {results.get('pdfs_downloaded', 0)}")
        print(f"  ❌ Download failures: {results.get('download_failures', 0)}")
        
        # Method effectiveness
        print(f"\n🎯 METHOD EFFECTIVENESS:")
        print(f"  🥇 PRIMARY method successes: {results.get('primary_method_success', 0)}")
        print(f"  🥈 FALLBACK method successes: {results.get('fallback_method_success', 0)}")
        print(f"  ❌ Total failures (both methods): {results.get('total_failures', 0)}")
        
        # Calculate success rates
        total_processed = results.get('companies_processed', 0)
        if total_processed > 0:
            overall_success_rate = (results.get('companies_successful', 0) / total_processed) * 100
            primary_rate = (results.get('primary_method_success', 0) / total_processed) * 100
            fallback_rate = (results.get('fallback_method_success', 0) / total_processed) * 100
            
            print(f"\n📈 SUCCESS RATES:")
            print(f"  🎯 Overall success rate: {overall_success_rate:.1f}%")
            print(f"  🥇 Primary method rate: {primary_rate:.1f}%")
            print(f"  🥈 Fallback method rate: {fallback_rate:.1f}%")
        
        # Corruption analysis
        print(f"\n🔍 CORRUPTION ANALYSIS:")
        print(f"  💥 Corrupted ZIP files: {results.get('corrupted_zips', 0)}")
        print(f"  📄 Misnamed PDFs (fixed): {results.get('misnamed_pdfs', 0)}")
        print(f"  🌐 Server error pages: {results.get('server_errors', 0)}")
        
        # Performance metrics
        duration_seconds = results.get('duration_seconds', 0)
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        
        print(f"\n⏱️ PERFORMANCE:")
        print(f"  🕒 Total time: {hours}h {minutes}m {seconds}s")
        if total_processed > 0:
            avg_time = duration_seconds / total_processed
            print(f"  ⚡ Average time per company: {avg_time:.1f}s")
        
        # Failed companies info
        failed_count = results.get('failed_companies_count', 0)
        if failed_count > 0:
            print(f"\n🔄 RETRY INFORMATION:")
            print(f"  ❌ Companies requiring retry: {failed_count}")
            print(f"  📝 Retry log saved to: failed_companies_retry.json")
            print(f"  🔄 To retry: python Optimized_nse.py --retry-failed")
        else:
            print(f"\n🎉 PERFECT RUN - No companies failed!")
        
        # Output information
        print(f"\n📁 OUTPUT:")
        print(f"  📂 Output directory: {self.base_output_dir}")
        print(f"  📝 Main log: {self.base_output_dir / 'enhanced_scraper.log'}")
        if failed_count > 0:
            print(f"  🔄 Retry log: {self.base_output_dir / 'failed_companies_retry.json'}")
        
        print(f"{'='*80}")


def main():
    """Main function for the ultimate scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ultimate NSE Scraper - Best of Both Worlds')
    parser.add_argument('--output-dir', default='NSE_Annual_Reports',
                       help='Output directory for scraped data')
    parser.add_argument('--max-companies', type=int,
                       help='Maximum number of companies to process')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Start from company index (for resuming)')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry companies that failed in previous run')
    parser.add_argument('--test-ultimate', action='store_true',
                       help='Test ultimate logic with sample companies')
    
    args = parser.parse_args()
    
    try:
        # Create ultimate scraper
        scraper = UltimateNSEScraper(base_output_dir=args.output_dir)
        
        print("🚀 Ultimate NSE Scraper - Best of Both Worlds")
        print("🥇 PRIMARY: Proven working method (test.py)")
        print("🥈 FALLBACK: Enhanced multi-attempt method (nse.py)")
        print("📝 FAILURE TRACKING: Automatic retry log generation")
        print(f"📁 Output directory: {scraper.base_output_dir}")
        print()
        
        if args.test_ultimate:
            # Test mode with sample companies
            print("🧪 Testing ultimate logic with sample companies...")
            test_companies = ['TCS', 'RELIANCE', 'INFY', 'HDFCBANK']
            
            results = {}
            for ticker in test_companies:
                print(f"\n--- Testing {ticker} with Ultimate Method ---")
                result = scraper.process_company_ultimate(ticker)
                results[ticker] = result
                
                if result['success']:
                    method = result['method_used']
                    reports = len(result['reports'])
                    downloads = len([d for d in result['downloads'] if d.get('success', False)])
                    print(f"✅ Success with {method.upper()} method: {reports} reports, {downloads} downloads")
                else:
                    print(f"❌ Failed with both methods")
            
            # Print test summary
            successful = sum(1 for r in results.values() if r['success'])
            print(f"\n🧪 Test Results: {successful}/{len(test_companies)} companies successful")
            
        elif args.retry_failed:
            # Retry failed companies
            print("🔄 Retrying companies that failed in previous run...")
            results = scraper.retry_failed_companies()
            
            if 'error' in results:
                print(f"❌ Error: {results['error']}")
            else:
                print(f"🔄 Retried: {results['retried_companies']} companies")
                print(f"✅ Newly successful: {results['newly_successful']}")
                print(f"❌ Still failed: {results['still_failed']}")
                
                if results['still_failed'] == 0:
                    print("🎉 All companies now successful!")
                
        else:
            # Normal processing mode
            if args.max_companies:
                print(f"🎯 Processing {args.max_companies} companies")
            else:
                print("🌍 Processing ALL companies from NSE")
            
            if args.start_from > 0:
                print(f"▶️ Starting from company #{args.start_from + 1}")
            
            print("\n🚀 Starting ultimate processing...")
            
            # Process companies
            results = scraper.process_all_companies(
                max_companies=args.max_companies,
                start_from=args.start_from
            )
            
            # Print comprehensive summary
            scraper.print_ultimate_summary(results)
            
            # Additional recommendations
            if 'error' not in results:
                total_failed = results.get('total_failures', 0)
                primary_success = results.get('primary_method_success', 0)
                fallback_success = results.get('fallback_method_success', 0)
                
                print(f"\n💡 RECOMMENDATIONS:")
                
                if total_failed == 0:
                    print("  🎉 Perfect run! No companies failed.")
                elif total_failed > 0:
                    print(f"  🔄 {total_failed} companies failed - use --retry-failed to retry them")
                
                if primary_success > fallback_success:
                    print("  🥇 Primary method is highly effective - working logic is solid")
                elif fallback_success > primary_success:
                    print("  🥈 Fallback method caught many failures - enhanced logic is valuable")
                else:
                    print("  ⚖️ Both methods are equally effective - great dual approach")
                
                corruption_issues = results.get('corrupted_zips', 0) + results.get('server_errors', 0)
                if corruption_issues > 10:
                    print("  ⚠️ High server-side corruption detected - some files may be temporarily unavailable")
                
                if results.get('misnamed_pdfs', 0) > 0:
                    print(f"  ✅ Fixed {results['misnamed_pdfs']} misnamed PDF files automatically")
                
                # Performance recommendations
                duration = results.get('duration_seconds', 0)
                companies_processed = results.get('companies_processed', 0)
                if companies_processed > 0:
                    avg_time = duration / companies_processed
                    if avg_time > 30:
                        print("  ⏱️ Consider using --max-companies for smaller batches if processing is slow")
                    elif avg_time < 10:
                        print("  ⚡ Excellent processing speed - system is well optimized")
        
    except KeyboardInterrupt:
        print("\n⏹️ Ultimate scraping interrupted by user")
        
        # Save current progress if possible
        if 'scraper' in locals() and hasattr(scraper, 'failed_companies'):
            try:
                scraper._save_failure_log()
                print("💾 Saved current progress to failure log")
            except:
                pass
                
    except Exception as e:
        print(f"❌ Critical error in ultimate scraper: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()