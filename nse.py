#!/usr/bin/env python3
"""
Enhanced NSE Scraper with Corruption Detection and Graceful Error Handling
Version: Enhanced Super Patient v2.0

Key Features:
1. 4 attempts for company page loading with progressive delays
2. Enhanced ZIP corruption detection and handling
3. Graceful handling of server-side corrupted files
4. Detailed logging of corruption issues
5. Saves metadata even for failed downloads
6. Better content type detection
7. Misnamed PDF file handling (PDFs with .zip extension)

Author: AI Assistant
Date: August 2025
Version: Enhanced Super Patient v2.0
"""

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

# Core scraping imports
import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd

# Selenium for JavaScript-heavy pages
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class EnhancedNSEScraper:
    """
    Enhanced NSE Scraper with corruption detection and graceful error handling
    """
    
    def __init__(self, base_output_dir: str = "nse_annual_reports_enhanced"):
        """Initialize the Enhanced NSE Scraper."""
        self.base_url = "https://www.nseindia.com"
        self.annual_reports_url = f"{self.base_url}/companies-listing/corporate-filings-annual-reports"
        self.securities_csv_url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        self.base_output_dir = Path(base_output_dir)
        
        # Create directories
        self.base_output_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Setup session
        self.session = requests.Session()
        self._setup_session()
        
        # ENHANCED SETTINGS
        self.request_delay = 0.8
        self.selenium_timeout = 25
        
        # WAIT TIMES
        self.initial_wait = 6        # Initial wait after page load
        self.content_wait = 5        # Wait for content to render
        self.extraction_wait = 3     # Wait before starting extraction
        self.download_prep_wait = 2  # Wait before downloading
        
        # RETRY ATTEMPTS
        self.company_retry_attempts = 4   # Company page load attempts
        self.company_retry_delay = 7      # Delay between company retries
        self.download_retry_attempts = 4  # Download attempts
        self.download_retry_delay = 3     # Delay between download retries
        
        # Validation settings
        self.min_pdf_size = 10000  # 10KB minimum
        
        # Cache for companies
        self.companies_cache = None
        
        # Corruption tracking
        self.corruption_stats = {
            'corrupted_zips': 0,
            'misnamed_pdfs': 0,
            'server_errors': 0,
            'total_zip_attempts': 0,
            'successful_extractions': 0
        }
        
    def _setup_logging(self):
        """Setup logging configuration."""
        self.logger = logging.getLogger('Enhanced_NSE_Scraper')
        self.logger.setLevel(logging.INFO)
        
        if self.logger.handlers:
            self.logger.handlers.clear()
        
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
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def _setup_session(self):
        """Setup HTTP session with proper headers."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0'
        }
        self.session.headers.update(headers)
        
        try:
            response = self.session.get(self.base_url, timeout=10)
            if response.status_code == 200:
                self.logger.info("Session initialized successfully")
        except Exception as e:
            self.logger.warning(f"Session initialization warning: {e}")
    
    def extract_companies_from_csv(self) -> List[Dict[str, str]]:
        """Extract company list from NSE's securities CSV file."""
        if self.companies_cache:
            self.logger.info(f"Using cached companies: {len(self.companies_cache)} companies")
            return self.companies_cache
        
        companies = []
        
        try:
            self.logger.info("Downloading NSE securities CSV...")
            
            # Download CSV with timeout
            response = self.session.get(self.securities_csv_url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV content
            csv_content = response.text
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            # Extract companies
            for row in csv_reader:
                symbol = row.get('SYMBOL', '').strip()
                company_name = row.get('NAME OF COMPANY', '').strip()
                
                if symbol and company_name:
                    companies.append({
                        'ticker': symbol,
                        'company_name': company_name,
                        'value': symbol
                    })
            
            self.logger.info(f"Successfully extracted {len(companies)} companies from CSV")
            
            # Cache the results
            self.companies_cache = companies
            
            # Save companies list for reference
            companies_file = self.base_output_dir / 'companies_list.json'
            with open(companies_file, 'w', encoding='utf-8') as f:
                json.dump(companies, f, indent=2, ensure_ascii=False)
            
            return companies
            
        except Exception as e:
            self.logger.error(f"Failed to extract companies from CSV: {e}")
            self.logger.info("Falling back to predefined company list")
            return self._get_fallback_company_list()
    
    def _get_fallback_company_list(self) -> List[Dict[str, str]]:
        """Fallback list of major NSE companies."""
        return [
            {'ticker': 'TCS', 'company_name': 'Tata Consultancy Services Limited', 'value': 'TCS'},
            {'ticker': 'RELIANCE', 'company_name': 'Reliance Industries Limited', 'value': 'RELIANCE'},
            {'ticker': '20MICRONS', 'company_name': '20 Microns Limited', 'value': '20MICRONS'},
            {'ticker': '360ONE', 'company_name': '360 ONE WAM Limited', 'value': '360ONE'},
            {'ticker': '3IINFOLTD', 'company_name': '3i Infotech Limited', 'value': '3IINFOLTD'},
            {'ticker': '3MINDIA', 'company_name': '3M India Limited', 'value': '3MINDIA'},
            {'ticker': 'AARTIIND', 'company_name': 'Aarti Industries Limited', 'value': 'AARTIIND'},
            {'ticker': 'AARTIDRUGS', 'company_name': 'Aarti Drugs Limited', 'value': 'AARTIDRUGS'}
        ]
    
    def get_company_page_with_enhanced_patience(self, ticker: str) -> Optional[BeautifulSoup]:
        """Load company's annual reports page with enhanced patience and report validation."""
        
        for attempt in range(self.company_retry_attempts):
            self.logger.info(f"🔄 Loading page for {ticker} (attempt {attempt + 1}/{self.company_retry_attempts})")
            
            soup = self._load_single_attempt_with_patience(ticker, attempt)
            
            if soup:
                # Validate that we got meaningful content
                if self._validate_company_page_content(soup, ticker):
                    self.logger.info(f"✅ Successfully loaded valid content for {ticker} on attempt {attempt + 1}")
                    
                    # EXTRA WAIT before extraction to let content fully render
                    self.logger.info(f"⏳ Waiting {self.extraction_wait}s before extraction...")
                    time.sleep(self.extraction_wait)
                    
                    # ENHANCED: Quick test extraction to see if we can find reports
                    test_reports = self._quick_test_extraction(soup, ticker)
                    
                    if len(test_reports) > 0:
                        self.logger.info(f"✅ Confirmed {len(test_reports)} reports found for {ticker} on attempt {attempt + 1}")
                        return soup
                    else:
                        self.logger.warning(f"⚠️ Page loaded but 0 reports extracted for {ticker} on attempt {attempt + 1}")
                        
                        # If this isn't the last attempt, try again
                        if attempt < self.company_retry_attempts - 1:
                            self.logger.info(f"🔄 Retrying {ticker} due to 0 reports - attempt {attempt + 2} in {self.company_retry_delay} seconds...")
                            time.sleep(self.company_retry_delay)
                            continue
                        else:
                            # Last attempt - return what we have and log the issue
                            self.logger.warning(f"📄 Returning page for {ticker} despite 0 reports (final attempt)")
                            return soup
                else:
                    self.logger.warning(f"⚠️ Page loaded but no annual reports content for {ticker} on attempt {attempt + 1}")
                    
                    # If this isn't the last attempt, wait and try again
                    if attempt < self.company_retry_attempts - 1:
                        self.logger.info(f"🔄 Retrying {ticker} in {self.company_retry_delay} seconds...")
                        time.sleep(self.company_retry_delay)
                        continue
                    else:
                        # Last attempt - return what we have
                        self.logger.info(f"📄 Returning page content for {ticker} despite validation issues (final attempt)")
                        return soup
            else:
                self.logger.error(f"❌ Failed to load page for {ticker} on attempt {attempt + 1}")
                
                # If this isn't the last attempt, wait and try again
                if attempt < self.company_retry_attempts - 1:
                    self.logger.info(f"🔄 Retrying {ticker} in {self.company_retry_delay} seconds...")
                    time.sleep(self.company_retry_delay)
                    continue
        
        # All attempts failed
        self.logger.error(f"❌ Failed to load page for {ticker} after {self.company_retry_attempts} attempts")
        return None
    
    def _quick_test_extraction(self, soup: BeautifulSoup, ticker: str) -> List[Dict]:
        """Quick test to see if we can extract reports from the current page."""
        try:
            # Use the same extraction logic but don't log details
            company = {'ticker': ticker, 'company_name': f'{ticker} Limited'}
            
            # Method 1: Quick table check
            reports = []
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Look for any links in table rows
                for row in rows[1:3]:  # Check first few rows only
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        links = cell.find_all('a', href=True)
                        for link in links:
                            href = link.get('href', '')
                            if self._is_valid_document_link(href, link.get_text()):
                                # Found at least one valid document link
                                reports.append({'url': href, 'ticker': ticker})
                                if len(reports) >= 3:  # Found enough for testing
                                    return reports
            
            # Method 2: Quick link check if no table results
            if not reports:
                all_links = soup.find_all('a', href=True)
                for link in all_links[:20]:  # Check first 20 links only
                    href = link.get('href', '')
                    link_text = link.get_text().strip()
                    if self._is_valid_document_link(href, link_text):
                        combined_text = link_text.lower()
                        if any(word in combined_text for word in ['annual', 'report', 'attachment']):
                            reports.append({'url': href, 'ticker': ticker})
                            if len(reports) >= 3:
                                return reports
            
            return reports
            
        except Exception as e:
            self.logger.debug(f"Quick test extraction error for {ticker}: {e}")
            return []
    
    def _load_single_attempt_with_patience(self, ticker: str, attempt_num: int) -> Optional[BeautifulSoup]:
        """Load company page - single attempt with maximum patience and enhanced waiting."""
        driver = None
        try:
            # Optimized Chrome options
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-breakpad')
            chrome_options.add_argument('--disable-component-extensions-with-background-pages')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--mute-audio')
            chrome_options.add_argument('--no-zygote')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(self.selenium_timeout)
            
            # Construct URL
            url = f"{self.annual_reports_url}?symbol={ticker}"
            
            driver.get(url)
            
            # Progressive wait time (gets longer with each retry)
            wait_time = self.initial_wait + (attempt_num * 3)  # 6, 9, 12, 15 seconds
            self.logger.debug(f"Initial wait: {wait_time}s for attempt {attempt_num + 1}")
            time.sleep(wait_time)
            
            # Enhanced waiting strategy for content detection
            wait = WebDriverWait(driver, self.selenium_timeout)
            
            content_loaded = False
            
            # Strategy 1: Wait for any table with data
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
                self.logger.debug(f"Table detected for {ticker}")
                content_loaded = True
            except TimeoutException:
                pass
            
            # Strategy 2: Wait for any corporate filing content
            if not content_loaded:
                try:
                    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'ATTACHMENT') or contains(text(), 'Annual') or contains(text(), 'Report')]")))
                    self.logger.debug(f"Corporate content detected for {ticker}")
                    content_loaded = True
                except TimeoutException:
                    pass
            
            # Strategy 3: Wait for any links
            if not content_loaded:
                try:
                    wait.until(EC.presence_of_element_located((By.XPATH, "//a[@href]")))
                    self.logger.debug(f"Links detected for {ticker}")
                    content_loaded = True
                except TimeoutException:
                    pass
            
            # Progressive content wait
            content_wait = self.content_wait + (attempt_num * 2)  # 5, 7, 9, 11 seconds
            self.logger.debug(f"Content wait: {content_wait}s for attempt {attempt_num + 1}")
            time.sleep(content_wait)
            
            # ENHANCED: Multiple scroll actions to trigger lazy loading
            try:
                # Scroll to bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)  # Increased wait after scroll
                
                # Scroll to top
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                # Scroll to middle
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(2)
                
                # Final scroll to bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                
                # Back to top for extraction
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(2)
                
                # ENHANCED: Try to trigger any AJAX calls by waiting for network idle
                # Additional wait for dynamic content
                additional_wait = 5 + (attempt_num * 2)  # 5, 7, 9, 11 seconds
                self.logger.debug(f"Additional dynamic content wait: {additional_wait}s")
                time.sleep(additional_wait)
                
            except Exception as scroll_error:
                self.logger.debug(f"Scroll error for {ticker}: {scroll_error}")
                pass
            
            # Get page source and parse
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            return soup
                
        except Exception as e:
            self.logger.error(f"Error loading page for {ticker} (attempt {attempt_num + 1}): {e}")
            return None
            
        finally:
            if driver:
                driver.quit()
    
    def _validate_company_page_content(self, soup: BeautifulSoup, ticker: str) -> bool:
        """Validate that the page has meaningful corporate filings content."""
        tables = soup.find_all('table')
        rows = soup.find_all('tr')
        links = soup.find_all('a', href=True)
        pdf_links = [link for link in links if '.pdf' in link.get('href', '').lower()]
        
        # Look for text mentioning annual reports or attachments
        page_text = soup.get_text().lower()
        has_annual = 'annual' in page_text
        has_report = 'report' in page_text
        has_attachment = 'attachment' in page_text
        has_filing = 'filing' in page_text
        
        self.logger.debug(f"Content validation for {ticker}:")
        self.logger.debug(f"  Tables: {len(tables)}, Rows: {len(rows)}, Links: {len(links)}, PDF links: {len(pdf_links)}")
        self.logger.debug(f"  Keywords - Annual: {has_annual}, Report: {has_report}, Attachment: {has_attachment}, Filing: {has_filing}")
        
        # Validation criteria
        has_meaningful_tables = len(tables) > 0 and len(rows) > 3
        has_content_indicators = has_annual or has_report or has_attachment or has_filing
        has_strong_indicators = has_attachment and (has_filing or has_annual)
        
        is_valid = (
            (has_meaningful_tables and has_content_indicators) or
            (len(pdf_links) > 0 and has_content_indicators) or
            has_strong_indicators or
            len(tables) > 2
        )
        
        self.logger.debug(f"  Validation result: {is_valid}")
        return is_valid
    
    def extract_annual_reports(self, soup: BeautifulSoup, company: Dict[str, str]) -> List[Dict]:
        """Extract annual reports from company page with enhanced debugging."""
        reports = []
        ticker = company['ticker']
        
        self.logger.info(f"📊 Extracting reports for {ticker}")
        
        # ENHANCED DEBUG: Log page structure
        self._debug_page_structure(soup, ticker)
        
        # Method 1: Look for tables with corporate filings structure
        reports.extend(self._extract_from_corporate_tables(soup, company))
        
        # Method 2: Look for any links that could be annual reports
        if not reports:
            self.logger.info(f"🔍 No table reports found for {ticker}, trying link extraction...")
            reports.extend(self._extract_from_any_links(soup, company))
        
        # Method 3: Look for specific patterns in text
        if not reports:
            self.logger.info(f"🔍 No link reports found for {ticker}, trying text pattern extraction...")
            reports.extend(self._extract_from_text_patterns(soup, company))
        
        # ENHANCED: If still no reports, do deeper analysis
        if not reports:
            self.logger.warning(f"🚨 No reports found for {ticker} - performing deep analysis...")
            self._deep_analysis_for_missing_reports(soup, ticker)
        
        self.logger.info(f"📋 Found {len(reports)} potential reports for {ticker}")
        return reports
    
    def _debug_page_structure(self, soup: BeautifulSoup, ticker: str) -> None:
        """Debug the page structure to understand why reports might not be found."""
        try:
            # Count basic elements
            tables = soup.find_all('table')
            rows = soup.find_all('tr')
            links = soup.find_all('a', href=True)
            
            # Look for NSE-specific content
            page_text = soup.get_text().lower()
            has_attachment = 'attachment' in page_text
            has_annual = 'annual' in page_text
            has_report = 'report' in page_text
            has_filing = 'filing' in page_text
            
            # Check for specific NSE indicators
            nse_indicators = ['nsearchives', 'annual_reports', 'corporate filings']
            has_nse_indicators = any(indicator in page_text for indicator in nse_indicators)
            
            self.logger.debug(f"Page structure for {ticker}:")
            self.logger.debug(f"  Tables: {len(tables)}, Rows: {len(rows)}, Links: {len(links)}")
            self.logger.debug(f"  Content indicators - Attachment: {has_attachment}, Annual: {has_annual}, Report: {has_report}, Filing: {has_filing}")
            self.logger.debug(f"  NSE indicators: {has_nse_indicators}")
            
            # Log table structures
            for i, table in enumerate(tables[:3]):  # First 3 tables only
                table_rows = table.find_all('tr')
                if table_rows:
                    headers = []
                    header_row = table_rows[0]
                    header_cells = header_row.find_all(['th', 'td'])
                    headers = [cell.get_text().strip() for cell in header_cells]
                    self.logger.debug(f"  Table {i+1} headers: {headers[:5]}")  # First 5 headers
                    
                    # Check if table has links
                    table_links = table.find_all('a', href=True)
                    self.logger.debug(f"  Table {i+1} links: {len(table_links)}")
            
        except Exception as e:
            self.logger.debug(f"Debug analysis error for {ticker}: {e}")
    
    def _deep_analysis_for_missing_reports(self, soup: BeautifulSoup, ticker: str) -> None:
        """Perform deep analysis when no reports are found."""
        try:
            # Look for any PDF or ZIP links anywhere on the page
            all_links = soup.find_all('a', href=True)
            document_links = []
            
            for link in all_links:
                href = link.get('href', '').lower()
                text = link.get_text().strip()
                
                if any(ext in href for ext in ['.pdf', '.zip', 'download', 'attachment']):
                    document_links.append({
                        'href': href,
                        'text': text,
                        'parent_text': link.parent.get_text()[:100] if link.parent else ''
                    })
            
            self.logger.debug(f"Found {len(document_links)} potential document links for {ticker}")
            
            if document_links:
                for i, doc_link in enumerate(document_links[:5]):  # Show first 5
                    self.logger.debug(f"  Doc link {i+1}: {doc_link['text']} -> {doc_link['href']}")
            
            # Look for form elements or AJAX indicators
            forms = soup.find_all('form')
            scripts = soup.find_all('script')
            
            self.logger.debug(f"Page has {len(forms)} forms and {len(scripts)} scripts")
            
            # Check for dynamic content indicators
            page_source = str(soup)
            dynamic_indicators = ['ajax', 'json', 'api', 'xhr', 'fetch']
            has_dynamic = any(indicator in page_source.lower() for indicator in dynamic_indicators)
            
            if has_dynamic:
                self.logger.debug(f"Page appears to have dynamic content - may need additional wait time")
            
            # Look for error messages
            error_indicators = ['no data', 'not found', 'error', 'unavailable']
            page_text_lower = soup.get_text().lower()
            errors_found = [indicator for indicator in error_indicators if indicator in page_text_lower]
            
            if errors_found:
                self.logger.warning(f"Possible error indicators found for {ticker}: {errors_found}")
            
        except Exception as e:
            self.logger.debug(f"Deep analysis error for {ticker}: {e}")
    
    def _extract_from_corporate_tables(self, soup: BeautifulSoup, company: Dict[str, str]) -> List[Dict]:
        """Extract from standard corporate filings tables."""
        reports = []
        ticker = company['ticker']
        
        tables = soup.find_all('table')
        
        for table_idx, table in enumerate(tables):
            self.logger.debug(f"Analyzing table {table_idx + 1} for {ticker}")
            
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
                
            # Get headers
            header_row = rows[0]
            headers = []
            
            header_cells = header_row.find_all('th')
            if not header_cells:
                header_cells = header_row.find_all('td')
            
            headers = [th.get_text().strip().upper() for th in header_cells]
            self.logger.debug(f"Table headers: {headers}")
            
            # Find column indices
            attachment_col_idx = -1
            from_year_col_idx = -1
            to_year_col_idx = -1
            submission_type_col_idx = -1
            date_col_idx = -1
            
            for i, header in enumerate(headers):
                header = header.upper()
                if 'ATTACHMENT' in header or 'DOCUMENT' in header or 'FILE' in header:
                    attachment_col_idx = i
                elif 'FROM' in header and ('YEAR' in header or 'DATE' in header):
                    from_year_col_idx = i
                elif 'TO' in header and ('YEAR' in header or 'DATE' in header):
                    to_year_col_idx = i
                elif 'TYPE' in header or 'CATEGORY' in header:
                    submission_type_col_idx = i
                elif 'DATE' in header and 'FILING' in header:
                    date_col_idx = i
            
            # If no clear attachment column, look for any column with links
            if attachment_col_idx == -1:
                for row in rows[1:3]:
                    cells = row.find_all(['td', 'th'])
                    for i, cell in enumerate(cells):
                        links = cell.find_all('a', href=True)
                        if links:
                            attachment_col_idx = i
                            self.logger.debug(f"Found links in column {i}")
                            break
                    if attachment_col_idx != -1:
                        break
            
            # Process data rows
            for row_idx, row in enumerate(rows[1:], 1):
                cells = row.find_all(['td', 'th'])
                
                if len(cells) == 0:
                    continue
                
                try:
                    # Extract data
                    from_year = ""
                    to_year = ""
                    submission_type = ""
                    filing_date = ""
                    
                    if from_year_col_idx >= 0 and from_year_col_idx < len(cells):
                        from_year = cells[from_year_col_idx].get_text().strip()
                    if to_year_col_idx >= 0 and to_year_col_idx < len(cells):
                        to_year = cells[to_year_col_idx].get_text().strip()
                    if submission_type_col_idx >= 0 and submission_type_col_idx < len(cells):
                        submission_type = cells[submission_type_col_idx].get_text().strip()
                    if date_col_idx >= 0 and date_col_idx < len(cells):
                        filing_date = cells[date_col_idx].get_text().strip()
                    
                    # Find links in the row
                    pdf_links = []
                    
                    if attachment_col_idx >= 0 and attachment_col_idx < len(cells):
                        attachment_cell = cells[attachment_col_idx]
                        links = attachment_cell.find_all('a', href=True)
                        pdf_links.extend(links)
                    else:
                        for cell in cells:
                            links = cell.find_all('a', href=True)
                            pdf_links.extend(links)
                    
                    # Process each link
                    for link in pdf_links:
                        href = link.get('href', '')
                        link_text = link.get_text().strip()
                        
                        # Check if it's a potentially valid document link
                        if self._is_valid_document_link(href, link_text):
                            # Make URL absolute
                            if href.startswith('http'):
                                pdf_url = href
                            else:
                                pdf_url = urljoin(self.base_url, href)
                            
                            # Extract all text for analysis
                            row_text = ' '.join([cell.get_text().strip() for cell in cells])
                            combined_text = f"{submission_type} {link_text} {from_year} {to_year} {row_text}".lower()
                            
                            # Enhanced annual report detection
                            if self._is_likely_annual_report(combined_text, pdf_url, from_year, to_year):
                                # Create financial year
                                financial_year = self._create_financial_year(from_year, to_year, combined_text)
                                
                                # Create subject
                                subject = self._create_subject(submission_type, link_text, financial_year)
                                
                                report = {
                                    'date': self._parse_date(filing_date or to_year or from_year),
                                    'ticker': ticker,
                                    'year': financial_year,
                                    'url': pdf_url,
                                    'subject': subject,
                                    'company_name': company['company_name'],
                                    'filing_date_text': filing_date or to_year or from_year,
                                    'extraction_method': 'enhanced_table',
                                    'from_year': from_year,
                                    'to_year': to_year,
                                    'submission_type': submission_type
                                }
                                reports.append(report)
                                self.logger.info(f"✅ Found report: {subject} for {financial_year}")
                
                except Exception as e:
                    self.logger.debug(f"Error parsing row {row_idx} for {ticker}: {e}")
                    continue
        
        return reports
    
    def _extract_from_any_links(self, soup: BeautifulSoup, company: Dict[str, str]) -> List[Dict]:
        """Extract from any links that might be annual reports."""
        reports = []
        ticker = company['ticker']
        
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            link_text = link.get_text().strip()
            
            if self._is_valid_document_link(href, link_text):
                parent_text = ""
                parent = link.parent
                for _ in range(3):
                    if parent:
                        parent_text += " " + parent.get_text()
                        parent = parent.parent
                    else:
                        break
                
                combined_text = f"{link_text} {parent_text}".lower()
                
                if href.startswith('http'):
                    pdf_url = href
                else:
                    pdf_url = urljoin(self.base_url, href)
                
                if self._is_likely_annual_report(combined_text, pdf_url):
                    financial_year = self._extract_year_from_text(combined_text)
                    subject = link_text if link_text else "Annual Report"
                    
                    report = {
                        'date': 'unknown',
                        'ticker': ticker,
                        'year': financial_year,
                        'url': pdf_url,
                        'subject': subject,
                        'company_name': company['company_name'],
                        'filing_date_text': 'unknown',
                        'extraction_method': 'enhanced_links'
                    }
                    reports.append(report)
                    self.logger.info(f"✅ Found report from link: {subject}")
        
        return reports
    
    def _extract_from_text_patterns(self, soup: BeautifulSoup, company: Dict[str, str]) -> List[Dict]:
        """Extract from text patterns when no clear structure is found."""
        reports = []
        # Implementation for text pattern extraction if needed
        return reports
    
    def download_pdf_with_enhanced_handling(self, url: str, file_path: Path) -> bool:
        """Download PDF with enhanced corruption detection and handling."""
        if file_path.exists():
            existing_size = file_path.stat().st_size
            if existing_size >= self.min_pdf_size:
                self.logger.info(f"Valid file already exists: {file_path} ({existing_size} bytes)")
                return True
            else:
                file_path.unlink()
                self.logger.warning(f"Removed small existing file: {file_path} ({existing_size} bytes)")
        
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Wait before starting download
        time.sleep(self.download_prep_wait)
        
        # Retry loop for downloads
        for attempt in range(self.download_retry_attempts):
            try:
                self.logger.info(f"📥 Downloading {url} (attempt {attempt + 1}/{self.download_retry_attempts})")
                
                # Progressive delay between attempts
                if attempt > 0:
                    delay = self.download_retry_delay + (attempt * 2)
                    time.sleep(delay)
                else:
                    time.sleep(self.request_delay)
                
                # Enhanced headers
                headers = {
                    'Accept': 'application/zip, application/pdf, application/octet-stream, */*',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                response = self.session.get(url, stream=True, timeout=35, headers=headers)
                response.raise_for_status()
                
                # Check response headers
                content_type = response.headers.get('content-type', '').lower()
                content_disposition = response.headers.get('content-disposition', '')
                
                self.logger.debug(f"Response headers - Content-Type: {content_type}, Content-Disposition: {content_disposition}")
                
                # Handle based on detected content type and URL
                if 'zip' in content_type or url.lower().endswith('.zip'):
                    self.corruption_stats['total_zip_attempts'] += 1
                    success = self._extract_pdf_from_zip_with_corruption_handling(response, file_path, attempt)
                else:
                    success = self._save_direct_pdf_with_validation(response, file_path, attempt)
                
                if success:
                    if file_path.exists() and file_path.stat().st_size >= self.min_pdf_size:
                        file_size = file_path.stat().st_size
                        self.logger.info(f"✅ Successfully downloaded: {file_path} ({file_size} bytes)")
                        return True
                    else:
                        self.logger.warning(f"⚠️ Downloaded file too small on attempt {attempt + 1}")
                        if file_path.exists():
                            file_path.unlink()
                
            except Exception as e:
                self.logger.warning(f"❌ Download attempt {attempt + 1} failed for {url}: {e}")
                
                if file_path.exists():
                    file_path.unlink()
                
                if attempt == self.download_retry_attempts - 1:
                    self.logger.error(f"❌ Failed to download after {self.download_retry_attempts} attempts: {url}")
                    return False
        
        return False
    
    def _extract_pdf_from_zip_with_corruption_handling(self, response, file_path: Path, attempt: int) -> bool:
        """Extract PDF from ZIP file with enhanced corruption detection."""
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
                        
                        # Capture first 1024 bytes for analysis
                        if len(content_sample) < 1024:
                            content_sample += chunk[:1024 - len(content_sample)]
                
                temp_zip_path = temp_zip.name
            
            self.logger.debug(f"Downloaded {total_size} bytes for ZIP analysis")
            
            # Validate ZIP file size
            if total_size < 1000:
                self.logger.warning(f"Downloaded ZIP too small ({total_size} bytes) on attempt {attempt + 1}")
                return False
            
            # Quick corruption check - validate ZIP signature
            if not content_sample.startswith(b'PK'):
                # Check if it's actually a PDF or HTML error
                if content_sample.startswith(b'%PDF'):
                    self.logger.info(f"File with .zip extension is actually a PDF, saving directly")
                    self.corruption_stats['misnamed_pdfs'] += 1
                    return self._save_misnamed_pdf(temp_zip_path, file_path)
                elif b'<html' in content_sample.lower() or b'<!doctype' in content_sample.lower():
                    self.logger.error(f"Server returned HTML error page instead of ZIP file")
                    self.corruption_stats['server_errors'] += 1
                    self._log_server_error(content_sample)
                    return False
                else:
                    self.logger.error(f"File doesn't have ZIP signature. First 50 bytes: {content_sample[:50]}")
                    self.corruption_stats['corrupted_zips'] += 1
                    return False
            
            # Try to validate ZIP file integrity before extraction
            corruption_status = self._check_zip_corruption(temp_zip_path)
            
            if corruption_status == "corrupted":
                self.logger.error(f"ZIP file is corrupted on server side (attempt {attempt + 1})")
                self.corruption_stats['corrupted_zips'] += 1
                self._log_corrupted_zip_info(temp_zip_path, file_path)
                return False
            elif corruption_status == "empty":
                self.logger.error(f"ZIP file exists but contains no files (attempt {attempt + 1})")
                self.corruption_stats['corrupted_zips'] += 1
                return False
            elif corruption_status == "no_pdf":
                self.logger.error(f"ZIP file is valid but contains no PDF files (attempt {attempt + 1})")
                return False
            elif corruption_status != "valid":
                self.logger.error(f"Unknown ZIP validation issue: {corruption_status}")
                self.corruption_stats['corrupted_zips'] += 1
                return False
            
            # If we get here, ZIP appears valid - proceed with extraction
            try:
                with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    pdf_files = [f for f in file_list if f.lower().endswith('.pdf')]
                    
                    # Extract the first PDF
                    pdf_file = pdf_files[0]
                    self.logger.debug(f"Extracting PDF: {pdf_file}")
                    
                    with zip_ref.open(pdf_file) as pdf_data:
                        pdf_content = pdf_data.read()
                        
                        # Validate extracted PDF
                        if len(pdf_content) < self.min_pdf_size:
                            self.logger.warning(f"Extracted PDF too small ({len(pdf_content)} bytes)")
                            return False
                        
                        if not pdf_content.startswith(b'%PDF'):
                            self.logger.warning(f"Extracted file doesn't appear to be a valid PDF")
                            return False
                        
                        # Write to final location
                        with open(file_path, 'wb') as output_file:
                            output_file.write(pdf_content)
                    
                    file_size = file_path.stat().st_size
                    if file_size >= self.min_pdf_size:
                        self.logger.info(f"📦 Successfully extracted PDF from ZIP: {file_path} ({file_size} bytes)")
                        self.corruption_stats['successful_extractions'] += 1
                        return True
                    else:
                        self.logger.warning(f"Final PDF file too small: {file_path} ({file_size} bytes)")
                        if file_path.exists():
                            file_path.unlink()
                        return False
                        
            except zipfile.BadZipFile as e:
                self.logger.error(f"ZIP file format error: {e}")
                self.corruption_stats['corrupted_zips'] += 1
                return False
            except Exception as e:
                self.logger.error(f"Error extracting from ZIP: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error processing ZIP file: {e}")
            return False
            
        finally:
            # Cleanup temporary ZIP file
            if temp_zip_path and os.path.exists(temp_zip_path):
                try:
                    os.unlink(temp_zip_path)
                except:
                    pass
    
    def _check_zip_corruption(self, zip_path: str) -> str:
        """Check ZIP file for corruption and return status."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Test ZIP file integrity
                bad_file = zip_ref.testzip()
                if bad_file:
                    return "corrupted"
                
                # Get file list
                file_list = zip_ref.namelist()
                
                if not file_list:
                    return "empty"
                
                # Check for PDF files
                pdf_files = [f for f in file_list if f.lower().endswith('.pdf')]
                if not pdf_files:
                    return "no_pdf"
                
                # Try to read info of first PDF to ensure it's accessible
                pdf_file = pdf_files[0]
                info = zip_ref.getinfo(pdf_file)
                if info.file_size == 0:
                    return "corrupted"
                
                return "valid"
                
        except zipfile.BadZipFile:
            return "corrupted"
        except Exception as e:
            self.logger.debug(f"ZIP validation error: {e}")
            return "corrupted"
    
    def _save_misnamed_pdf(self, source_path: str, target_path: Path) -> bool:
        """Save a file that has .zip extension but is actually a PDF."""
        try:
            with open(source_path, 'rb') as source_file:
                content = source_file.read()
                
            if len(content) >= self.min_pdf_size and content.startswith(b'%PDF'):
                with open(target_path, 'wb') as target_file:
                    target_file.write(content)
                
                file_size = target_path.stat().st_size
                self.logger.info(f"✅ Saved misnamed PDF: {target_path} ({file_size} bytes)")
                return True
            else:
                self.logger.warning(f"File doesn't appear to be a valid PDF")
                return False
                
        except Exception as e:
            self.logger.error(f"Error saving misnamed PDF: {e}")
            return False
    
    def _log_server_error(self, content_sample: bytes) -> None:
        """Log server error content for debugging."""
        try:
            content_str = content_sample.decode('utf-8', errors='ignore')
            # Extract error information from HTML
            if 'error' in content_str.lower():
                error_lines = [line.strip() for line in content_str.split('\n') 
                              if 'error' in line.lower() or 'not found' in line.lower()]
                if error_lines:
                    self.logger.error(f"Server error details: {'; '.join(error_lines[:3])}")
        except Exception:
            pass
    
    def _log_corrupted_zip_info(self, zip_path: str, target_path: Path) -> None:
        """Log information about corrupted ZIP file for reporting."""
        try:
            file_size = os.path.getsize(zip_path)
            
            # Create a corrupted files log
            corrupted_log_path = self.base_output_dir / "corrupted_files.log"
            
            with open(corrupted_log_path, 'a', encoding='utf-8') as log_file:
                timestamp = datetime.now().isoformat()
                log_file.write(f"{timestamp}: CORRUPTED ZIP - Target: {target_path}, Size: {file_size} bytes\n")
            
            self.logger.warning(f"Logged corrupted ZIP to {corrupted_log_path}")
            
        except Exception as e:
            self.logger.debug(f"Could not log corrupted ZIP info: {e}")
    
    def _save_direct_pdf_with_validation(self, response, file_path: Path, attempt: int) -> bool:
        """Save PDF directly from response with enhanced validation."""
        try:
            # Read response content
            content = b''
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
            
            # Validate content size
            if len(content) < self.min_pdf_size:
                self.logger.warning(f"Downloaded content too small ({len(content)} bytes) on attempt {attempt + 1}")
                return False
            
            # Check if it's actually a PDF or valid content
            if content.startswith(b'%PDF') or len(content) > 50000:  # Either PDF or large file
                with open(file_path, 'wb') as f:
                    f.write(content)
                
                file_size = file_path.stat().st_size
                if file_size >= self.min_pdf_size:
                    self.logger.debug(f"Downloaded valid file: {file_path} ({file_size} bytes)")
                    return True
                else:
                    self.logger.warning(f"Saved file too small: {file_path} ({file_size} bytes) on attempt {attempt + 1}")
                    if file_path.exists():
                        file_path.unlink()
                    return False
            else:
                self.logger.warning(f"Downloaded content doesn't appear to be valid on attempt {attempt + 1}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error saving PDF on attempt {attempt + 1}: {e}")
            if file_path.exists():
                file_path.unlink()
            return False
    
    # Helper methods for report extraction
    def _is_valid_document_link(self, href: str, link_text: str) -> bool:
        """Check if a link could be a valid document."""
        if not href:
            return False
        
        href_lower = href.lower()
        text_lower = link_text.lower()
        
        # Skip RSS feeds and other non-document links
        if 'rss' in href_lower or 'xml' in href_lower:
            return False
        
        document_indicators = [
            '.pdf', '.zip', '.doc', '.docx',
            'download', 'attachment', 'document',
            'nsearchives.nseindia.com'
        ]
        
        return any(indicator in href_lower or indicator in text_lower for indicator in document_indicators)
    
    def _is_likely_annual_report(self, text: str, url: str = "", from_year: str = "", to_year: str = "") -> bool:
        """Enhanced annual report detection with multiple criteria."""
        text_lower = text.lower()
        url_lower = url.lower()
        
        # Strong indicators
        strong_annual_indicators = [
            'annual report',
            'yearly report', 
            'audited annual results',
            'annual audited results'
        ]
        
        # Medium indicators
        medium_indicators = [
            'annual',
            'yearly',
            'year ended',
            'financial year',
            'fy'
        ]
        
        # URL indicators
        url_indicators = [
            'annual_report',
            'annual-report',
            'annualreport',
            '/ar_',
            'nsearchives.nseindia.com'
        ]
        
        # NSE specific patterns
        nse_patterns = [
            'attachment pdf',
            'attachment zip',
            'new',
            'revised'
        ]
        
        # Check for strong indicators
        has_strong = any(indicator in text_lower for indicator in strong_annual_indicators)
        if has_strong:
            return True
        
        # Check for URL indicators
        has_url_indicator = any(indicator in url_lower for indicator in url_indicators)
        if has_url_indicator:
            return True
        
        # Check for year range (strong indicator for annual reports)
        has_year_range = bool(re.search(r'\b(20\d{2})\s*[-–]\s*(20\d{2}|\d{2})\b', text))
        if has_year_range:
            return True
        
        # Check for valid from/to years
        if from_year and to_year:
            try:
                from_yr = int(from_year)
                to_yr = int(to_year) if len(to_year) == 4 else int(from_year[:2] + to_year)
                if 2000 <= from_yr <= 2030 and (to_yr - from_yr) == 1:
                    return True
            except ValueError:
                pass
        
        # Combination of medium indicators + NSE patterns
        has_medium = any(indicator in text_lower for indicator in medium_indicators)
        has_nse = any(pattern in text_lower for pattern in nse_patterns)
        
        if has_medium and has_nse:
            return True
        
        # Last resort: if it's a PDF/ZIP and mentions annual or report
        is_document = any(ext in url_lower for ext in ['.pdf', '.zip'])
        mentions_report = any(word in text_lower for word in ['annual', 'report', 'yearly'])
        
        if is_document and mentions_report:
            return True
        
        return False
    
    def _create_financial_year(self, from_year: str, to_year: str, text: str = "") -> str:
        """Create financial year string with fallback to text extraction."""
        if from_year and to_year:
            try:
                if len(from_year) == 4 and len(to_year) == 4:
                    return f"{from_year}-{to_year[-2:]}"
                elif len(from_year) == 4 and len(to_year) == 2:
                    return f"{from_year}-{to_year}"
                else:
                    return f"{from_year}-{to_year}"
            except:
                pass
        
        if text:
            year_from_text = self._extract_year_from_text(text)
            if year_from_text != "unknown":
                return year_from_text
        
        return from_year or to_year or "unknown"
    
    def _extract_year_from_text(self, text: str) -> str:
        """Extract financial year from text."""
        patterns = [
            r'(\d{4})-(\d{2,4})',
            r'FY\s*(\d{4})-?(\d{2,4})?',
            r'year\s+ended?\s+.*?(\d{4})',
            r'(\d{4})\s*-\s*(\d{2,4})',
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
    
    def _create_subject(self, submission_type: str, link_text: str, financial_year: str) -> str:
        """Create appropriate subject for the report."""
        if submission_type and submission_type.strip() and submission_type.strip() not in ['-', 'N/A']:
            return submission_type.strip()
        elif link_text and link_text.strip() and len(link_text.strip()) > 3:
            return link_text.strip()[:100]
        else:
            return f"Annual Report {financial_year}"
    
    def _parse_date(self, date_text: str) -> str:
        """Parse date from text with improved handling."""
        if not date_text or date_text.strip() in ["unknown", "", "-"]:
            return "unknown"
        
        date_text = date_text.strip()
        
        if len(date_text) == 4 and date_text.isdigit():
            return f"{date_text}-03-31"
        
        return date_text
    
    def save_metadata(self, report: Dict, file_path: Path) -> bool:
        """Save report metadata with enhanced information."""
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            enhanced_report = report.copy()
            enhanced_report.update({
                'downloaded_at': datetime.now().isoformat(),
                'scraper_version': 'Enhanced_v2.0',
                'parsing_engine': 'Enhanced_Selenium_BeautifulSoup'
            })
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(enhanced_report, f, indent=2, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving metadata: {e}")
            return False
    
    def process_company_with_enhanced_handling(self, company: Dict[str, str]) -> Dict[str, int]:
        """Process a single company with enhanced error handling."""
        ticker = company['ticker']
        stats = {
            'reports_found': 0, 
            'pdfs_downloaded': 0, 
            'metadata_saved': 0, 
            'errors': 0,
            'corrupted_files': 0,
            'server_errors': 0,
            'misnamed_pdfs': 0
        }
        
        try:
            # Load company page with enhanced patience
            soup = self.get_company_page_with_enhanced_patience(ticker)
            
            if not soup:
                self.logger.warning(f"Could not load page for {ticker} after all attempts")
                stats['errors'] += 1
                return stats
            
            # Extract reports
            reports = self.extract_annual_reports(soup, company)
            stats['reports_found'] = len(reports)
            
            if not reports:
                self.logger.warning(f"No annual reports found for {ticker}")
                return stats
            
            # Process each report with enhanced handling
            for report in reports:
                try:
                    year = report['year']
                    
                    # Skip if URL is unknown or invalid
                    if report['url'] in ['unknown', ''] or 'RSS' in report['url']:
                        self.logger.info(f"Skipping invalid URL for {ticker} {year}")
                        folder_path = self.base_output_dir / ticker / year
                        folder_path.mkdir(parents=True, exist_ok=True)
                        metadata_path = folder_path / "document_meta.json"
                        
                        # Save metadata for skipped files
                        skipped_report = report.copy()
                        skipped_report['download_status'] = 'skipped'
                        skipped_report['skip_reason'] = 'invalid_url'
                        skipped_report['downloaded_at'] = datetime.now().isoformat()
                        
                        if self.save_metadata(skipped_report, metadata_path):
                            stats['metadata_saved'] += 1
                        continue
                    
                    # Create folder structure
                    folder_path = self.base_output_dir / ticker / year
                    folder_path.mkdir(parents=True, exist_ok=True)
                    
                    pdf_path = folder_path / "document.pdf"
                    metadata_path = folder_path / "document_meta.json"
                    
                    # Download PDF with enhanced handling
                    download_success = self.download_pdf_with_enhanced_handling(report['url'], pdf_path)
                    
                    if download_success:
                        stats['pdfs_downloaded'] += 1
                        # Save successful metadata
                        success_report = report.copy()
                        success_report['download_status'] = 'success'
                        success_report['downloaded_at'] = datetime.now().isoformat()
                        success_report['file_size'] = pdf_path.stat().st_size if pdf_path.exists() else 0
                    else:
                        # Save failure metadata
                        success_report = report.copy()
                        success_report['download_status'] = 'failed'
                        success_report['failure_reason'] = 'download_failed_or_corrupted'
                        success_report['downloaded_at'] = datetime.now().isoformat()
                        
                        # Check what type of failure this was
                        if '.zip' in report['url'].lower():
                            stats['corrupted_files'] += 1
                    
                    # Save metadata
                    if self.save_metadata(success_report, metadata_path):
                        stats['metadata_saved'] += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing report for {ticker}: {e}")
                    stats['errors'] += 1
            
        except Exception as e:
            self.logger.error(f"Error processing company {ticker}: {e}")
            stats['errors'] += 1
        
        return stats
    
    def run_enhanced_scraping(self, max_companies: int = None) -> Dict[str, int]:
        """Run the enhanced scraping process."""
        start_time = datetime.now()
        overall_stats = {
            'start_time': start_time.isoformat(),
            'companies_discovered': 0,
            'companies_processed': 0,
            'reports_found': 0,
            'pdfs_downloaded': 0,
            'metadata_saved': 0,
            'errors': 0,
            'corrupted_files': 0,
            'server_errors': 0,
            'misnamed_pdfs': 0
        }
        
        try:
            self.logger.info("🚀 Starting Enhanced NSE Annual Reports Scraper")
            self.logger.info(f"⏰ Settings - Company attempts: {self.company_retry_attempts}, Download attempts: {self.download_retry_attempts}")
            self.logger.info(f"⏳ Wait times - Initial: {self.initial_wait}s, Content: {self.content_wait}s, Extraction: {self.extraction_wait}s")
            self.logger.info("🔧 Enhanced features - Corruption detection, graceful error handling, detailed logging")
            
            # Extract companies from CSV
            companies = self.extract_companies_from_csv()
            overall_stats['companies_discovered'] = len(companies)
            
            if not companies:
                self.logger.error("No companies found")
                return overall_stats
            
            # Limit companies for testing
            if max_companies:
                companies = companies[:max_companies]
                self.logger.info(f"Processing limited set: {len(companies)} companies")
            
            # Process each company with enhanced handling
            for i, company in enumerate(companies, 1):
                ticker = company['ticker']
                self.logger.info(f"🔄 Processing {i}/{len(companies)}: {ticker}")
                
                # Process company with enhanced handling
                company_stats = self.process_company_with_enhanced_handling(company)
                
                # Update overall stats
                for key in ['reports_found', 'pdfs_downloaded', 'metadata_saved', 'errors', 'corrupted_files', 'server_errors', 'misnamed_pdfs']:
                    if key in company_stats:
                        overall_stats[key] += company_stats[key]
                
                if company_stats['reports_found'] > 0:
                    overall_stats['companies_processed'] += 1
                
                # Progress update
                if i % 5 == 0:
                    self.logger.info(f"📊 Progress: {i}/{len(companies)} companies processed")
                    self.logger.info(f"📈 Stats - Reports: {overall_stats['reports_found']}, Downloads: {overall_stats['pdfs_downloaded']}")
                    self.logger.info(f"🚨 Issues - Corrupted: {self.corruption_stats['corrupted_zips']}, Server errors: {self.corruption_stats['server_errors']}")
            
            # Final statistics
            end_time = datetime.now()
            duration = end_time - start_time
            
            overall_stats.update({
                'end_time': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'average_time_per_company': duration.total_seconds() / len(companies) if companies else 0
            })
            
            # Add corruption stats
            overall_stats.update(self.corruption_stats)
            
            self.logger.info("🎉 Enhanced scraping completed!")
            
        except Exception as e:
            self.logger.error(f"Critical error: {e}")
            overall_stats['errors'] += 1
        
        return overall_stats
    
    def print_enhanced_summary(self, stats: Dict) -> None:
        """Print detailed summary with enhanced statistics."""
        print("\n" + "="*70)
        print("🚀 ENHANCED NSE SCRAPER - FINAL SUMMARY")
        print("="*70)
        print(f"📊 BASIC STATISTICS:")
        print(f"  🏢 Companies discovered: {stats.get('companies_discovered', 0)}")
        print(f"  ✅ Companies processed: {stats.get('companies_processed', 0)}")
        print(f"  📄 Reports found: {stats.get('reports_found', 0)}")
        print(f"  💾 PDFs downloaded: {stats.get('pdfs_downloaded', 0)}")
        print(f"  📝 Metadata saved: {stats.get('metadata_saved', 0)}")
        print(f"  ❌ Errors: {stats.get('errors', 0)}")
        
        # Enhanced corruption statistics
        print(f"\n🔍 CORRUPTION ANALYSIS:")
        print(f"  📦 Total ZIP attempts: {stats.get('total_zip_attempts', 0)}")
        print(f"  ✅ Successful extractions: {stats.get('successful_extractions', 0)}")
        print(f"  💥 Corrupted ZIP files: {stats.get('corrupted_zips', 0)}")
        print(f"  🔄 Misnamed PDFs (fixed): {stats.get('misnamed_pdfs', 0)}")
        print(f"  🌐 Server error pages: {stats.get('server_errors', 0)}")
        
        # Calculate success rates
        if stats.get('total_zip_attempts', 0) > 0:
            zip_success_rate = round((stats.get('successful_extractions', 0) / stats['total_zip_attempts']) * 100, 1)
            corruption_rate = round((stats.get('corrupted_zips', 0) / stats['total_zip_attempts']) * 100, 1)
            print(f"  📈 ZIP success rate: {zip_success_rate}%")
            print(f"  📉 ZIP corruption rate: {corruption_rate}%")
        
        # Performance metrics
        duration_seconds = stats.get('duration_seconds', 0)
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        
        print(f"\n⏱️  PERFORMANCE:")
        print(f"  🕐 Total time: {hours}h {minutes}m {seconds}s")
        print(f"  ⚡ Avg time per company: {stats.get('average_time_per_company', 0):.1f}s")
        
        # Success rates
        if stats.get('companies_discovered', 0) > 0:
            success_rate = round((stats.get('companies_processed', 0) / stats['companies_discovered']) * 100, 1)
            print(f"  📈 Processing success rate: {success_rate}%")
        
        if stats.get('reports_found', 0) > 0:
            download_rate = round((stats.get('pdfs_downloaded', 0) / stats['reports_found']) * 100, 1)
            print(f"  💾 Download success rate: {download_rate}%")
        
        print(f"\n📁 OUTPUT:")
        print(f"  📂 Output directory: {self.base_output_dir}")
        print(f"  📄 Main log: {self.base_output_dir / 'enhanced_scraper.log'}")
        print(f"  🚨 Corruption log: {self.base_output_dir / 'corrupted_files.log'}")
        
        print("\n🚀 ENHANCED FEATURES:")
        print("  ✅ Advanced corruption detection")
        print("  ✅ Graceful handling of server-side corrupted files")
        print("  ✅ Automatic misnamed PDF detection and fixing")
        print("  ✅ Detailed failure analysis and logging")
        print("  ✅ Metadata saved even for failed downloads")
        print("  ✅ Progressive retry delays")
        print("  ✅ Enhanced content type detection")
        print("="*70)


def test_enhanced_scraper():
    """Test the enhanced scraper with problematic companies."""
    print("🧪 Testing Enhanced Scraper...")
    
    scraper = EnhancedNSEScraper(base_output_dir="test_enhanced")
    
    # Test companies that were having issues
    test_companies = [
        {'ticker': '20MICRONS', 'company_name': '20 Microns Limited'},
        {'ticker': 'AARTIIND', 'company_name': 'Aarti Industries Limited'},
        {'ticker': 'AARTIDRUGS', 'company_name': 'Aarti Drugs Limited'},
        {'ticker': '360ONE', 'company_name': '360 ONE WAM Limited'}
    ]
    
    for company in test_companies:
        print(f"\n--- Testing {company['ticker']} with Enhanced Handling ---")
        stats = scraper.process_company_with_enhanced_handling(company)
        print(f"Results: {stats['reports_found']} reports found, {stats['pdfs_downloaded']} downloaded")
        print(f"Issues: Corrupted={stats['corrupted_files']}, Errors={stats['errors']}")
    
    print(f"\nCorruption Statistics:")
    print(f"  Corrupted ZIPs: {scraper.corruption_stats['corrupted_zips']}")
    print(f"  Misnamed PDFs: {scraper.corruption_stats['misnamed_pdfs']}")
    print(f"  Server errors: {scraper.corruption_stats['server_errors']}")
    print(f"  Successful extractions: {scraper.corruption_stats['successful_extractions']}")
    
    print("🧪 Enhanced test completed!")


def main():
    """Main function for the enhanced scraper."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced NSE Scraper with Corruption Detection')
    parser.add_argument('--output-dir', default='nse_annual_reports_enhanced',
                       help='Output directory for scraped data')
    parser.add_argument('--max-companies', type=int,
                       help='Maximum number of companies to process')
    parser.add_argument('--test-enhanced', action='store_true',
                       help='Test enhanced logic with problematic companies')
    parser.add_argument('--company-attempts', type=int, default=4,
                       help='Number of company page load attempts (default: 4)')
    parser.add_argument('--download-attempts', type=int, default=4,
                       help='Number of download attempts (default: 4)')
    parser.add_argument('--extra-patient', action='store_true',
                       help='Use even longer wait times for maximum patience')
    parser.add_argument('--skip-corrupted', action='store_true',
                       help='Skip known corrupted files faster (recommended)')
    
    args = parser.parse_args()
    
    # Handle test mode
    if args.test_enhanced:
        test_enhanced_scraper()
        return
    
    try:
        # Create enhanced scraper
        scraper = EnhancedNSEScraper(base_output_dir=args.output_dir)
        scraper.company_retry_attempts = args.company_attempts
        scraper.download_retry_attempts = args.download_attempts
        
        # Extra patient mode
        if args.extra_patient:
            scraper.initial_wait = 8
            scraper.content_wait = 7
            scraper.extraction_wait = 5
            scraper.download_prep_wait = 3
            scraper.company_retry_delay = 10
            print("🐌 Using EXTRA PATIENT mode with maximum wait times")
        
        # Skip corrupted mode (faster processing)
        if args.skip_corrupted:
            scraper.download_retry_attempts = 2  # Reduce retries for corrupted files
            print("⚡ Using SKIP CORRUPTED mode for faster processing")
        
        print("🚀 Enhanced NSE Scraper - Advanced Corruption Detection & Handling")
        print(f"📁 Output directory: {scraper.base_output_dir}")
        print(f"🏢 Max companies: {args.max_companies or 'All'}")
        print(f"🔄 Company attempts: {scraper.company_retry_attempts}")
        print(f"📥 Download attempts: {scraper.download_retry_attempts}")
        print(f"⏳ Wait times: Initial={scraper.initial_wait}s, Content={scraper.content_wait}s")
        print("\n🚀 ENHANCED FEATURES:")
        print("  - Advanced ZIP corruption detection")
        print("  - Graceful handling of server-side corrupted files")
        print("  - Automatic misnamed PDF detection and fixing")
        print("  - Detailed failure analysis and logging")
        print("  - Metadata saved even for failed downloads")
        print("  - Progressive retry delays")
        print("  - Enhanced content type detection")
        print("  - Server error page detection")
        print()
        
        # Run the scraping
        stats = scraper.run_enhanced_scraping(max_companies=args.max_companies)
        
        # Print enhanced summary
        scraper.print_enhanced_summary(stats)
        
        # Additional recommendations based on results
        print("\n💡 RECOMMENDATIONS:")
        if stats.get('corrupted_zips', 0) > 0:
            print(f"  🚨 Found {stats['corrupted_zips']} corrupted ZIP files on NSE's server")
            print("  📝 Check corrupted_files.log for details")
            print("  📧 Consider reporting to NSE for file integrity issues")
        
        if stats.get('server_errors', 0) > 0:
            print(f"  🌐 Encountered {stats['server_errors']} server error pages")
            print("  🔄 These may be temporary - try again later")
        
        if stats.get('misnamed_pdfs', 0) > 0:
            print(f"  🔄 Fixed {stats['misnamed_pdfs']} misnamed PDF files")
            print("  ✅ These were automatically handled")
        
        corruption_rate = 0
        if stats.get('total_zip_attempts', 0) > 0:
            corruption_rate = (stats.get('corrupted_zips', 0) / stats['total_zip_attempts']) * 100
        
        if corruption_rate > 20:
            print("  ⚠️  High corruption rate detected - this is a server-side issue")
            print("  💡 Use --skip-corrupted flag for faster processing")
        elif corruption_rate > 10:
            print("  ⚠️  Moderate corruption rate - some files may be temporarily unavailable")
        else:
            print("  ✅ Low corruption rate - server files are mostly healthy")
        
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()