#!/usr/bin/env python3
"""
Ticket CSV Downloader & Aggregator from .eml Emails
==================================================

This script:
1. Parses a .eml email file
2. Extracts all hyperlinks ending in .csv
3. Downloads each CSV file (with retry logic)
4. Logs download and aggregation process
5. Combines all CSVs into one (single header)
6. Stores everything in a timestamped output folder


Usage:
    python download_tickets.py --email emails/tickets.eml --name JulyCampaign
"""

import os
import sys
import re
import csv
import time
import argparse
import logging
from datetime import datetime
from email import message_from_file
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


class TicketCSVDownloader:
    """
    Main class to handle downloading and aggregating CSVs from .eml email files.
    """

    def __init__(self, eml_path: str, output_name: str, base_output_dir: str = "output"):
        """
        Initialize the downloader.

        :param eml_path: Path to the .eml email file
        :param output_name: User-defined name for output (e.g., "JulyCampaign")
        :param base_output_dir: Base directory to store outputs
        """
        self.eml_path = eml_path
        self.output_name = output_name
        self.base_output_dir = base_output_dir

        # Derived paths
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_folder = f"dtb.{self.output_name}_{self.timestamp}"
        self.output_dir = os.path.join(self.base_output_dir, self.output_folder)
        self.raw_dir = os.path.join(self.output_dir, "raw_downloads")
        self.logs_dir = os.path.join(self.output_dir, "logs")

        # Retry settings
        self.retry_attempts = 3
        self.retry_delay = 2  # seconds
        self.timeout = 10  # seconds
        self.user_agent = "TicketCSV-Downloader/1.0"

        # Logging
        self.download_logger = None
        self.agg_logger = None

        self._setup_directories()
        self._setup_logging()

    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        os.makedirs(self.base_output_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

    def _setup_logging(self):
        """Set up logging for downloads and aggregation."""
        # Download logger
        download_log_path = os.path.join(self.logs_dir, "downloads.log")
        self.download_logger = self._create_logger("DOWNLOAD", download_log_path)

        # Aggregation logger
        agg_log_path = os.path.join(self.logs_dir, "aggregation.log")
        self.agg_logger = self._create_logger("AGGREGATE", agg_log_path)

    def _create_logger(self, name: str, log_path: str) -> logging.Logger:
        """
        Create a custom logger that writes to a file with timestamped entries.

        :param name: Logger name (e.g., DOWNLOAD, AGGREGATE)
        :param log_path: Path to log file
        :return: Configured logger
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        formatter = logging.Formatter(fmt="%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False  # Prevent duplicate logs
        return logger

    def extract_csv_urls(self) -> list:
        """
        Extract all .csv URLs from the HTML body of the .eml file.

        :return: List of CSV URLs
        """
        if not os.path.exists(self.eml_path):
            self.download_logger.error(f"EMAIL NOT FOUND: {self.eml_path}")
            return []

        try:
            with open(self.eml_path, "r", encoding="utf-8", errors="ignore") as f:
                msg = message_from_file(f)

            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    content_disposition = str(part.get("Content-Disposition", ""))
                    if "attachment" not in content_disposition:
                        content_type = part.get_content_type()
                        if content_type == "text/html":
                            payload = part.get_payload(decode=True)
                            if payload:
                                body += payload.decode("utf-8", errors="ignore")
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="ignore")

            # Extract all hrefs from <a> tags
            links = re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>', body, re.IGNORECASE)
            # Filter only those ending with .csv
            csv_urls = [url.strip() for url in links if url.lower().endswith(".csv")]
            self.agg_logger.info(f"Found {len(csv_urls)} .csv URLs in email.")
            return csv_urls

        except Exception as e:
            self.download_logger.error(f"FAILED TO PARSE EMAIL: {str(e)}")
            return []

    def download_file(self, url: str) -> str:
        """
        Download a single CSV file with retry logic.

        :param url: URL to download
        :return: Full path to saved file, or None if failed
        """
        filename = os.path.basename(urlparse(url).path)
        if not filename or "." not in filename:
            filename = f"download_{int(time.time())}.csv"

        filepath = os.path.join(self.raw_dir, filename)

        for attempt in range(1, self.retry_attempts + 1):
            try:
                req = Request(url, headers={"User-Agent": self.user_agent})
                with urlopen(req, timeout=self.timeout) as response:
                    if response.status == 200:
                        data = response.read()
                        with open(filepath, "wb") as f:
                            f.write(data)
                        self.download_logger.info(f"DOWNLOAD: {url} -> {filename} - SUCCESS")
                        return filepath
                    else:
                        msg = f"HTTP {response.status}"
                        if attempt < self.retry_attempts:
                            time.sleep(self.retry_delay)
                            continue
                        self.download_logger.error(f"DOWNLOAD: {url} -> {filename} - FAILED ({msg})")
                        return None

            except HTTPError as e:
                msg = f"HTTP {e.code}"
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay)
                    continue
                self.download_logger.error(f"DOWNLOAD: {url} -> {filename} - FAILED ({msg})")
                return None

            except URLError as e:
                msg = str(e.reason) if hasattr(e, "reason") else str(e)
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay)
                    continue
                self.download_logger.error(f"DOWNLOAD: {url} -> {filename} - FAILED ({msg})")
                return None

            except Exception as e:
                msg = str(e)
                if attempt < self.retry_attempts:
                    time.sleep(self.retry_delay)
                    continue
                self.download_logger.error(f"DOWNLOAD: {url} -> {filename} - FAILED ({msg})")
                return None

        return None

    def aggregate_csv_files(self):
        """
        Combine all downloaded CSVs into one file (single header).
        Skips header rows in subsequent files.
        """
        csv_files = [f for f in os.listdir(self.raw_dir) if f.lower().endswith(".csv")]
        combined_path = os.path.join(self.output_dir, f"combined_{self.output_name}.csv")

        if not csv_files:
            self.agg_logger.info("No CSV files to aggregate.")
            return

        header_written = False
        total_data_rows = 0

        try:
            with open(combined_path, "w", newline="", encoding="utf-8") as combined:
                writer = None

                for file in csv_files:
                    filepath = os.path.join(self.raw_dir, file)
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            sample = f.read(1024)
                            f.seek(0)
                            has_header = False
                            try:
                                has_header = csv.Sniffer().has_header(sample)
                            except csv.Error:
                                pass  # Can't detect header, assume no

                            reader = csv.reader(f)
                            rows = list(reader)

                        if not rows:
                            self.agg_logger.info(f"{file} - SKIPPED (empty)")
                            continue

                        if not writer:
                            writer = csv.writer(combined)

                        if not header_written:
                            writer.writerow(rows[0])
                            header_written = True
                            data_rows = rows[1:]
                        else:
                            data_rows = rows[1:] if has_header else rows

                        row_count = len(data_rows)
                        for row in data_rows:
                            writer.writerow(row)
                        total_data_rows += row_count
                        self.agg_logger.info(f"{file} - ADDED ({row_count} rows)")

                    except Exception as e:
                        self.agg_logger.info(f"{file} - FAILED ({str(e)})")

            final_row_count = total_data_rows + (1 if header_written else 0)
            self.agg_logger.info(f"FINAL: combined_{self.output_name}.csv - CREATED ({final_row_count} rows)")
            print(f"Aggregation complete: {combined_path}")

        except Exception as e:
            self.agg_logger.info(f"AGGREGATION FAILED: {str(e)}")

    def run(self):
        """
        Execute the full pipeline: extract → download → aggregate.
        """
        print(f"Parsing email: {self.eml_path}")
        print(f"Output folder: {self.output_dir}")

        # Step 1: Extract CSV URLs
        urls = self.extract_csv_urls()
        if not urls:
            print(" No .csv URLs found in email.")
            return

        # Step 2: Download each URL
        downloaded_files = []
        for url in urls:
            filepath = self.download_file(url)
            if filepath:
                downloaded_files.append(filepath)

        print(f" Downloaded {len(downloaded_files)} out of {len(urls)} files.")

        # Step 3: Aggregate all CSVs
        self.aggregate_csv_files()
        print(f"Process complete. Check logs in: {self.logs_dir}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download and aggregate CSVs from hyperlinks in a .eml email."
    )
    parser.add_argument("--email", required=True, help="Path to the .eml file (e.g., emails/alert.eml)")
    parser.add_argument("--name", required=True, help="Custom name for output (e.g., JulyCampaign)")

    args = parser.parse_args()

    # Instantiate and run
    downloader = TicketCSVDownloader(eml_path=args.email, output_name=args.name)
    downloader.run()


if __name__ == "__main__":
    main()