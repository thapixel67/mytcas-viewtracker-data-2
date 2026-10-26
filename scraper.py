"""
myTCAS ViewTracker - Scalable Async Data Pipeline
Extracts view counts and details for all nationwide TCAS programs.
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple
import aiohttp
from bs4 import BeautifulSoup

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Constants & Configurations
BASE_URL = "https://course.mytcas.com"
UNIVERSITIES_URL = f"{BASE_URL}/universities"
MASTER_CATALOG_FILE = "master_catalog.json"
OUTPUT_DATA_FILE = "data.json"

MAX_CONCURRENT_REQUESTS = 25
REQUEST_TIMEOUT = 18
MAX_RETRIES = 3
THAI_TZ = timezone(timedelta(hours=7))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Referer": "https://course.mytcas.com/",
}


class TCASPipeline:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.build_id: Optional[str] = None
        self.previous_data: Dict[str, dict] = {}

    async def init_session(self):
        connector = aiohttp.TCPConnector(limit=100, ssl=False, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers=HEADERS
        )
        self._load_previous_data()

    def _load_previous_data(self):
        """Loads previous run to ensure zero-loss fallback."""
        if os.path.exists(OUTPUT_DATA_FILE):
            try:
                with open(OUTPUT_DATA_FILE, "r", encoding="utf-8") as f:
                    old_payload = json.load(f)
                    items = old_payload.get("programs", [])
                    self.previous_data = {item["id"]: item for item in items if "id" in item}
                    logging.info(f"Loaded {len(self.previous_data)} records from previous dataset.")
            except Exception as e:
                logging.warning(f"Could not load previous dataset: {e}")

    async def fetch_with_retry(self, url: str, is_json: bool = False) -> Optional[any]:
        """Asynchronous HTTP GET with jitter backoff."""
        for attempt in range(1, MAX_RETRIES + 1):
            async with self.semaphore:
                try:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            if is_json:
                                return await response.json()
                            return await response.text()
                        elif response.status == 404:
                            return None
                        elif response.status in (429, 403, 503):
                            delay = attempt * 2 + (0.1 * (time.time() % 10))
                            logging.warning(f"Rate limited/Blocked ({response.status}) at {url}. Backing off {delay:.1f}s...")
                            await asyncio.sleep(delay)
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        logging.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts: {e}")
                    await asyncio.sleep(attempt * 1.5)
        return None

    # ================= STAGE 1: DISCOVERY =================
    async def discover_build_id_and_universities(self) -> Tuple[Optional[str], List[dict]]:
        """Inspects HTML and __NEXT_DATA__ to extract buildId and universities."""
        logging.info("Stage 1.1: Discovering Next.js Build ID and Universities...")
        html = await self.fetch_with_retry(UNIVERSITIES_URL)
        if not html:
            raise RuntimeError("Failed to reach universities landing page.")

        soup = BeautifulSoup(html, "html.parser")
        next_data_script = soup.find("script", id="__NEXT_DATA__")
        
        build_id = None
        universities = []

        if next_data_script and next_data_script.string:
            try:
                next_json = json.loads(next_data_script.string)
                build_id = next_json.get("buildId")
                page_props = next_json.get("props", {}).get("pageProps", {})
                universities = page_props.get("universities", [])
            except json.JSONDecodeError:
                pass

        # Fallback if __NEXT_DATA__ did not yield build_id
        if not build_id:
            match = re.search(r'/_next/static/([a-zA-Z0-9_-]+)/_buildManifest\.js', html)
            if match:
                build_id = match.group(1)

        # Fallback university scraping via DOM
        if not universities:
            for a_tag in soup.find_all("a", href=True):
                m = re.match(r"/universities/([a-zA-Z0-9_-]+)", a_tag["href"])
                if m:
                    universities.append({"id": m.group(1), "name": a_tag.get_text(strip=True)})

        logging.info(f"Detected BuildId: {build_id} | Discovered {len(universities)} universities.")
        self.build_id = build_id
        return build_id, universities

    async def crawl_university_programs(self, uni_id: str) -> Set[str]:
        """Discovers all Program IDs within a university."""
        program_ids: Set[str] = set()

        # Method A: Next.js Data JSON Endpoint (Fastest)
        if self.build_id:
            data_url = f"{BASE_URL}/_next/data/{self.build_id}/universities/{uni_id}.json"
            json_data = await self.fetch_with_retry(data_url, is_json=True)
            if json_data:
                text_payload = json.dumps(json_data)
                # Next.js payload contains program IDs usually matching 10-16 alphanumeric codes
                found = re.findall(r'/programs/([0-9A-Za-z]{10,20})', text_payload)
                program_ids.update(found)
                # Also check direct nested program IDs in JSON
                found_direct = re.findall(r'"programId":\s*"([0-9A-Za-z]{10,20})"', text_payload)
                program_ids.update(found_direct)
                if program_ids:
                    return program_ids

        # Method B: Fallback HTML Scraping
        uni_url = f"{BASE_URL}/universities/{uni_id}"
        html = await self.fetch_with_retry(uni_url)
        if html:
            found = re.findall(r'/programs/([0-9A-Za-z]{10,20})', html)
            program_ids.update(found)

        return program_ids

    async def run_discovery_stage(self) -> List[str]:
        """Coordinates full nationwide program ID discovery."""
        build_id, universities = await self.discover_build_id_and_universities()
        logging.info(f"Stage 1.2: Crawling programs across {len(universities)} institutions...")

        tasks = [self.crawl_university_programs(u.get("id")) for u in universities if u.get("id")]
        results = await asyncio.gather(*tasks)

        all_ids: Set[str] = set()
        for r in results:
            all_ids.update(r)

        # Merge with existing master catalog to preserve historical courses
        if os.path.exists(MASTER_CATALOG_FILE):
            try:
                with open(MASTER_CATALOG_FILE, "r", encoding="utf-8") as f:
                    cached_catalog = set(json.load(f))
                    all_ids.update(cached_catalog)
            except Exception:
                pass

        sorted_catalog = sorted(list(all_ids))
        logging.info(f"Stage 1 Complete: Total active programs discovered: {len(sorted_catalog)}")
        
        with open(MASTER_CATALOG_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted_catalog, f, ensure_ascii=False, indent=2)

        return sorted_catalog

    # ================= STAGE 2: BATCH SCRAPING =================
    def _extract_views_from_text(self, text: str) -> Optional[int]:
        """Regex extractor for Thai view count patterns."""
        match = re.search(r'ยอดผู้เข้าชม[:\s]*([\d,]+)\s*ครั้ง', text)
        if match:
            clean = match.group(1).replace(",", "").strip()
            if clean.isdigit():
                return int(clean)
        return None

    async def scrape_single_program(self, prog_id: str) -> Optional[dict]:
        """Extracts data for an individual program with anti-zero fallback."""
        prog_data = None
        views = None

        # Strategy 1: Next.js Data API
        if self.build_id:
            json_url = f"{BASE_URL}/_next/data/{self.build_id}/programs/{prog_id}.json"
            raw_json = await self.fetch_with_retry(json_url, is_json=True)
            if raw_json and "pageProps" in raw_json:
                props = raw_json["pageProps"]
                prog = props.get("program", props.get("data", {}))
                
                # Check direct view field inside JSON
                for key in ("views", "view_count", "pageviews", "visit_count"):
                    if key in prog and isinstance(prog[key], (int, float)):
                        views = int(prog[key])
                        break

                prog_data = {
                    "id": prog_id,
                    "name": prog.get("name") or prog.get("program_name") or "",
                    "faculty": prog.get("faculty_name") or prog.get("faculty") or "",
                    "university": prog.get("university_name") or prog.get("university") or "",
                    "campus": prog.get("campus_name") or prog.get("campus") or "-",
                    "degree": prog.get("degree_name") or prog.get("degree") or "-",
                    "cost": prog.get("cost") or prog.get("tuition_fee") or "-",
                    "quota_r1": prog.get("quota_r1", 0),
                    "quota_r2": prog.get("quota_r2", 0),
                    "quota_r3": prog.get("quota_r3", 0),
                    "quota_r4": prog.get("quota_r4", 0),
                }

        # Strategy 2: Fallback to HTML Scraping
        if not prog_data or views is None:
            html_url = f"{BASE_URL}/programs/{prog_id}"
            html = await self.fetch_with_retry(html_url)
            if html:
                extracted_views = self._extract_views_from_text(html)
                if extracted_views is not None:
                    views = extracted_views

                if not prog_data:
                    soup = BeautifulSoup(html, "html.parser")
                    title = soup.find("h1")
                    prog_name = title.get_text(strip=True) if title else ""
                    
                    prog_data = {
                        "id": prog_id,
                        "name": prog_name,
                        "faculty": "",
                        "university": "",
                        "campus": "-",
                        "degree": "-",
                        "cost": "-",
                        "quota_r1": 0,
                        "quota_r2": 0,
                        "quota_r3": 0,
                        "quota_r4": 0,
                    }

        # Fallback Check & Zero-Value Integrity Guard
        prev_record = self.previous_data.get(prog_id, {})
        
        # If current views are suspiciously 0 or 1, and we had higher previous data, retain previous
        if (views is None or views <= 1) and prev_record.get("views", 0) > 1:
            views = prev_record["views"]

        if not prog_data and prev_record:
            prog_data = prev_record.copy()

        if prog_data:
            prog_data["views"] = views if views is not None else 0
            # Fill missing attributes from historical cache
            for field in ["name", "faculty", "university", "campus", "degree"]:
                if not prog_data.get(field) and prev_record.get(field):
                    prog_data[field] = prev_record[field]
            
            # Calculate total seats
            prog_data["total_seats"] = sum([
                prog_data.get("quota_r1", 0) or 0,
                prog_data.get("quota_r2", 0) or 0,
                prog_data.get("quota_r3", 0) or 0,
                prog_data.get("quota_r4", 0) or 0
            ])
            return prog_data

        return None

    # ================= STAGE 3: COMPILATION =================
    async def run(self):
        start_time = time.time()
        await self.init_session()

        try:
            catalog = await self.run_discovery_stage()
            logging.info(f"Stage 2: Scraping metrics for {len(catalog)} programs in parallel...")

            # Progress tracking chunking
            total = len(catalog)
            completed_programs = []
            chunk_size = 500

            for i in range(0, total, chunk_size):
                chunk = catalog[i : i + chunk_size]
                tasks = [self.scrape_single_program(pid) for pid in chunk]
                chunk_results = await asyncio.gather(*tasks)
                
                for item in chunk_results:
                    if item and item["id"]:
                        completed_programs.append(item)

                elapsed = time.time() - start_time
                logging.info(
                    f"Progress: [{min(i + chunk_size, total)}/{total}] programs processed "
                    f"({(min(i + chunk_size, total)/total)*100:.1f}%) in {elapsed:.1f}s"
                )

            logging.info("Stage 3: Sorting and compiling output dataset...")
            # Rank strictly by views descending
            completed_programs.sort(key=lambda x: x.get("views", 0), reverse=True)

            payload = {
                "metadata": {
                    "last_updated": datetime.now(THAI_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    "total_programs": len(completed_programs),
                    "execution_time_sec": round(time.time() - start_time, 2),
                    "build_id": self.build_id,
                },
                "programs": completed_programs,
            }

            with open(OUTPUT_DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=None, separators=(',', ':'))

            logging.info(
                f"Execution finished in {payload['metadata']['execution_time_sec']}s. "
                f"Saved {len(completed_programs)} programs into '{OUTPUT_DATA_FILE}'."
            )

        finally:
            await self.session.close()


if __name__ == "__main__":
    pipeline = TCASPipeline()
    asyncio.run(pipeline.run())
