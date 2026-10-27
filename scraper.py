#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
myTCAS ViewTracker - Production Pipeline (Universal Key Mapping & Deep Header Spoofing)
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiohttp
from bs4 import BeautifulSoup

TZ_TH = timezone(timedelta(hours=7))

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("myTCAS-Scraper")
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.propagate = False

BASE_URL = "https://course.mytcas.com"
MAX_CONCURRENT_REQUESTS = 30
REQUEST_TIMEOUT_SEC = 15
SOCK_READ_TIMEOUT = 10
OUTPUT_FILE = Path("data.json")
SPLIT_DIR = Path("data/universities")

S3_COURSES_URL = "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/courses.json"
S3_UNIVERSITIES_URL = "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas/universities.json"

VIEW_COUNT_PATTERNS = [
    re.compile(r"ยอดผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r"ผู้เข้าชม\s*[:\-]?\s*([0-9,]+)\s*ครั้ง", re.IGNORECASE),
    re.compile(r"ยอดผู้เข้าชม\s*([0-9,]+)", re.IGNORECASE),
    re.compile(r'"(?:views|view_count|viewCount|viewsCount|hits|pageviews|total_views)"\s*:\s*([0-9]+)', re.IGNORECASE),
    re.compile(r'([0-9,]+)\s*ครั้ง', re.IGNORECASE),
]

def get_browser_headers() -> Dict[str, str]:
    """สร้าง Headers จำลอง Google Chrome ตัวจริงบน Windows เพื่อหลบ Cloudflare WAF"""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://course.mytcas.com/",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
    }

def classify_faculty_group(faculty: str, program_name: str) -> str:
    text = f"{faculty} {program_name}".lower()
    if any(k in text for k in ["แพทยศาสตร", "ทันตแพทย", "เภสัชศาสตร", "สัตวแพทย"]):
        return "แพทย์ / ทันตะ / เภสัช / สัตวแพทย์"
    if any(k in text for k in ["พยาบาล", "สหเวช", "เทคนิคการแพทย์", "สาธารณสุข", "กายภาพบำบัด"]):
        return "พยาบาล / สหเวช / สุขภาพ"
    if any(k in text for k in ["คอมพิวเตอร์", "ดิจิทัล", "สารสนเทศ", "ปัญญาประดิษฐ์", "ข้อมูล", "ไอที", "ai", "data", "cyber"]):
        return "วิทยาการคอม / IT / ดิจิทัล"
    if any(k in text for k in ["วิศวกรรม", "ช่างอุตสาหกรรม"]):
        return "วิศวกรรมศาสตร์"
    if any(k in text for k in ["บริหารธุรกิจ", "บัญชี", "พาณิชยศาสตร์", "การตลาด", "การจัดการ", "โลจิสติกส์", "การเงิน"]):
        return "บริหารธุรกิจ / บัญชี"
    if "เศรษฐศาสตร" in text:
        return "เศรษฐศาสตร์"
    if any(k in text for k in ["นิติศาสตร", "กฎหมาย", "รัฐศาสตร", "การปกครอง"]):
        return "นิติศาสตร์ / รัฐศาสตร์"
    if any(k in text for k in ["อักษรศาสตร", "ศิลปศาสตร", "มนุษยศาสตร", "ภาษา"]):
        return "มนุษยศาสตร์ / ศิลปศาสตร์ / ภาษา"
    if any(k in text for k in ["ครุศาสตร", "ศึกษาศาสตร", "ครู"]):
        return "ครุศาสตร์ / ศึกษาศาสตร์"
    if any(k in text for k in ["สถาปัตยกรรม", "ศิลปกรรม", "นิเทศศาสตร", "การออกแบบ"]):
        return "สถาปัตย์ / ศิลปกรรม / นิเทศศาสตร์"
    if any(k in text for k in ["เกษตร", "ประมง", "วนศาสตร", "อุตสาหกรรมเกษตร"]):
        return "เกษตรศาสตร์ / อุตสาหกรรมเกษตร"
    if "วิทยาศาสตร" in text:
        return "วิทยาศาสตร์"
    return "อื่นๆ / สหวิทยาการ"

def extract_val(d: Dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""

async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    timeout = aiohttp.ClientTimeout(total=25, sock_read=15)
    try:
        async with session.get(url, headers=get_browser_headers(), timeout=timeout) as response:
            if response.status == 200:
                return await response.json(content_type=None)
    except Exception as e:
        logger.warning(f"ดึง JSON จาก {url} ไม่สำเร็จ: {e}")
    return None

async def fetch_program_html(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> Optional[str]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC, sock_read=SOCK_READ_TIMEOUT)
    async with semaphore:
        await asyncio.sleep(random.uniform(0.04, 0.10))
        try:
            async with session.get(url, headers=get_browser_headers(), timeout=timeout, allow_redirects=True) as response:
                if response.status == 200:
                    return await response.text(errors="replace")
        except Exception:
            pass
    return None

def parse_program_data(pid: str, html: Optional[str], raw: Dict, uni_map: Dict[str, str]) -> Dict[str, Any]:
    # ดึงรหัสสถาบันและชื่อมหาวิทยาลัย
    u_id = extract_val(raw, "u_id", "university_id") or pid[:3]
    university = extract_val(raw, "u_name", "university_name") or uni_map.get(u_id, "ไม่ระบุสถาบัน")
    faculty = extract_val(raw, "faculty_name", "faculty_name_th", "faculty") or "ไม่ระบุคณะ"
    
    # ดึงชื่อสาขาวิชาจากทุกคีย์ที่เป็นไปได้ใน S3
    program_name = extract_val(raw, "name", "name_th", "program_name", "program_name_th", "major_name", "field_name", "title")
    if not program_name or "ระบบการคัดเลือก" in program_name:
        program_name = f"สาขาวิชา ({pid})"

    def parse_seat(key: str) -> int:
        val = raw.get(key, 0)
        try:
            return int(str(val).replace(",", "").strip() or 0)
        except ValueError:
            return 0

    r1 = parse_seat("r1") or parse_seat("round1")
    r2 = parse_seat("r2") or parse_seat("round2")
    r3 = parse_seat("r3") or parse_seat("round3")
    r4 = parse_seat("r4") or parse_seat("round4")
    total_seats = r1 + r2 + r3 + r4
    if total_seats == 0:
        total_seats = parse_seat("total_seats") or parse_seat("seats") or parse_seat("quota")

    views = 0
    if html:
        for p in VIEW_COUNT_PATTERNS:
            m = p.search(html)
            if m:
                raw_v = m.group(1).replace(",", "").strip()
                if raw_v.isdigit() and int(raw_v) > 0:
                    views = int(raw_v)
                    break

        if views == 0:
            soup = BeautifulSoup(html, "html.parser")
            tag = soup.find("script", id="__NEXT_DATA__")
            if tag and tag.string:
                for p in VIEW_COUNT_PATTERNS:
                    m = p.search(tag.string)
                    if m:
                        views = int(m.group(1).replace(",", ""))
                        break

    if views == 0:
        meta_views = extract_val(raw, "views", "view_count", "hits", "pageviews")
        if meta_views.isdigit():
            views = int(meta_views)

    category = classify_faculty_group(faculty, program_name)
    campus = extract_val(raw, "campus_name", "campus") or "วิทยาเขตหลัก"
    program_type = extract_val(raw, "program_type") or "ภาษาไทย ปกติ"

    # บันทึกแบบ Universal Key Mapping (รองรับทั้ง index.html เดิมและ index.html ใหม่)
    return {
        # ID Keys
        "id": pid,
        "program_id": pid,
        "code": pid,
        "program_code": pid,

        # Name Keys
        "name": program_name,
        "program_name": program_name,
        "title": program_name,
        "major_name": program_name,

        # Institution Keys
        "faculty": faculty,
        "faculty_name": faculty,
        "university": university,
        "university_name": university,
        "u_name": university,
        "university_id": u_id,

        # Seat / Quota Keys
        "total_seats": total_seats,
        "seats": total_seats,
        "quota": total_seats,
        "r1": r1,
        "r2": r2,
        "r3": r3,
        "r4": r4,

        # Metric Keys
        "views": views,
        "view_count": views,
        "viewCount": views,
        "hits": views,
        "views_diff": 0,

        # Meta
        "category": category,
        "campus": campus,
        "program_type": program_type,
        "url": f"{BASE_URL}/programs/{pid}",
    }

async def main() -> None:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ttl_dns_cache=300, ssl=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info("🔍 [Stage 1] กำลังดึงฐานข้อมูลหลักสูตรจาก myTCAS S3 Catalog...")
        
        uni_json, courses_json = await asyncio.gather(
            fetch_json(session, S3_UNIVERSITIES_URL),
            fetch_json(session, S3_COURSES_URL)
        )

        uni_map = {}
        if isinstance(uni_json, list):
            for item in uni_json:
                uid = extract_val(item, "u_id", "university_id", "id")
                uname = extract_val(item, "u_name", "university_name", "name_th")
                if uid and uname:
                    uni_map[uid] = uname

        if not isinstance(courses_json, list) or len(courses_json) == 0:
            logger.error("❌ ไม่สามารถดึงรายการหลักสูตรจาก S3 ได้")
            return

        catalog = {}
        for item in courses_json:
            pid = extract_val(item, "id", "program_id", "code")
            if len(pid) >= 8:
                catalog[pid] = item
                uid = extract_val(item, "u_id", "university_id")
                uname = extract_val(item, "u_name", "university_name")
                if uid and uname and uid not in uni_map:
                    uni_map[uid] = uname

        total = len(catalog)
        pids = list(catalog.keys())
        logger.info(f"✅ โหลดสำเร็จ: มหาวิทยาลัย {len(uni_map)} แห่ง | หลักสูตรทั้งหมด {total:,} สาขา")

        # Probe Check: ทดสอบยิง 1 หน้าเพื่อดูสถานะ HTTP และการตอบกลับของเซิร์ฟเวอร์ myTCAS
        sample_pid = pids[0]
        sample_url = f"{BASE_URL}/programs/{sample_pid}"
        try:
            async with session.get(sample_url, headers=get_browser_headers(), timeout=aiohttp.ClientTimeout(total=10)) as resp:
                sample_text = await resp.text()
                logger.info(f"📡 [Probe Check] ยิงทดสอบ {sample_url} -> Status: HTTP {resp.status} (ขนาด {len(sample_text):,} ตัวอักษร)")
                if resp.status != 200:
                    logger.warning(f"⚠️ เซิร์ฟเวอร์ส่งรหัส {resp.status}: {sample_text[:180]}")
        except Exception as e:
            logger.warning(f"⚠️ Probe Exception: {e}")

        logger.info(f"🚀 [Stage 2] กำลังประมวลผลข้อมูลและดึงยอดวิว...")
        start_ts = time.time()
        results = []
        batch_size = 350

        for idx in range(0, total, batch_size):
            batch = pids[idx : idx + batch_size]
            async def process_one(pid):
                html = await fetch_program_html(session, f"{BASE_URL}/programs/{pid}", semaphore)
                return parse_program_data(pid, html, catalog[pid], uni_map)

            batch_res = await asyncio.gather(*(process_one(p) for p in batch))
            results.extend(batch_res)
            done = min(idx + batch_size, total)
            elapsed = time.time() - start_ts
            logger.info(f"ความคืบหน้า: {done:,}/{total:,} ({done/total*100:.1f}%) | {done/elapsed:.1f} สาขา/วินาที")

        # Stage 3: บันทึกและจัดอันดับ
        logger.info("📊 [Stage 3] กำลังจัดอันดับและบันทึกไฟล์...")
        results.sort(key=lambda x: (x["views"], x["total_seats"]), reverse=True)

        for i, item in enumerate(results, 1):
            item["rank"] = i

        now_th = datetime.now(TZ_TH)
        now_str = now_th.strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            # Meta ที่เข้ากันได้กับทุกแบบฟอร์มของ index.html
            "updated_at": now_str,
            "last_updated": now_str,
            "updatedAt": now_str,
            "timestamp": now_str,
            "metadata": {
                "updated_at": now_str,
                "last_updated": now_str,
                "updatedAt": now_str,
                "timestamp": now_str,
                "total_programs": len(results),
                "total_universities": len({p["university"] for p in results if p["university"] != "ไม่ระบุสถาบัน"}),
                "total_views": sum(p["views"] for p in results),
                "total_seats": sum(p["total_seats"] for p in results),
            },
            "programs": results,
            "data": results,
            "courses": results,
        }

        with OUTPUT_FILE.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

        SPLIT_DIR.mkdir(parents=True, exist_ok=True)
        by_uni = {}
        for p in results:
            by_uni.setdefault(p["university_id"], []).append(p)
        for uid, rows in by_uni.items():
            safe_id = re.sub(r"[^0-9A-Za-z_\-]", "_", uid)
            with (SPLIT_DIR / f"{safe_id}.json").open("w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, separators=(",", ":"))

        logger.info(f"🎉 เสร็จสิ้น! บันทึก {len(results):,} สาขาลง data.json เรียบร้อย")

if __name__ == "__main__":
    asyncio.run(main())
