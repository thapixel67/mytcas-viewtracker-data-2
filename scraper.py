#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
myTCAS ViewTracker - 100% Real Data Scraper & JS Bundle Endpoint Extractor
"""

import asyncio
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from urllib.parse import urljoin

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
S3_BASE_URL = "https://my-tcas.s3.ap-southeast-1.amazonaws.com/mytcas"
S3_COURSES_URL = f"{S3_BASE_URL}/courses.json"
S3_UNIVERSITIES_URL = f"{S3_BASE_URL}/universities.json"

MAX_CONCURRENT_REQUESTS = 40
REQUEST_TIMEOUT_SEC = 15
SOCK_READ_TIMEOUT = 10
OUTPUT_FILE = Path("data.json")
SPLIT_DIR = Path("data/universities")

# รหัสสาขาเภสัชกรรมอุตสาหการ จุฬาฯ สำหรับใช้ตรวจสอบความถูกต้องกับหน้าเว็บจริง (รับ 50+12=62 คน, ยอดวิว ~23,865)
TEST_PROGRAM_ID = "10010133130201A"

def get_headers() -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json,text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://course.mytcas.com/",
        "Origin": "https://course.mytcas.com",
        "Connection": "keep-alive",
    }

def classify_faculty_group(faculty: str, program_name: str) -> str:
    text = f"{faculty} {program_name}".lower()
    if any(k in text for k in ["แพทยศาสตร", "ทันตแพทย", "เภสัชศาสตร", "สัตวแพทย"]):
        return "แพทย์ / ทันตะ / เภสัช / สัตวแพทย์"
    if any(k in text for k in ["พยาบาล", "สหเวช", "เทคนิคการแพทย์", "สาธารณสุข", "กายภาพบำบัด", "รังสี"]):
        return "พยาบาล / สหเวช / สุขภาพ"
    if any(k in text for k in ["คอมพิวเตอร์", "ดิจิทัล", "สารสนเทศ", "ปัญญาประดิษฐ์", "ข้อมูล", "ไอที", "ซอฟต์แวร์", "ai", "data", "cyber"]):
        return "วิทยาการคอม / IT / ดิจิทัล"
    if any(k in text for k in ["วิศวกรรม", "ช่างอุตสาหกรรม"]):
        return "วิศวกรรมศาสตร์"
    if any(k in text for k in ["บริหารธุรกิจ", "บัญชี", "พาณิชยศาสตร์", "การตลาด", "การจัดการ", "โลจิสติกส์", "การเงิน"]):
        return "บริหารธุรกิจ / บัญชี"
    if "เศรษฐศาสตร" in text:
        return "เศรษฐศาสตร์"
    if any(k in text for k in ["นิติศาสตร", "กฎหมาย", "รัฐศาสตร", "การปกครอง", "ความสัมพันธ์ระหว่างประเทศ"]):
        return "นิติศาสตร์ / รัฐศาสตร์"
    if any(k in text for k in ["อักษรศาสตร", "ศิลปศาสตร", "มนุษยศาสตร", "ภาษา"]):
        return "มนุษยศาสตร์ / ศิลปศาสตร์ / ภาษา"
    if any(k in text for k in ["ครุศาสตร", "ศึกษาศาสตร", "การสอน", "ครู"]):
        return "ครุศาสตร์ / ศึกษาศาสตร์"
    if any(k in text for k in ["สถาปัตยกรรม", "ศิลปกรรม", "นิเทศศาสตร", "วารสาร", "สื่อสารมวลชน", "การออกแบบ"]):
        return "สถาปัตย์ / ศิลปกรรม / นิเทศศาสตร์"
    if any(k in text for k in ["เกษตร", "ประมง", "วนศาสตร", "อุตสาหกรรมเกษตร", "อาหาร"]):
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

async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: Optional[asyncio.Semaphore] = None) -> Optional[Any]:
    timeout = aiohttp.ClientTimeout(total=20, sock_read=12)
    try:
        if semaphore:
            async with semaphore:
                async with session.get(url, headers=get_headers(), timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
        else:
            async with session.get(url, headers=get_headers(), timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
    except Exception:
        pass
    return None

async def fetch_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    timeout = aiohttp.ClientTimeout(total=15, sock_read=10)
    try:
        async with session.get(url, headers=get_headers(), timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.text(errors="replace")
    except Exception:
        pass
    return None

async def inspect_spa_bundles(session: aiohttp.ClientSession) -> Dict[str, Any]:
    """
    ดาวน์โหลดและแกะไฟล์ JavaScript (.js) ของเว็บ course.mytcas.com
    เพื่อค้นหา Endpoint จริงที่ใช้ดึง 'ยอดผู้เข้าชม' และข้อมูล 'รอบ 1-4'
    """
    logger.info("🔎 [JS Inspector] กำลังแกะไฟล์ JavaScript ของเว็บ course.mytcas.com เพื่อหา API ยอดผู้เข้าชมจริง...")
    html = await fetch_text(session, f"{BASE_URL}/programs/{TEST_PROGRAM_ID}")
    discovered_urls: Set[str] = set()
    discovered_json_names: Set[str] = set()

    if not html:
        logger.warning("⚠️ ไม่สามารถโหลดหน้า SPA Shell ได้")
        return {"urls": [], "json_names": []}

    soup = BeautifulSoup(html, "html.parser")
    script_urls: Set[str] = set()
    for s in soup.find_all("script", src=True):
        script_urls.add(urljoin(BASE_URL, s["src"]))
    for link in soup.find_all("link", href=True):
        if link["href"].endswith(".js"):
            script_urls.add(urljoin(BASE_URL, link["href"]))

    logger.info(f"📦 พบไฟล์ Script หลักใน HTML จำนวน {len(script_urls)} ไฟล์: {list(script_urls)}")

    # ดาวน์โหลดไฟล์ JS หลัก และค้นหา Chunk ย่อยทั้งหมด
    all_js_contents: Dict[str, str] = {}
    chunk_urls: Set[str] = set()

    for s_url in script_urls:
        js_code = await fetch_text(session, s_url)
        if js_code:
            all_js_contents[s_url] = js_code
            # ค้นหาไฟล์ .js ย่อยที่ถูกอ้างอิงใน Bundle (Vite / Webpack chunks)
            for m in re.finditer(r'["\']([0-9A-Za-z_/\-\.]+\.js)["\']', js_code):
                path = m.group(1)
                if not path.startswith("http"):
                    if path.startswith("/"):
                        chunk_urls.add(urljoin(BASE_URL, path))
                    else:
                        base_dir = s_url.rsplit("/", 1)[0] + "/"
                        chunk_urls.add(urljoin(base_dir, path))

    # ดาวน์โหลด Chunk ย่อยเพิ่มเติม (จำกัดสูงสุด 25 ไฟล์)
    extra_chunks = sorted(chunk_urls - set(all_js_contents.keys()))[:25]
    if extra_chunks:
        logger.info(f"📦 กำลังสแกน JS Chunks ย่อยเพิ่มเติม {len(extra_chunks)} ไฟล์...")
        chunk_texts = await asyncio.gather(*(fetch_text(session, u) for u in extra_chunks))
        for u, txt in zip(extra_chunks, chunk_texts):
            if txt:
                all_js_contents[u] = txt

    # สแกนเนื้อหา JS ทุกไฟล์เพื่อหา URL, ไฟล์ .json และโค้ดส่วนที่แสดงคำว่า "ยอดผู้เข้าชม"
    view_keywords = ["ยอดผู้เข้าชม", "\\u0e22\\u0e2d\\u0e14\\u0e1c\\u0e39\\u0e49\\u0e40\\u0e02\\u0e49\\u0e32\\u0e0a\\u0e21", "page_views", "view_count", "viewCount"]
    for js_url, code in all_js_contents.items():
        for m in re.finditer(r'https?://[^\s"\'`<>\\]+', code):
            u = m.group(0).rstrip(".,;)]}")
            if not any(x in u for x in ["w3.org", "reactjs", "vuejs", "mozilla", "google.com/chrome", "reg.chula"]):
                discovered_urls.add(u)

        for m in re.finditer(r'["\'/`]([0-9A-Za-z_\-/]+\.json)["\'`]', code):
            discovered_json_names.add(m.group(1).lstrip("/"))

        for kw in view_keywords:
            idx = code.find(kw)
            if idx != -1:
                snippet = code[max(0, idx - 250) : min(len(code), idx + 250)]
                logger.info(f"🎯 พบโค้ด '{kw}' ใน {js_url.split('/')[-1]}:\n>>> {snippet} <<<")

    logger.info(f"🌐 URL ที่พบใน JS Bundle: {sorted(discovered_urls)}")
    logger.info(f"📄 ไฟล์ .json ที่พบใน JS Bundle: {sorted(discovered_json_names)}")

    return {
        "urls": sorted(discovered_urls),
        "json_names": sorted(discovered_json_names),
        "js_contents": all_js_contents,
    }

async def probe_program_endpoints(session: aiohttp.ClientSession, pid: str, discovered_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    ทดสอบดึงข้อมูลรายสาขา (เช่น 10010133130201A) จาก S3 และ API ที่ค้นพบใน JS Bundle
    """
    candidates = [
        f"{S3_BASE_URL}/programs/{pid}.json",
        f"{S3_BASE_URL}/courses/{pid}.json",
        f"{S3_BASE_URL}/rounds/{pid}.json",
        f"{S3_BASE_URL}/{pid}.json",
        f"{S3_BASE_URL}/rounds.json",
        f"{S3_BASE_URL}/views.json",
        f"{S3_BASE_URL}/stats.json",
    ]
    for jname in discovered_info.get("json_names", []):
        candidates.append(f"{S3_BASE_URL}/{jname}")
        candidates.append(f"{BASE_URL}/{jname}")

    for u in discovered_info.get("urls", []):
        if "amazonaws.com" in u or "mytcas" in u:
            candidates.append(f"{u.rstrip('/')}/{pid}.json")
            candidates.append(f"{u.rstrip('/')}/programs/{pid}.json")
            candidates.append(f"{u.rstrip('/')}/programs/{pid}")

    # ตัดรายการซ้ำ
    unique_candidates = list(dict.fromkeys(candidates))
    logger.info(f"🧪 กำลังทดสอบยิง Endpoint สำหรับรหัส {pid} จำนวน {len(unique_candidates)} รายการ...")

    found_endpoints = {}
    for url in unique_candidates:
        data = await fetch_json(session, url)
        if data is not None:
            sample_str = json.dumps(data, ensure_ascii=False)[:350]
            logger.info(f"✅ [HTTP 200 JSON] พบข้อมูลที่ {url} -> ตัวอย่าง: {sample_str}")
            found_endpoints[url] = data

    return found_endpoints

def extract_seats_and_views_from_json(obj: Any) -> Dict[str, int]:
    """แกะจำนวนรับ รอบ 1-4 และยอดผู้เข้าชมจากโครงสร้าง JSON ของสาขา"""
    r1 = r2 = r3 = r4 = total_seats = views = 0

    if isinstance(obj, dict):
        # ค้นหายอดวิวจากทุกคีย์ที่เป็นไปได้
        for k in ["views", "view_count", "viewCount", "page_views", "pageViews", "hits", "count", "total_views", "number_views"]:
            val = obj.get(k)
            if isinstance(val, (int, float)) and val > 0:
                views = int(val)
                break
            elif isinstance(val, str) and val.replace(",", "").isdigit():
                views = int(val.replace(",", ""))
                break

        # หากมีรายการรอบรับสมัครอยู่ในฟิลด์ย่อย หรือตัวอ็อบเจกต์เอง
        rounds_list = obj.get("rounds") or obj.get("admission_rounds") or obj.get("quotas") or obj.get("data")
        if isinstance(rounds_list, list):
            for r in rounds_list:
                if not isinstance(r, dict):
                    continue
                r_type = str(r.get("type") or r.get("round_type") or r.get("round_id") or r.get("round") or "")
                seats = int(r.get("receive_student_number") or r.get("receive_seats") or r.get("quota") or r.get("seats") or 0)
                if r_type.startswith("1") or "portfolio" in r_type.lower():
                    r1 += seats
                elif r_type.startswith("2") or "quota" in r_type.lower():
                    r2 += seats
                elif r_type.startswith("3") or "admission" in r_type.lower():
                    r3 += seats
                elif r_type.startswith("4") or "direct" in r_type.lower():
                    r4 += seats

    elif isinstance(obj, list):
        for r in obj:
            if not isinstance(r, dict):
                continue
            r_type = str(r.get("type") or r.get("round_type") or r.get("round_id") or r.get("round") or "")
            seats = int(r.get("receive_student_number") or r.get("receive_seats") or r.get("quota") or r.get("seats") or 0)
            if r_type.startswith("1") or "portfolio" in r_type.lower():
                r1 += seats
            elif r_type.startswith("2") or "quota" in r_type.lower():
                r2 += seats
            elif r_type.startswith("3") or "admission" in r_type.lower():
                r3 += seats
            elif r_type.startswith("4") or "direct" in r_type.lower():
                r4 += seats

            for k in ["views", "view_count", "viewCount", "page_views", "hits"]:
                if r.get(k) and int(str(r[k]).replace(",", "")) > views:
                    views = int(str(r[k]).replace(",", ""))

    total_seats = r1 + r2 + r3 + r4
    return {"r1": r1, "r2": r2, "r3": r3, "r4": r4, "total_seats": total_seats, "views": views}

async def main() -> None:
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ttl_dns_cache=300, ssl=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:
        # 1. สแกน JS Bundle ของเว็บเพื่อค้นหา Endpoint จริง
        discovered = await inspect_spa_bundles(session)
        found_test_data = await probe_program_endpoints(session, TEST_PROGRAM_ID, discovered)

        # 2. โหลด S3 Catalog หลัก (universities.json + courses.json)
        logger.info("🔍 [Stage 1] กำลังดึงฐานข้อมูลหลักสูตรจาก myTCAS S3 Catalog...")
        uni_json, courses_json = await asyncio.gather(
            fetch_json(session, S3_UNIVERSITIES_URL),
            fetch_json(session, S3_COURSES_URL)
        )

        uni_map = {}
        if isinstance(uni_json, list):
            for item in uni_json:
                uid = extract_val(item, "u_id", "university_id", "id")
                uname = extract_val(item, "u_name", "university_name", "university_name_th", "name_th")
                if uid and uname:
                    uni_map[uid] = uname

        if not isinstance(courses_json, list) or not courses_json:
            logger.error("❌ ไม่สามารถดึงรายการหลักสูตรจาก S3 ได้")
            return

        # แสดงตัวอย่างข้อมูลจริงของเภสัชฯ จุฬาฯ (10010133130201A) ใน courses.json
        for c in courses_json:
            if extract_val(c, "program_id", "id") == TEST_PROGRAM_ID:
                logger.info(f"📋 [Debug ข้อมูลจริงใน courses.json ของ {TEST_PROGRAM_ID}]:\n{json.dumps(c, ensure_ascii=False)}")
                break

        # ตรวจสอบว่ามีไฟล์รวมรอบรับสมัคร (เช่น rounds.json) หรือไฟล์รายสาขาหรือไม่
        global_rounds_map: Dict[str, List[Dict]] = {}
        per_program_url_template: Optional[str] = None

        for url, data in found_test_data.items():
            if url.endswith(f"{TEST_PROGRAM_ID}.json") or url.endswith(f"/{TEST_PROGRAM_ID}"):
                per_program_url_template = url.replace(TEST_PROGRAM_ID, "{pid}")
                logger.info(f"🎯 พบ Endpoint รายสาขา: {per_program_url_template}")
            elif isinstance(data, list) and len(data) > 100 and url != S3_COURSES_URL and url != S3_UNIVERSITIES_URL:
                for row in data:
                    if isinstance(row, dict):
                        p = extract_val(row, "program_id", "id")
                        if p:
                            global_rounds_map.setdefault(p, []).append(row)

        catalog = {}
        for item in courses_json:
            pid = extract_val(item, "program_id", "id", "code")
            if len(pid) >= 8:
                catalog[pid] = item

        total = len(catalog)
        pids = list(catalog.keys())
        logger.info(f"🚀 [Stage 2] กำลังประมวลผลข้อมูลจริงทั้ง {total:,} หลักสูตร (ปิดระบบสุ่มตัวเลข 100%)...")

        results = []
        start_ts = time.time()

        if per_program_url_template:
            batch_size = 400
            for idx in range(0, total, batch_size):
                batch = pids[idx : idx + batch_size]
                async def fetch_detail(pid: str):
                    url = per_program_url_template.format(pid=pid)
                    detail_json = await fetch_json(session, url, semaphore)
                    return pid, detail_json
                batch_details = await asyncio.gather(*(fetch_detail(p) for p in batch))
                for pid, detail_json in batch_details:
                    raw = catalog[pid]
                    metrics = extract_seats_and_views_from_json(detail_json) if detail_json else {"r1": 0, "r2": 0, "r3": 0, "r4": 0, "total_seats": 0, "views": 0}
                    results.append(build_program_record(pid, raw, uni_map, metrics))
                done = min(idx + batch_size, total)
                logger.info(f"ความคืบหน้า: {done:,}/{total:,} ({done/total*100:.1f}%)")
        else:
            for pid in pids:
                raw = catalog[pid]
                rounds_data = global_rounds_map.get(pid, [])
                metrics = extract_seats_and_views_from_json(rounds_data) if rounds_data else {"r1": 0, "r2": 0, "r3": 0, "r4": 0, "total_seats": 0, "views": 0}
                results.append(build_program_record(pid, raw, uni_map, metrics))

        # Stage 3: จัดอันดับและบันทึก
        results.sort(key=lambda x: (x["views"], x["total_seats"]), reverse=True)
        for i, item in enumerate(results, 1):
            item["rank"] = i

        now_str = datetime.now(TZ_TH).strftime("%Y-%m-%d %H:%M:%S")
        total_views = sum(p["views"] for p in results)
        max_views = results[0]["views"] if results else 0
        total_seats = sum(p["total_seats"] for p in results)

        payload = {
            "updated_at": now_str,
            "last_updated": now_str,
            "updatedAt": now_str,
            "timestamp": now_str,
            "total_programs": len(results),
            "total_universities": len({p["university"] for p in results if p["university"] != "ไม่ระบุสถาบัน"}),
            "total_views": total_views,
            "max_views": max_views,
            "total_seats": total_seats,
            "metadata": {
                "updated_at": now_str,
                "last_updated": now_str,
                "total_programs": len(results),
                "total_universities": len({p["university"] for p in results if p["university"] != "ไม่ระบุสถาบัน"}),
                "total_views": total_views,
                "max_views": max_views,
                "total_seats": total_seats,
            },
            "programs": results,
            "data": results,
            "courses": results,
        }

        with OUTPUT_FILE.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

        logger.info(f"🎉 เสร็จสิ้น! บันทึก {len(results):,} สาขา | ยอดวิวรวม {total_views:,} ครั้ง | ที่นั่งรวม {total_seats:,} ที่นั่ง")

def build_program_record(pid: str, raw: Dict, uni_map: Dict[str, str], metrics: Dict[str, int]) -> Dict[str, Any]:
    u_id = extract_val(raw, "university_id", "u_id") or pid[:3]
    university = extract_val(raw, "university_name_th", "u_name", "university_name") or uni_map.get(u_id, "ไม่ระบุสถาบัน")
    faculty = extract_val(raw, "faculty_name_th", "faculty_name", "faculty") or "ไม่ระบุคณะ"

    prog_title = extract_val(raw, "program_name_th", "program_name", "name_th")
    field_title = extract_val(raw, "field_name_th", "field_name", "major_name_th")
    if field_title and prog_title and field_title not in prog_title:
        program_name = f"{prog_title} สาขาวิชา{field_title}"
    else:
        program_name = prog_title or field_title or f"สาขาวิชา ({pid})"

    # จำนวนรับจาก metrics หรือ fallback จาก major_acceptance_number / number_acceptance_mko2 ใน courses.json
    total_seats = metrics["total_seats"]
    if total_seats == 0:
        for k in ["major_acceptance_number", "number_acceptance_mko2"]:
            v = raw.get(k)
            if isinstance(v, (int, float)) and v > 0:
                total_seats = int(v)
                break
            elif isinstance(v, str) and v.replace(",", "").isdigit():
                total_seats = int(v.replace(",", ""))
                break

    views = metrics["views"]
    category = classify_faculty_group(faculty, program_name)

    return {
        "id": pid,
        "program_id": pid,
        "code": pid,
        "name": program_name,
        "program_name": program_name,
        "title": program_name,
        "faculty": faculty,
        "faculty_name": faculty,
        "university": university,
        "university_name": university,
        "university_id": u_id,
        "total_seats": total_seats,
        "seats": total_seats,
        "quota": total_seats,
        "r1": metrics["r1"],
        "r2": metrics["r2"],
        "r3": metrics["r3"],
        "r4": metrics["r4"],
        "views": views,
        "view_count": views,
        "viewCount": views,
        "views_diff": 0,
        "category": category,
        "campus": extract_val(raw, "campus_name_th", "campus_name") or "วิทยาเขตหลัก",
        "program_type": extract_val(raw, "program_type_name_th", "program_type") or "ภาษาไทย ปกติ",
        "url": f"{BASE_URL}/programs/{pid}",
    }

if __name__ == "__main__":
    asyncio.run(main())
