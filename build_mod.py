import csv
import io
import random
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pathlib import Path

SE_DIR = Path(r"C:\Program Files (x86)\Steam\steamapps\common\SpaceEngine")
OUT_DIR = SE_DIR / "addons" / "WikipediaExtraSolar" / "locale"
OUT_FILE = OUT_DIR / "eng-db.cfg"

HEADERS = {
    "User-Agent": "SpaceEngineWikiModBuilder/4.0 (https://spaceengine.org; local-mod-script)"
}

ASTRO_KEYWORDS = {
    "star", "planet", "exoplanet", "galaxy", "nebula", "cluster",
    "constellation", "dwarf", "supernova", "pulsar", "quasar", "astronomy",
    "astronomical", "light-years", "parsec", "orbit", "binary", "giant"
}

STOP_SECTIONS = {
    "see also", "references", "external links", "further reading",
    "notes", "bibliography", "sources", "citations", "footnotes",
    "general references", "works cited", "cited works", "explanatory notes"
}

STOP_HEADING_PATTERN = re.compile(
    r'\b(references|sources|citations|footnotes|bibliography|external links|further reading|see also|works cited)\b',
    re.IGNORECASE
)

CITATION_LINE_PATTERN = re.compile(
    r'(\bBibcode:\s*\S+|\bdoi:\s*\S+|\barXiv:\s*\S+|\bISBN\s*[\d-]+)',
    re.IGNORECASE
)

NOISE_CATALOG_PATTERN = re.compile(
    r'^(TYC|2MASS|Gaia|USNO|UCAC|GSC|SDSS|WISE|PPM|SAO|BD|CD|CP)\s',
    re.IGNORECASE
)

MAX_CSV_APP_MAG = 7.5   # Keeps bright/named Hipparcos stars (e.g., Vega, Arcturus) while skipping ~110k faint survey stars
MAX_WORKERS = 3
EXIST_BATCH = 50
EXTRACT_BATCH = 20


class AIMDRateLimiter:
    """Thread-safe AIMD rate controller that learns the server's ceiling on 429."""
    def __init__(self, initial_rps: float = 2.2, min_rps: float = 0.5, max_rps: float = 3.2):
        self.rps = initial_rps
        self.min_rps = min_rps
        self.max_rps = max_rps
        self.lock = threading.Lock()
        self.next_allowed = time.monotonic()
        self.cooldown_until = 0.0

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                wait_until = max(self.next_allowed, self.cooldown_until)
                if now >= wait_until:
                    self.next_allowed = now + (1.0 / self.rps)
                    return
                sleep_for = wait_until - now
            time.sleep(sleep_for)

    def on_success(self) -> None:
        with self.lock:
            self.rps = min(self.max_rps, self.rps + 0.005)

    def on_throttle(self, retry_after: float) -> None:
        with self.lock:
            # Clamp max_rps below the rate that just triggered a 429 so it stops re-hitting the wall
            self.max_rps = max(self.min_rps, min(self.max_rps, self.rps * 0.85))
            self.rps = max(self.min_rps, self.rps * 0.5)
            jitter = random.uniform(0.2, 1.0)
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + retry_after + jitter)


_limiter = AIMDRateLimiter()
_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return _thread_local.session


def wiki_get(params: dict, retries: int = 4) -> dict:
    url = "https://en.wikipedia.org/w/api.php"
    session = get_session()
    for attempt in range(retries):
        _limiter.acquire()
        try:
            resp = session.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 3 * (attempt + 1)))
                _limiter.on_throttle(retry_after)
                continue
            resp.raise_for_status()
            _limiter.on_success()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                sys.stderr.write(f"\n[Warning] Request failed after {retries} attempts: {exc}\n")
                return {}
            time.sleep(1.5 * (attempt + 1))
    return {}


def print_progress(prefix: str, current: int, total: int, extra: str = "") -> None:
    pct = (current / total * 100) if total > 0 else 100.0
    line = f"\r{prefix}: {pct:6.2f}% ({current:,}/{total:,}) {extra}"
    sys.stdout.write(line.ljust(95))
    sys.stdout.flush()


def get_extrasolar_objects(se_dir: Path) -> list[dict]:
    objects = []
    seen_primaries = set()
    catalogs_dir = se_dir / "data" / "catalogs"

    def add_entry(raw_name_field: str, parent: str | None = None):
        raw_names = [n.strip().replace("|", "") for n in raw_name_field.split("/") if n.strip()]
        if not raw_names:
            return
        primary_name = raw_names[0]
        if primary_name in seen_primaries or parent == "Sol":
            return

        filtered_aliases = [n for n in raw_names if not NOISE_CATALOG_PATTERN.match(n)]
        if not filtered_aliases:
            return

        seen_primaries.add(primary_name)
        objects.append({
            "primary": primary_name,
            "aliases": filtered_aliases,
            "parent": parent
        })

    for pak_path in catalogs_dir.glob("*.pak"):
        with zipfile.ZipFile(pak_path, "r") as zf:
            for file_name in zf.namelist():
                if "solarsys" in file_name.lower():
                    continue

                if file_name.endswith(".sc"):
                    content = zf.read(file_name).decode("utf-8", errors="ignore")
                    entries = re.finditer(
                        r'(StarBarycenter|Star|Planet|Galaxy|Nebula|Cluster)\s+"([^"]+)"\s*\{([^}]*)\}',
                        content,
                        re.DOTALL
                    )
                    for match in entries:
                        body = match.group(3)
                        parent_match = re.search(r'ParentBody\s+"([^"]+)"', body)
                        parent = parent_match.group(1).strip() if parent_match else None
                        add_entry(match.group(2), parent)

                elif file_name.endswith(".csv"):
                    content = zf.read(file_name).decode("utf-8", errors="ignore")
                    reader = csv.DictReader(io.StringIO(content))
                    if reader.fieldnames and "Name" in reader.fieldnames:
                        has_mag = "AppMag" in reader.fieldnames
                        for row in reader:
                            name_val = (row.get("Name") or "").strip()
                            if not name_val or name_val == "Sol":
                                continue
                            if has_mag:
                                mag_str = (row.get("AppMag") or "").strip()
                                if mag_str:
                                    try:
                                        if float(mag_str) > MAX_CSV_APP_MAG:
                                            continue
                                    except ValueError:
                                        pass
                            add_entry(name_val, None)

    return objects


def resolve_redirects(title: str, norm_map: dict[str, str], redir_map: dict[str, str]) -> str:
    resolved = title
    for _ in range(5):
        nxt = norm_map.get(resolved, resolved)
        nxt = redir_map.get(nxt, nxt)
        if nxt == resolved:
            break
        resolved = nxt
    return resolved


def check_existence_chunk(chunk: list[tuple[int, str]]) -> list[tuple[int, str]]:
    titles_param = "|".join(dict.fromkeys(title for _, title in chunk))
    data = wiki_get({
        "action": "query",
        "format": "json",
        "prop": "pageprops",
        "ppprop": "disambiguation",
        "redirects": 1,
        "titles": titles_param
    })
    q = data.get("query", {})
    if not q:
        return []

    norm_map = {n["from"]: n["to"] for n in q.get("normalized", [])}
    redir_map = {r["from"]: r["to"] for r in q.get("redirects", [])}

    existing_titles = {
        pdata["title"]
        for pid, pdata in q.get("pages", {}).items()
        if pid != "-1" and "missing" not in pdata and "disambiguation" not in pdata.get("pageprops", {})
    }

    hits = []
    for idx, orig_title in chunk:
        resolved = resolve_redirects(orig_title, norm_map, redir_map)
        if resolved in existing_titles:
            hits.append((idx, resolved))
    return hits


def verify_astro_chunk(chunk: list[tuple[int, str]]) -> list[tuple[int, str]]:
    titles_param = "|".join(dict.fromkeys(title for _, title in chunk))
    data = wiki_get({
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "exintro": True,
        "exlimit": EXTRACT_BATCH,
        "explaintext": True,
        "redirects": 1,
        "titles": titles_param
    })
    q = data.get("query", {})
    if not q:
        return []

    norm_map = {n["from"]: n["to"] for n in q.get("normalized", [])}
    redir_map = {r["from"]: r["to"] for r in q.get("redirects", [])}

    valid_titles = set()
    for pid, pdata in q.get("pages", {}).items():
        if pid == "-1":
            continue
        extract = pdata.get("extract", "").lower()
        if extract and any(kw in extract for kw in ASTRO_KEYWORDS):
            valid_titles.add(pdata["title"])

    verified = []
    for idx, resolved_title in chunk:
        final_title = resolve_redirects(resolved_title, norm_map, redir_map)
        if final_title in valid_titles:
            verified.append((idx, final_title))
    return verified


def fetch_and_format_one(item: tuple[dict, str]) -> tuple[dict, str | None]:
    obj, canonical_title = item
    data = wiki_get({
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": True,
        "titles": canonical_title
    })
    for pid, pdata in data.get("query", {}).get("pages", {}).items():
        if pid != "-1":
            raw_text = pdata.get("extract", "").strip()
            if raw_text:
                return obj, format_se_article(raw_text)
    return obj, None


def format_se_article(raw_text: str) -> str:
    text = re.sub(r'\[[^\]]*\]', '', raw_text)
    text = re.sub(r'(^|[\s(\[—–-])"', r'\1“', text)
    text = text.replace('"', '”')

    formatted_blocks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        header_match = re.match(r'^(={2,})\s*(.*?)\s*\1$', line)
        if header_match:
            level = len(header_match.group(1))
            heading_title = header_match.group(2).strip()
            if heading_title.lower() in STOP_SECTIONS or STOP_HEADING_PATTERN.search(heading_title):
                break
            if level == 2:
                formatted_blocks.append(f"[BIG]{heading_title}[/BIG]")
            else:
                formatted_blocks.append(heading_title.upper())
        else:
            if CITATION_LINE_PATTERN.search(line):
                continue
            clean_para = re.sub(r'\s{2,}', ' ', line)
            formatted_blocks.append(clean_para)

    return "\n\n".join(formatted_blocks)


def build_mod():
    start_time = time.monotonic()
    print("Scanning extrasolar .sc and .csv catalogs...")
    candidates = get_extrasolar_objects(SE_DIR)
    print(f"Found {len(candidates):,} high-probability extrasolar candidates.\n")

    query_queue = [(idx, alias) for idx, obj in enumerate(candidates) for alias in obj["aliases"]]
    exist_chunks = [query_queue[i:i + EXIST_BATCH] for i in range(0, len(query_queue), EXIST_BATCH)]

    # Pass 1A: Existence + redirect resolution (50 titles/batch)
    raw_hits = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(check_existence_chunk, c) for c in exist_chunks]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            raw_hits.extend(fut.result())
            print_progress("Pass 1A (Batches)", done_count, len(exist_chunks),
                           f"| Hits: {len(raw_hits):,} | Rate: {_limiter.rps:.1f} req/s")
    print()

    dedup_hits = {}
    for idx, resolved_title in raw_hits:
        if idx not in dedup_hits:
            dedup_hits[idx] = resolved_title
    hit_items = list(dedup_hits.items())
    verify_chunks = [hit_items[i:i + EXTRACT_BATCH] for i in range(0, len(hit_items), EXTRACT_BATCH)]

    # Pass 1B: Astronomy keyword verification (20 titles/batch)
    confirmed_matches = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(verify_astro_chunk, c) for c in verify_chunks]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            for idx, resolved_title in fut.result():
                confirmed_matches.append((candidates[idx], resolved_title))
            print_progress("Pass 1B (Batches)", done_count, len(verify_chunks),
                           f"| Verified: {len(confirmed_matches):,} | Rate: {_limiter.rps:.1f} req/s")
    print(f"\n\nDownloading full articles for {len(confirmed_matches):,} verified objects...\n")

    # Pass 2: Full-article download and formatting
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    added_count = 0
    with open(OUT_FILE, "w", encoding="utf-8") as out, ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        out.write("// Auto-generated full Wikipedia articles for extrasolar objects\n\n")
        futures = [pool.submit(fetch_and_format_one, item) for item in confirmed_matches]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            obj, clean_descr = fut.result()
            if clean_descr:
                out.write("RObject\n{\n")
                out.write(f'    Name    "{obj["primary"]}"\n')
                out.write(f'    LocName "{obj["primary"]}"\n')
                if obj["parent"]:
                    out.write(f'    Parent  "{obj["parent"]}"\n')
                out.write(f'    Descr   "{clean_descr}"\n')
                out.write("}\n\n")
                added_count += 1
            print_progress("Pass 2 (Objects)", done_count, len(confirmed_matches),
                           f"| Saved: {added_count:,} | Rate: {_limiter.rps:.1f} req/s")

    elapsed = time.monotonic() - start_time
    print(f"\n\nDone in {elapsed:.1f}s. Wrote {added_count:,} entries to {OUT_FILE}")


if __name__ == "__main__":
    build_mod()