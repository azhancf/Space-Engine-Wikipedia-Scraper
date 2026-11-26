import csv
import io
import random
import re
import sys
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pathlib import Path

SE_DIR = Path(r"C:\Program Files (x86)\Steam\steamapps\common\SpaceEngine")
OUT_DIR = SE_DIR / "addons" / "WikipediaExtraSolar" / "locale"
OUT_FILE = OUT_DIR / "eng-db.cfg"

HEADERS = {
    "User-Agent": "SpaceEngineWikiModBuilder/5.1 (https://spaceengine.org; local-mod-script)"
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

CATALOG_DESIGNATION_PATTERN = re.compile(
    r'^(HIP|HD|HR|GJ|Gliese|Wolf|Ross|Luyten|LHS|LP|Lalande|Lacaille|Struve|STT|STF|'
    r'TYC|2MASS|Gaia|USNO|UCAC|GSC|SDSS|WISE|PPM|SAO|BD|CD|CP|ADS|CCDM|WDS)\b',
    re.IGNORECASE
)

NOTABLE_STAR_PATTERN = re.compile(
    r'^(GJ|Gliese|Wolf|Ross|Lalande|Lacaille|Luyten|Groombridge|Kruger|Struve|Proxima|Barnard|Kapteyn)\b',
    re.IGNORECASE
)

MAX_CSV_APP_MAG = 7.5
MAX_NEARBY_DIST_PC = 25.0
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


def get_existing_entries(cfg_path: Path) -> dict[str, str]:
    """Reads eng-db.cfg and returns {Name: Descr} for all already-documented objects."""
    entries = {}
    if not cfg_path.exists():
        return entries

    current_name = None
    in_descr = False
    descr_lines = []

    with open(cfg_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.rstrip("\r\n")
            stripped = line.strip()

            if in_descr:
                if stripped.endswith('"'):
                    descr_lines.append(stripped[:-1])
                    if current_name:
                        entries[current_name] = "\n".join(descr_lines)
                    in_descr = False
                    descr_lines = []
                else:
                    descr_lines.append(line)
                continue

            name_match = re.match(r'^Name\s+"([^"]+)"', stripped)
            if name_match:
                current_name = name_match.group(1)
                continue

            descr_match = re.match(r'^Descr\s+"(.*)$', stripped)
            if descr_match:
                rest = descr_match.group(1)
                if rest.endswith('"'):
                    if current_name:
                        entries[current_name] = rest[:-1]
                else:
                    in_descr = True
                    descr_lines = [rest]

    return entries


def is_notable_csv_star(aliases: list[str]) -> bool:
    for alias in aliases:
        if NOTABLE_STAR_PATTERN.match(alias):
            return True
        if not CATALOG_DESIGNATION_PATTERN.match(alias) and not NOISE_CATALOG_PATTERN.match(alias):
            return True
    return False


def get_extrasolar_objects(se_dir: Path) -> list[dict]:
    objects = []
    seen_primaries = set()
    catalogs_dir = se_dir / "data" / "catalogs"

    def add_entry(raw_name_field: str, obj_type: str, parent: str | None = None):
        raw_names = [n.strip().replace("|", "") for n in raw_name_field.split("/") if n.strip()]
        if not raw_names:
            return
        primary_name = raw_names[0]
        if primary_name in seen_primaries or parent == "Sol":
            return

        filtered_aliases = [n for n in raw_names if not NOISE_CATALOG_PATTERN.match(n)]
        if not filtered_aliases:
            return

        enriched_aliases = list(filtered_aliases)
        if parent and obj_type in {"Star", "StarBarycenter"}:
            parent_primary = parent.split("/")[0].strip()
            parent_words = parent_primary.split()
            if len(parent_words) >= 2:
                constellation_or_suffix = parent_words[-1]
                for alias in filtered_aliases:
                    if " " not in alias and not CATALOG_DESIGNATION_PATTERN.match(alias):
                        combined = f"{alias} {constellation_or_suffix}"
                        if combined not in enriched_aliases:
                            enriched_aliases.append(combined)

        seen_primaries.add(primary_name)
        objects.append({
            "primary": primary_name,
            "aliases": enriched_aliases,
            "parent": parent,
            "type": obj_type
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
                        obj_type = match.group(1)
                        body = match.group(3)
                        parent_match = re.search(r'ParentBody\s+"([^"]+)"', body)
                        parent = parent_match.group(1).strip() if parent_match else None
                        add_entry(match.group(2), obj_type, parent)

                elif file_name.endswith(".csv"):
                    content = zf.read(file_name).decode("utf-8", errors="ignore")
                    reader = csv.DictReader(io.StringIO(content))
                    if reader.fieldnames and "Name" in reader.fieldnames:
                        has_mag = "AppMag" in reader.fieldnames
                        has_dist = "Dist" in reader.fieldnames
                        for row in reader:
                            name_val = (row.get("Name") or "").strip()
                            if not name_val or name_val == "Sol":
                                continue
                            raw_aliases = [n.strip().replace("|", "") for n in name_val.split("/") if n.strip()]

                            if not is_notable_csv_star(raw_aliases):
                                is_nearby = False
                                if has_dist and (row.get("Dist") or "").strip():
                                    try:
                                        if float(row["Dist"]) <= MAX_NEARBY_DIST_PC:
                                            is_nearby = True
                                    except ValueError:
                                        pass

                                if not is_nearby and has_mag and (row.get("AppMag") or "").strip():
                                    try:
                                        if float(row["AppMag"]) > MAX_CSV_APP_MAG:
                                            continue
                                    except ValueError:
                                        pass

                            add_entry(name_val, "Star", None)

    return objects


def build_star_systems(objects: list[dict]) -> dict[int, list[int]]:
    """Groups Star and StarBarycenter indices by their top-level stellar system root."""
    name_to_idx = {}
    for idx, obj in enumerate(objects):
        name_to_idx[obj["primary"]] = idx
        for alias in obj["aliases"]:
            if alias not in name_to_idx:
                name_to_idx[alias] = idx

    systems = defaultdict(list)
    for idx, obj in enumerate(objects):
        if obj["type"] not in {"Star", "StarBarycenter"}:
            continue

        root_idx = idx
        visited = {idx}
        curr = obj
        while curr["parent"]:
            parent_primary = curr["parent"].split("/")[0].strip()
            p_idx = name_to_idx.get(parent_primary)
            if p_idx is None or p_idx in visited:
                break
            parent_obj = objects[p_idx]
            if parent_obj["type"] not in {"Star", "StarBarycenter"}:
                break
            visited.add(p_idx)
            root_idx = p_idx
            curr = parent_obj

        systems[root_idx].append(idx)

    return systems


def write_robject(out, obj: dict, clean_descr: str) -> None:
    out.write("RObject\n{\n")
    out.write(f'    Name    "{obj["primary"]}"\n')
    out.write(f'    LocName "{obj["primary"]}"\n')
    if obj["parent"]:
        out.write(f'    Parent  "{obj["parent"]}"\n')
    out.write(f'    Descr   "{clean_descr}"\n')
    out.write("}\n\n")


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


def fetch_and_format_title(canonical_title: str) -> tuple[str, str | None]:
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
                return canonical_title, format_se_article(raw_text)
    return canonical_title, None


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
    existing_entries = get_existing_entries(OUT_FILE)
    print(f"Found {len(existing_entries):,} objects already in {OUT_FILE.name}.")

    print("Scanning extrasolar .sc and .csv catalogs...")
    all_objects = get_extrasolar_objects(SE_DIR)
    systems = build_star_systems(all_objects)

    # Step 1: Locally copy existing descriptions to undocumented companion stars in multi-star systems
    local_propagated: list[tuple[int, str]] = []
    for root_idx, member_indices in systems.items():
        if len(member_indices) <= 1:
            continue
        documented_members = [
            (m_idx, existing_entries[all_objects[m_idx]["primary"]])
            for m_idx in member_indices
            if all_objects[m_idx]["primary"] in existing_entries
        ]
        if not documented_members:
            continue

        root_name = all_objects[root_idx]["primary"]
        shared_descr = existing_entries.get(root_name, documented_members[0][1])
        for m_idx in member_indices:
            m_name = all_objects[m_idx]["primary"]
            if m_name not in existing_entries:
                local_propagated.append((m_idx, shared_descr))
                existing_entries[m_name] = shared_descr

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    added_count = 0
    if local_propagated:
        file_has_content = OUT_FILE.exists() and OUT_FILE.stat().st_size > 0
        with open(OUT_FILE, "a" if file_has_content else "w", encoding="utf-8") as out:
            if not file_has_content:
                out.write("// Auto-generated full Wikipedia articles for extrasolar objects\n\n")
            else:
                out.write("\n")
            for m_idx, shared_descr in sorted(local_propagated, key=lambda x: x[0]):
                write_robject(out, all_objects[m_idx], shared_descr)
                added_count += 1
        print(f"Copied existing system descriptions to {added_count:,} companion stars locally (0 API calls).")

    # Step 2: Check remaining undocumented objects on Wikipedia
    new_indices = [idx for idx, obj in enumerate(all_objects) if obj["primary"] not in existing_entries]
    print(f"Found {len(all_objects):,} total candidates ({len(new_indices):,} remaining to check on Wikipedia).\n")

    if not new_indices:
        print(f"All catalog candidates are now present in {OUT_FILE.name}.")
        return

    query_queue = [(idx, alias) for idx in new_indices for alias in all_objects[idx]["aliases"]]
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
    verified_by_idx: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(verify_astro_chunk, c) for c in verify_chunks]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            for idx, resolved_title in fut.result():
                verified_by_idx[idx] = resolved_title
            print_progress("Pass 1B (Batches)", done_count, len(verify_chunks),
                           f"| Verified: {len(verified_by_idx):,} | Rate: {_limiter.rps:.1f} req/s")
    print()

    # Propagate newly matched system descriptions across any remaining multi-star systems
    new_propagated_count = 0
    for root_idx, member_indices in systems.items():
        if len(member_indices) <= 1:
            continue
        matched_members = [(m_idx, verified_by_idx[m_idx]) for m_idx in member_indices if m_idx in verified_by_idx]
        if not matched_members:
            continue

        system_title = verified_by_idx.get(root_idx, matched_members[0][1])
        for m_idx in member_indices:
            if m_idx not in verified_by_idx and all_objects[m_idx]["primary"] not in existing_entries:
                verified_by_idx[m_idx] = system_title
                new_propagated_count += 1

    if new_propagated_count:
        print(f"Propagated new system descriptions to {new_propagated_count:,} companion stars.")

    unique_titles = sorted(set(verified_by_idx.values()))
    print(f"\nDownloading {len(unique_titles):,} unique Wikipedia articles for {len(verified_by_idx):,} objects...\n")

    formatted_articles: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(fetch_and_format_title, title) for title in unique_titles]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            title, clean_descr = fut.result()
            if clean_descr:
                formatted_articles[title] = clean_descr
            print_progress("Pass 2 (Articles)", done_count, len(unique_titles),
                           f"| Ready: {len(formatted_articles):,} | Rate: {_limiter.rps:.1f} req/s")

    file_has_content = OUT_FILE.exists() and OUT_FILE.stat().st_size > 0
    with open(OUT_FILE, "a" if file_has_content else "w", encoding="utf-8") as out:
        if not file_has_content:
            out.write("// Auto-generated full Wikipedia articles for extrasolar objects\n\n")
        else:
            out.write("\n")

        for idx in sorted(verified_by_idx.keys()):
            obj = all_objects[idx]
            if obj["primary"] in existing_entries:
                continue
            clean_descr = formatted_articles.get(verified_by_idx[idx])
            if clean_descr:
                write_robject(out, obj, clean_descr)
                existing_entries[obj["primary"]] = clean_descr
                added_count += 1

    elapsed = time.monotonic() - start_time
    print(f"\n\nDone in {elapsed:.1f}s. Added {added_count:,} total new entries to {OUT_FILE}")


if __name__ == "__main__":
    build_mod()