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
    "User-Agent": "SpaceEngineWikiModBuilder/7.0 (https://spaceengine.org; local-mod-script)"
}

TEST_TARGETS = {
    "proxima", "proxima centauri", "proxima centauri b", "proxima centauri d", "alpha centauri",
    "51 peg", "51 pegasi", "51 pegasi b", "helvetios", "dimidium",
    "trappist-1", "trappist-1e",
    "55 cnc", "55 cancri", "55 cancri a", "55 cancri b", "55 cancri e", "copernicus", "galileo",
    "16 cyg", "16 cyg (ac)", "16 cygni", "16 cygni a", "16 cygni b", "16 cygni c", "16 cygni bb",
    "kepler-16", "kepler-16 a", "kepler-16 b", "kepler-16b",
    "hd 189733", "hd 189733 b",
    "ross 128", "ross 128 b",
    "gj 1214", "gj 1214 b",
    "lich", "psr 1257+12", "psr b1257+12", "psr b1257+12 c"
}

ASTRO_KEYWORDS = {
    "star", "planet", "exoplanet", "galaxy", "nebula", "cluster",
    "constellation", "dwarf", "supernova", "pulsar", "quasar",
    "light-years", "parsec", "orbit", "binary", "giant", "spectral type",
    "planetary system", "brown dwarf", "white dwarf", "red dwarf"
}

BIO_PATTERN = re.compile(
    r'(\(\d{1,2}\s+\w+\s+\d{4}\s*[–-]\s*\d{1,2}\s+\w+\s+\d{4}\)|'
    r'\bwas\s+an?\s+(?:[a-z]+\s+){0,2}(?:astronomer|physicist|mathematician|philosopher|polymath)\b)',
    re.IGNORECASE
)

EXOPLANET_ONLY_KEYWORDS = {
    "is an exoplanet", "is a circumbinary planet", "is a hot jupiter",
    "is a super-earth", "is a gas giant", "is a terrestrial planet",
    "is an extrasolar planet", "is a sub-neptune", "is a mini-neptune"
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
    r'TYC|2MASS|Gaia|USNO|UCAC|GSC|SDSS|WISE|PPM|SAO|BD|CD|CP|ADS|CCDM|WDS|PSR|Kepler|TRAPPIST|TOI|K2|WASP|HAT)\b',
    re.IGNORECASE
)

NOTABLE_STAR_PATTERN = re.compile(
    r'^(PSR|GJ|Gliese|Wolf|Ross|Lalande|Lacaille|Luyten|Groombridge|Kruger|Struve|Proxima|Barnard|Kapteyn)\b',
    re.IGNORECASE
)

CONSTELLATION_GENITIVES = {
    "And": "Andromedae", "Ant": "Antliae", "Aps": "Apodis", "Aqr": "Aquarii",
    "Aql": "Aquilae", "Ara": "Arae", "Ari": "Arietis", "Aur": "Aurigae",
    "Boo": "Boötis", "Cae": "Caeli", "Cam": "Camelopardalis", "Cnc": "Cancri",
    "CVn": "Canum Venaticorum", "CMa": "Canis Majoris", "CMi": "Canis Minoris",
    "Cap": "Capricorni", "Car": "Carinae", "Cas": "Cassiopeiae", "Cen": "Centauri",
    "Cep": "Cephei", "Cet": "Ceti", "Cha": "Chamaeleontis", "Cir": "Circini",
    "Col": "Columbae", "Com": "Comae Berenices", "CrA": "Coronae Australis",
    "CrB": "Coronae Borealis", "Crv": "Corvi", "Crt": "Crateris", "Cru": "Crucis",
    "Cyg": "Cygni", "Del": "Delphini", "Dor": "Doradus", "Dra": "Draconis",
    "Equ": "Equulei", "Eri": "Eridani", "For": "Fornacis", "Gem": "Geminorum",
    "Gru": "Gruis", "Her": "Herculis", "Hor": "Horologii", "Hya": "Hydrae",
    "Hyi": "Hydri", "Ind": "Indi", "Lac": "Lacertae", "Leo": "Leonis",
    "LMi": "Leonis Minoris", "Lep": "Leporis", "Lib": "Librae", "Lup": "Lupi",
    "Lyn": "Lyncis", "Lyr": "Lyrae", "Men": "Mensae", "Mic": "Microscopii",
    "Mon": "Monocerotis", "Mus": "Muscae", "Nor": "Normae", "Oct": "Octantis",
    "Oph": "Ophiuchi", "Ori": "Orionis", "Pav": "Pavonis", "Peg": "Pegasi",
    "Per": "Persei", "Phe": "Phoenicis", "Pic": "Pictoris", "Psc": "Piscium",
    "PsA": "Piscis Austrini", "Pup": "Puppis", "Pyx": "Pyxidis", "Ret": "Reticuli",
    "Sge": "Sagittae", "Sgr": "Sagittarii", "Sco": "Scorpii", "Scl": "Sculptoris",
    "Sct": "Scuti", "Ser": "Serpentis", "Sex": "Sextantis", "Tau": "Tauri",
    "Tel": "Telescopii", "Tri": "Trianguli", "TrA": "Trianguli Australis",
    "Tuc": "Tucanae", "UMa": "Ursae Majoris", "UMi": "Ursae Minoris",
    "Vel": "Velorum", "Vir": "Virginis", "Vol": "Volantis", "Vul": "Vulpeculae"
}

CONSTELLATION_ABBREV_RE = re.compile(
    r'\b(' + '|'.join(CONSTELLATION_GENITIVES.keys()) + r')\b'
)

STELLAR_TYPES = {"Star", "StarBarycenter", "Barycenter"}
BARYCENTER_TYPES = {"StarBarycenter", "Barycenter"}

MAX_CSV_APP_MAG = 8.0
MAX_NEARBY_DIST_PC = 30.0
MAX_WORKERS = 3
EXIST_BATCH = 50
EXTRACT_BATCH = 10


class AIMDRateLimiter:
    """Thread-safe AIMD rate controller that learns the server's ceiling on 429."""
    def __init__(self, initial_rps: float = 2.5, min_rps: float = 0.5, max_rps: float = 3.2):
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


def obj_key(obj: dict) -> str:
    return f"{obj['primary']}||{obj['parent'] or ''}"


def get_existing_entries(cfg_path: Path) -> dict[str, str]:
    """Reads eng-db.cfg and returns {Name||Parent: Descr} for all already-documented objects."""
    entries = {}
    if not cfg_path.exists():
        return entries

    current_name = None
    current_parent = ""
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
                        entries[f"{current_name}||{current_parent}"] = "\n".join(descr_lines)
                    in_descr = False
                    descr_lines = []
                    current_name = None
                    current_parent = ""
                else:
                    descr_lines.append(line)
                continue

            name_match = re.match(r'^Name\s+"([^"]+)"', stripped)
            if name_match:
                current_name = name_match.group(1)
                current_parent = ""
                continue

            parent_match = re.match(r'^Parent\s+"([^"]+)"', stripped)
            if parent_match:
                current_parent = parent_match.group(1)
                continue

            descr_match = re.match(r'^Descr\s+"(.*)$', stripped)
            if descr_match:
                rest = descr_match.group(1)
                if rest.endswith('"'):
                    if current_name:
                        entries[f"{current_name}||{current_parent}"] = rest[:-1]
                    current_name = None
                    current_parent = ""
                else:
                    in_descr = True
                    descr_lines = [rest]

    return entries


def iter_sc_blocks(content: str):
    """Yields (obj_type, raw_name, body) by matching balanced curly braces in .sc files."""
    header_re = re.compile(r'\b(StarBarycenter|Barycenter|Star|Planet|Galaxy|Nebula|Cluster)\s+"([^"]+)"\s*\{')
    pos = 0
    n = len(content)
    while pos < n:
        match = header_re.search(content, pos)
        if not match:
            break
        obj_type = match.group(1)
        raw_name = match.group(2)
        brace_start = match.end()
        depth = 1
        i = brace_start
        while i < n and depth > 0:
            ch = content[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            i += 1
        yield obj_type, raw_name, content[brace_start:i - 1]
        pos = i


def expand_catalog_aliases(aliases: list[str]) -> list[str]:
    """Expands 3-letter IAU constellation abbreviations (e.g., '55 Cnc e' -> '55 Cancri e') and PSR IDs."""
    expanded = list(aliases)
    for alias in aliases:
        full_const = CONSTELLATION_ABBREV_RE.sub(
            lambda m: CONSTELLATION_GENITIVES[m.group(1)], alias
        )
        if full_const != alias and full_const not in expanded:
            expanded.append(full_const)

        if alias.startswith("PSR ") and not alias.startswith(("PSR B", "PSR J")):
            psr_b = "PSR B" + alias[4:]
            if psr_b not in expanded:
                expanded.append(psr_b)
    return expanded


def is_notable_csv_star(aliases: list[str]) -> bool:
    for alias in aliases:
        if NOTABLE_STAR_PATTERN.match(alias):
            return True
        if not CATALOG_DESIGNATION_PATTERN.match(alias) and not NOISE_CATALOG_PATTERN.match(alias):
            return True
    return False


def get_extrasolar_objects(se_dir: Path) -> list[dict]:
    objects = []
    seen_keys = set()
    catalogs_dir = se_dir / "data" / "catalogs"

    def add_entry(raw_name_field: str, obj_type: str, parent: str | None = None):
        raw_names = [n.strip().replace("|", "") for n in raw_name_field.split("/") if n.strip()]
        if not raw_names:
            return
        primary_name = raw_names[0]
        if parent == "Sol":
            return

        dedup_id = f"{primary_name}||{parent or ''}"
        if dedup_id in seen_keys:
            return

        filtered_aliases = expand_catalog_aliases(
            [n for n in raw_names if not NOISE_CATALOG_PATTERN.match(n)]
        )
        if not filtered_aliases:
            return

        # Keep primary_name first for deep-sky objects so cluster/nebula names aren't displaced by member stars
        if obj_type in {"Galaxy", "Nebula", "Cluster"}:
            prioritized_aliases = list(filtered_aliases)
        else:
            prioritized_aliases = sorted(
                filtered_aliases,
                key=lambda a: (0 if (" " in a or "-" in a or CATALOG_DESIGNATION_PATTERN.match(a)) else 1)
            )

        if obj_type == "Planet":
            for alias in list(prioritized_aliases):
                if " " not in alias and "-" not in alias and not CATALOG_DESIGNATION_PATTERN.match(alias):
                    exo_title = f"{alias} (exoplanet)"
                    if exo_title not in prioritized_aliases:
                        prioritized_aliases.append(exo_title)

        if parent and obj_type in STELLAR_TYPES:
            parent_primary = parent.split("/")[0].strip()
            parent_words = parent_primary.split()
            if len(parent_words) >= 2:
                constellation_or_suffix = parent_words[-1]
                for alias in list(prioritized_aliases):
                    if " " not in alias and not CATALOG_DESIGNATION_PATTERN.match(alias):
                        combined = f"{alias} {constellation_or_suffix}"
                        if combined not in prioritized_aliases:
                            prioritized_aliases.insert(0, combined)

        seen_keys.add(dedup_id)
        objects.append({
            "primary": primary_name,
            "aliases": prioritized_aliases,
            "parent": parent,
            "type": obj_type
        })

        # Also generate a self-parented child Star entry for root stars/barycenters so clicking
        # the physical star in 3D space keeps its description after F3 selection
        if parent is None and obj_type in STELLAR_TYPES:
            child_dedup_id = f"{primary_name}||{primary_name}"
            if child_dedup_id not in seen_keys:
                seen_keys.add(child_dedup_id)
                objects.append({
                    "primary": primary_name,
                    "aliases": prioritized_aliases,
                    "parent": primary_name,
                    "type": "Star"
                })

    for pak_path in catalogs_dir.glob("*.pak"):
        with zipfile.ZipFile(pak_path, "r") as zf:
            for file_name in zf.namelist():
                if "solarsys" in file_name.lower():
                    continue

                if file_name.endswith(".sc"):
                    content = zf.read(file_name).decode("utf-8", errors="ignore")
                    for obj_type, raw_name, body in iter_sc_blocks(content):
                        parent_match = re.search(r'ParentBody\s+"([^"]+)"', body)
                        parent = parent_match.group(1).strip() if parent_match else None
                        add_entry(raw_name, obj_type, parent)

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
    """Groups Star, StarBarycenter, and Barycenter indices by their top-level stellar system root."""
    name_to_barycenter_idx = {}
    name_to_any_star_idx = {}

    for idx, obj in enumerate(objects):
        if obj["type"] not in STELLAR_TYPES:
            continue
        names = [obj["primary"], *obj["aliases"]]
        for n in names:
            if obj["type"] in BARYCENTER_TYPES and n not in name_to_barycenter_idx:
                name_to_barycenter_idx[n] = idx
            if obj["parent"] is None and n not in name_to_any_star_idx:
                name_to_any_star_idx[n] = idx

    for idx, obj in enumerate(objects):
        if obj["type"] not in STELLAR_TYPES:
            continue
        for n in (obj["primary"], *obj["aliases"]):
            if n not in name_to_any_star_idx:
                name_to_any_star_idx[n] = idx

    systems = defaultdict(list)
    for idx, obj in enumerate(objects):
        if obj["type"] not in STELLAR_TYPES:
            continue

        root_idx = idx
        visited = {idx}
        curr = obj
        while curr["parent"]:
            parent_primary = curr["parent"].split("/")[0].strip()
            p_idx = name_to_barycenter_idx.get(parent_primary) or name_to_any_star_idx.get(parent_primary)
            if p_idx is None or p_idx in visited:
                break
            visited.add(p_idx)
            root_idx = p_idx
            curr = objects[p_idx]

        systems[root_idx].append(idx)

    return systems


def select_test_indices(objects: list[dict], systems: dict[int, list[int]]) -> list[int]:
    """Selects all objects matching TEST_TARGETS plus their full star-system hierarchies and planets."""
    matched_indices = set()
    for idx, obj in enumerate(objects):
        names_lower = {obj["primary"].lower(), *(a.lower() for a in obj["aliases"])}
        if names_lower & TEST_TARGETS:
            matched_indices.add(idx)

    for root_idx, member_indices in systems.items():
        if matched_indices & set(member_indices):
            matched_indices.update(member_indices)

    matched_star_names = {
        name
        for idx in matched_indices
        for name in (objects[idx]["primary"], *objects[idx]["aliases"])
    }
    for idx, obj in enumerate(objects):
        if obj["parent"] and obj["parent"].split("/")[0].strip() in matched_star_names:
            matched_indices.add(idx)

    return sorted(matched_indices)


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


def check_existence_chunk(chunk: list[tuple[int, int, str]]) -> list[tuple[int, int, str]]:
    titles_param = "|".join(dict.fromkeys(title for _, _, title in chunk))
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
    for idx, alias_rank, orig_title in chunk:
        resolved = resolve_redirects(orig_title, norm_map, redir_map)
        if resolved in existing_titles:
            hits.append((idx, alias_rank, resolved))
    return hits


def is_valid_astro_extract(extract_lower: str, obj_type: str) -> bool:
    if not extract_lower:
        return False

    if BIO_PATTERN.search(extract_lower):
        return False

    if obj_type in STELLAR_TYPES:
        if any(ekw in extract_lower for ekw in EXOPLANET_ONLY_KEYWORDS):
            if not any(
                skw in extract_lower
                for skw in (
                    "binary star", "star system", "red dwarf", "main-sequence",
                    "spectral type", "pulsar", "rogue planet", "planetary-mass"
                )
            ):
                return False

    return any(kw in extract_lower for kw in ASTRO_KEYWORDS)


def verify_astro_chunk(chunk: list[tuple[int, str, str]]) -> list[tuple[int, str]]:
    titles_param = "|".join(dict.fromkeys(title for _, _, title in chunk))
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "exintro": True,
        "exsentences": 3,
        "exlimit": EXTRACT_BATCH,
        "explaintext": True,
        "redirects": 1,
        "titles": titles_param
    }

    norm_map = {}
    redir_map = {}
    extracts_by_title = {}

    while True:
        data = wiki_get(params)
        q = data.get("query", {})
        if not q:
            break

        for n in q.get("normalized", []):
            norm_map[n["from"]] = n["to"]
        for r in q.get("redirects", []):
            redir_map[r["from"]] = r["to"]

        for pid, pdata in q.get("pages", {}).items():
            if pid != "-1" and pdata.get("extract"):
                extracts_by_title[pdata["title"]] = pdata["extract"].lower()

        cont = data.get("continue")
        if cont and "excontinue" in cont:
            params["excontinue"] = cont["excontinue"]
        else:
            break

    verified = []
    for idx, obj_type, resolved_title in chunk:
        final_title = resolve_redirects(resolved_title, norm_map, redir_map)
        extract_lower = extracts_by_title.get(final_title, "")
        if is_valid_astro_extract(extract_lower, obj_type):
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
    text = text.replace("\\", "/")
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


def build_mod(test_mode: bool = False):
    start_time = time.monotonic()

    print("Scanning extrasolar .sc and .csv catalogs...")
    all_objects = get_extrasolar_objects(SE_DIR)
    systems = build_star_systems(all_objects)

    if test_mode:
        existing_entries = {}
        new_indices = select_test_indices(all_objects, systems)
        print(f"[TEST MODE] Selected {len(new_indices):,} objects across the test systems.\n")
    else:
        existing_entries = get_existing_entries(OUT_FILE)
        print(f"Found {len(existing_entries):,} existing entries in {OUT_FILE.name}.")

        # Step 1: Locally copy existing descriptions to undocumented stars/child-stars in multi-star systems (0 API calls)
        local_propagated: list[tuple[int, str]] = []
        for root_idx, member_indices in systems.items():
            if len(member_indices) <= 1:
                continue
            documented_members = [
                (m_idx, existing_entries[obj_key(all_objects[m_idx])])
                for m_idx in member_indices
                if obj_key(all_objects[m_idx]) in existing_entries
            ]
            if not documented_members:
                continue

            root_k = obj_key(all_objects[root_idx])
            shared_descr = existing_entries.get(root_k, documented_members[0][1])
            for m_idx in member_indices:
                m_k = obj_key(all_objects[m_idx])
                if m_k not in existing_entries:
                    local_propagated.append((m_idx, shared_descr))
                    existing_entries[m_k] = shared_descr

        OUT_DIR.mkdir(parents=True, exist_ok=True)
        if local_propagated:
            file_has_content = OUT_FILE.exists() and OUT_FILE.stat().st_size > 0
            with open(OUT_FILE, "a" if file_has_content else "w", encoding="utf-8") as out:
                if not file_has_content:
                    out.write("// Auto-generated full Wikipedia articles for extrasolar objects\n\n")
                else:
                    out.write("\n")
                for m_idx, shared_descr in sorted(local_propagated, key=lambda x: x[0]):
                    write_robject(out, all_objects[m_idx], shared_descr)
            print(f"Copied existing system descriptions to {len(local_propagated):,} companion/child stars locally (0 API calls).")

        new_indices = [idx for idx, obj in enumerate(all_objects) if obj_key(obj) not in existing_entries]
        print(f"Found {len(all_objects):,} total catalog entries ({len(new_indices):,} not yet in {OUT_FILE.name}).\n")

    if not new_indices:
        print("All catalog candidates already have entries in eng-db.cfg. Nothing new to query.")
        return

    query_queue = [
        (idx, rank, alias)
        for idx in new_indices
        for rank, alias in enumerate(all_objects[idx]["aliases"])
    ]
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

    hits_by_idx: dict[int, list[tuple[int, str]]] = defaultdict(list)
    for idx, alias_rank, resolved_title in raw_hits:
        hits_by_idx[idx].append((alias_rank, resolved_title))

    verify_items: list[tuple[int, str, str]] = []
    for idx, ranked_titles in hits_by_idx.items():
        seen_t = set()
        for _, title in sorted(ranked_titles, key=lambda x: x[0]):
            if title not in seen_t:
                seen_t.add(title)
                verify_items.append((idx, all_objects[idx]["type"], title))

    verify_chunks = [verify_items[i:i + EXTRACT_BATCH] for i in range(0, len(verify_items), EXTRACT_BATCH)]

    # Pass 1B: Astronomy keyword + anti-biography verification (10 titles/batch with exsentences=3)
    verified_by_idx: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(verify_astro_chunk, c) for c in verify_chunks]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            for idx, resolved_title in fut.result():
                if idx not in verified_by_idx:
                    verified_by_idx[idx] = resolved_title
            print_progress("Pass 1B (Batches)", done_count, len(verify_chunks),
                           f"| Verified: {len(verified_by_idx):,} | Rate: {_limiter.rps:.1f} req/s")
    print()

    # Propagate system descriptions across all stars/barycenters/child-stars in multi-star systems
    new_propagated_count = 0
    for root_idx, member_indices in systems.items():
        if len(member_indices) <= 1:
            continue
        matched_members = [(m_idx, verified_by_idx[m_idx]) for m_idx in member_indices if m_idx in verified_by_idx]
        if not matched_members:
            continue

        system_title = verified_by_idx.get(root_idx, matched_members[0][1])
        for m_idx in member_indices:
            if m_idx not in verified_by_idx and obj_key(all_objects[m_idx]) not in existing_entries:
                verified_by_idx[m_idx] = system_title
                new_propagated_count += 1

    if new_propagated_count:
        print(f"Propagated system descriptions to {new_propagated_count:,} companion/child stars.")

    if test_mode:
        print("\n--- TEST CASE VERIFICATION REPORT ---")
        for idx in new_indices:
            obj = all_objects[idx]
            matched_article = verified_by_idx.get(idx, "NONE")
            print(f"[{obj['type']:14}] {obj['primary']:24} (Parent: {str(obj['parent']):20}) -> {matched_article}")
        print("-------------------------------------\n")

    unique_titles = sorted(set(verified_by_idx.values()))
    print(f"Downloading {len(unique_titles):,} unique Wikipedia articles for {len(verified_by_idx):,} objects...\n")

    formatted_articles: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(fetch_and_format_title, title) for title in unique_titles]
        for done_count, fut in enumerate(as_completed(futures), start=1):
            title, clean_descr = fut.result()
            if clean_descr:
                formatted_articles[title] = clean_descr
            print_progress("Pass 2 (Articles)", done_count, len(unique_titles),
                           f"| Ready: {len(formatted_articles):,} | Rate: {_limiter.rps:.1f} req/s")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    added_count = 0

    if test_mode:
        with open(OUT_FILE, "w", encoding="utf-8") as out:
            out.write("// TEST MODE output - run build_mod.py without --test for full build\n\n")
            for idx in sorted(verified_by_idx.keys()):
                obj = all_objects[idx]
                clean_descr = formatted_articles.get(verified_by_idx[idx])
                if clean_descr:
                    write_robject(out, obj, clean_descr)
                    added_count += 1
    else:
        file_has_content = OUT_FILE.exists() and OUT_FILE.stat().st_size > 0
        with open(OUT_FILE, "a" if file_has_content else "w", encoding="utf-8") as out:
            if not file_has_content:
                out.write("// Auto-generated full Wikipedia articles for extrasolar objects\n\n")
            else:
                out.write("\n")

            for idx in sorted(verified_by_idx.keys()):
                obj = all_objects[idx]
                k = obj_key(obj)
                if k in existing_entries:
                    continue
                clean_descr = formatted_articles.get(verified_by_idx[idx])
                if clean_descr:
                    write_robject(out, obj, clean_descr)
                    existing_entries[k] = clean_descr
                    added_count += 1

    elapsed = time.monotonic() - start_time
    print(f"\n\nDone in {elapsed:.1f}s. Added {added_count:,} new entries to {OUT_FILE}.")


if __name__ == "__main__":
    build_mod(test_mode="--test" in sys.argv)