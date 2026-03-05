from __future__ import annotations

import io
import json
import re
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

import pdfplumber
from bs4 import BeautifulSoup


SPEAKER_HINTS = ["speaker", "faculty", "presenter", "moderator", "chair", "keynote", "physician", "doctor"]
SESSION_HINTS = ["session", "agenda", "program", "abstract", "workshop", "symposium", "panel", "lecture"]
NAV_NOISE_HINTS = {
    "terms and conditions",
    "cancellation policy",
    "become a supporter",
    "hotel information",
    "conference program",
    "register now",
    "privacy policy",
}
CONTEXT_NOISE_HINTS = {
    "privacy policy",
    "terms and conditions",
    "all rights reserved",
    "copyright",
    "cookie policy",
    "advertisement",
    "sponsor",
    "sponsorship",
    "exhibitor",
    "exhibit hall",
    "housing",
    "hotel",
    "travel",
    "venue",
    "register now",
    "book your room",
    "newsletter",
    "donate",
    "press release",
}
ASSET_YEAR_URL_RE = re.compile(
    r"https?://[^\s)]*(?:wp-content|uploads|static|assets)[^\s)]*/(?:19\d{2}|20\d{2})/",
    re.I,
)
URL_TOKEN_RE = re.compile(r"https?://[^\s)]+", re.I)
PERSON_CREDENTIAL_REGEX = re.compile(
    r"\b([A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3}),\s*"
    r"(M\.?D\.?|D\.?O\.?|O\.?D\.?|Ph\.?D\.?|MBBS|MPH|MS|NP|PA-C|PA|RN|MSN|DNP|PharmD|FIDSA|FAAO|AAHIVS)\b"
)
TIME_MARKER_REGEX = re.compile(r"\b\d{1,2}:\d{2}\s*(AM|PM)\b", re.I)
NETWORK_SIGNAL_KEYS = {
    "speakers",
    "presenters",
    "faculty",
    "sessions",
    "agenda",
    "bio",
    "abstract",
    "session",
    "speaker",
    "presenter",
}
MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
SPEAKER_PAIR_NAME_TAIL_RE = re.compile(
    r"(?P<session>.+?)\s*(?:[-:|•]|(?:\)\s*))\s*(?P<name>(?:[A-Z]\.\s+)?[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3})$"
)
NAME_WITH_AFFILIATION_RE = re.compile(
    r"^(?P<name>(?:[A-Z]\.\s+)?[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,4})\s*\([^)]{3,200}\)"
)
SESSION_HEADING_RE = re.compile(
    r"(?i)\b(session|keynote|plenary|symposium|workshop|lecture|panel|remarks|address)\b"
)
MARKDOWN_SESSION_SPEAKER_RE = re.compile(
    r"\[(?P<session>[^\]\n]{8,260})\]\([^)]+\)\s*(?:<br>\s*)?(?:[_*])?\s*(?P<name>(?:[A-Z]\.\s+)?[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3})"
)
PERSON_NAME_RE = re.compile(
    r"^(?:[A-Z]\.\s+)?[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,4}$"
)
NON_NAME_TOKENS = {
    "session",
    "program",
    "agenda",
    "abstract",
    "workshop",
    "symposium",
    "moderator",
    "panel",
    "prevention",
    "treatment",
    "care",
    "landscape",
    "services",
    "service",
    "delivery",
    "strategies",
    "options",
    "discussion",
    "join",
    "login",
    "log",
    "membership",
    "education",
    "research",
    "practice",
    "privacy",
    "policy",
    "contact",
    "home",
    "event",
    "events",
    "resource",
    "resources",
    "programming",
    "explore",
    "learn",
    "connect",
    "guidelines",
    "compensation",
    "commercial",
    "interest",
    "entity",
    "bureau",
    "safety",
    "time",
    "central",
}
NON_SESSION_TOKENS = {
    "join",
    "login",
    "log",
    "membership",
    "education",
    "research",
    "practice",
    "contact",
    "home",
    "search",
    "events",
    "event",
    "resources",
    "resource",
    "chat",
    "guidelines",
    "compensation",
    "commercial",
    "interest",
    "deadline",
    "submission",
    "upload",
    "instructions",
}
SPEAKER_PAIR_COLON_RE = re.compile(
    r"(?P<session>[^\n]{10,260}?:[^\n]{3,260}?)\s+(?P<name>(?:[A-Z]\.?\s+)?[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+){1,3})$"
)


def _clean_line(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_markdown(text: str) -> str:
    if not text:
        return ""
    value = MARKDOWN_LINK_RE.sub(r"\1", text)
    value = value.replace("*", " ").replace("_", " ")
    value = value.replace("•", " ")
    return _clean_line(unescape(value))


def _normalize_extracted_name(value: str) -> str:
    parts = [part for part in _clean_line(value).split() if part]
    while len(parts) >= 3 and parts[0].isupper():
        parts = parts[1:]
    return " ".join(parts)


def _looks_like_person_name(value: str) -> bool:
    cleaned = _normalize_extracted_name(value)
    if not cleaned:
        return False
    if not PERSON_NAME_RE.match(cleaned):
        return False
    parts = cleaned.split()
    if len(parts) < 2 or len(parts) > 5:
        return False
    lowered = [part.lower().strip(".") for part in parts]
    if len(set(lowered)) == 1:
        return False
    if len(parts) <= 2 and any(part in NON_NAME_TOKENS for part in lowered):
        return False
    if sum(1 for part in lowered if part in NON_NAME_TOKENS) >= 2:
        return False
    return True


def _looks_like_session_title(value: str) -> bool:
    cleaned = _clean_line(value)
    if len(cleaned) < 16:
        return False
    tokens = [part.lower().strip(".:") for part in cleaned.split() if part]
    if len(tokens) < 3:
        return False
    if len(set(tokens)) == 1:
        return False
    if sum(1 for token in tokens if token in NON_SESSION_TOKENS) >= 2:
        return False
    if SESSION_HEADING_RE.search(cleaned):
        return True
    # Most talk titles have richer phrasing; this filters menu-like nav text.
    return len(tokens) >= 4


def _looks_like_nav_noise(text: str) -> bool:
    lowered = text.lower()
    if any(hint in lowered for hint in NAV_NOISE_HINTS):
        # Keep if there are strong person/session markers despite nav strings.
        if PERSON_CREDENTIAL_REGEX.search(text) or TIME_MARKER_REGEX.search(text):
            return False
        return True
    return False


def _span_block(label: str, text: str) -> str:
    return f"[BLOCK {label}]\n{_clean_line(text)}"


def _extract_regex_spans(
    *,
    text: str,
    pattern: re.Pattern[str],
    radius: int,
    label: str,
    max_items: int,
) -> list[str]:
    out: list[str] = []
    for match in pattern.finditer(text):
        start = max(0, match.start() - radius)
        end = min(len(text), match.end() + radius)
        chunk = _clean_line(text[start:end])
        if len(chunk) < 30:
            continue
        out.append(_span_block(label, chunk))
        if len(out) >= max_items:
            break
    return out


def _registrable_domain(host: str) -> str:
    parts = [p for p in host.lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    return ".".join(parts[-2:])


def _same_site(url_a: str, url_b: str) -> bool:
    a = urlparse(url_a)
    b = urlparse(url_b)
    return _registrable_domain(a.netloc) == _registrable_domain(b.netloc)


def extract_page_title(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.string if soup.title else ""
    return _clean_line(title or "")


def extract_visible_text(html: str, max_chars: int = 2000) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    text = _clean_line(soup.get_text(" ", strip=True))
    return text[:max_chars]


def sanitize_conference_context_text(text: str, max_chars: int = 20000) -> str:
    if not text:
        return ""

    value = unescape(text)
    value = value.replace("\r", "\n")
    cleaned_lines: list[str] = []
    for raw_line in re.split(r"\n+", value):
        line = _clean_line(raw_line)
        if not line:
            continue

        lowered = line.lower()
        if lowered.startswith("copyright") or "all rights reserved" in lowered:
            continue
        if ASSET_YEAR_URL_RE.search(line):
            continue
        if URL_TOKEN_RE.fullmatch(line):
            continue

        if any(hint in lowered for hint in CONTEXT_NOISE_HINTS):
            if SESSION_HEADING_RE.search(line) or _looks_like_person_name(line) or PERSON_CREDENTIAL_REGEX.search(line):
                pass
            else:
                continue

        cleaned_lines.append(line)

    compact = _clean_line(" ".join(cleaned_lines))
    compact = URL_TOKEN_RE.sub(" ", compact)
    compact = _clean_line(compact)
    return compact[:max_chars]


def extract_event_focused_text(html: str, max_chars: int = 20000) -> str:
    focused_html = prioritize_event_content_html(html)
    visible = extract_visible_text(focused_html, max_chars=max_chars * 2)
    return sanitize_conference_context_text(visible, max_chars=max_chars)


def prioritize_event_content_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    for tag in soup.select("header, nav, footer, aside"):
        tag.extract()

    candidates = soup.select(
        "main, [role='main'], article, #content, #main, .content, .main-content, .event-content, .program-content"
    )
    best_html = ""
    best_len = 0
    for node in candidates:
        text_len = len(_clean_line(node.get_text(" ", strip=True)))
        if text_len > best_len:
            best_len = text_len
            best_html = str(node)

    if best_html and best_len >= 300:
        return best_html
    return str(soup)


def extract_internal_links(html: str, base_url: str, max_links: int = 60) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not _same_site(base_url, abs_url):
            continue
        if abs_url in seen:
            continue

        seen.add(abs_url)
        out.append(
            {
                "url": abs_url,
                "anchor": _clean_line(anchor.get_text(" ", strip=True))[:200],
            }
        )
        if len(out) >= max_links:
            break

    return out


def _looks_person_like(node: dict[str, Any]) -> bool:
    lowered_keys = {k.lower() for k in node.keys()}
    if "name" not in lowered_keys and not ({"firstname", "lastname"} <= lowered_keys):
        return False
    person_fields = {"bio", "title", "designation", "credentials", "affiliation", "organization", "company", "role"}
    return len(lowered_keys.intersection(person_fields)) > 0


def _looks_session_like(node: dict[str, Any]) -> bool:
    lowered_keys = {k.lower() for k in node.keys()}
    if "title" not in lowered_keys and "sessiontitle" not in lowered_keys:
        return False
    session_fields = {"abstract", "description", "speakers", "presenters", "faculty", "agenda", "time"}
    return len(lowered_keys.intersection(session_fields)) > 0


def _compact_dict(node: dict[str, Any], max_items: int = 30) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for idx, (key, value) in enumerate(node.items()):
        if idx >= max_items:
            break
        if isinstance(value, (str, int, float, bool)) or value is None:
            compact[key] = value
        elif isinstance(value, list):
            compact[key] = value[:8]
        elif isinstance(value, dict):
            compact[key] = {k: v for k, v in list(value.items())[:8]}
    return compact


def _walk_json_for_candidates(node: Any, out: list[dict[str, Any]], source: str) -> None:
    if isinstance(node, dict):
        if _looks_person_like(node):
            out.append({"kind": "person", "source": source, "data": _compact_dict(node)})
        elif _looks_session_like(node):
            out.append({"kind": "session", "source": source, "data": _compact_dict(node)})

        for value in node.values():
            _walk_json_for_candidates(value, out, source)
    elif isinstance(node, list):
        for item in node:
            _walk_json_for_candidates(item, out, source)


def _balanced_json_substring(text: str, start: int) -> str | None:
    if start < 0 or start >= len(text):
        return None

    open_char = text[start]
    if open_char == "{":
        close_char = "}"
    elif open_char == "[":
        close_char = "]"
    else:
        return None

    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == open_char:
            depth += 1
        elif ch == close_char:
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]

    return None


def _extract_assignment_json(text: str, name: str) -> Any | None:
    idx = text.find(name)
    if idx == -1:
        return None

    eq_idx = text.find("=", idx)
    if eq_idx == -1:
        return None

    brace_idx = text.find("{", eq_idx)
    bracket_idx = text.find("[", eq_idx)
    starts = [i for i in [brace_idx, bracket_idx] if i != -1]
    if not starts:
        return None
    start = min(starts)
    payload = _balanced_json_substring(text, start)
    if not payload:
        return None

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def extract_embedded_candidates(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, Any]] = []

    for script in soup.select("script"):
        script_text = (script.string or script.get_text() or "").strip()
        if not script_text:
            continue

        script_type = (script.get("type") or "").lower()
        script_id = (script.get("id") or "").lower()

        if script_type == "application/ld+json":
            try:
                payload = json.loads(script_text)
            except json.JSONDecodeError:
                continue
            _walk_json_for_candidates(payload, out, "ld_json")
            continue

        if script_id == "__next_data__":
            try:
                payload = json.loads(script_text)
            except json.JSONDecodeError:
                payload = None
            if payload is not None:
                _walk_json_for_candidates(payload, out, "next_data")
            continue

        for marker, source in [
            ("__NUXT__", "nuxt"),
            ("__APOLLO_STATE__", "apollo_state"),
            ("drupalSettings", "drupal_settings"),
            ("dataLayer", "data_layer"),
        ]:
            payload = _extract_assignment_json(script_text, marker)
            if payload is not None:
                _walk_json_for_candidates(payload, out, source)

    dedup: dict[str, dict[str, Any]] = {}
    for item in out:
        key = json.dumps(item, sort_keys=True, ensure_ascii=True)
        dedup[key] = item

    return list(dedup.values())[:400]


def _network_payload_has_signals(node: Any) -> bool:
    if isinstance(node, dict):
        keys = {str(k).lower() for k in node.keys()}
        if keys.intersection(NETWORK_SIGNAL_KEYS):
            return True
        return any(_network_payload_has_signals(v) for v in node.values())
    if isinstance(node, list):
        return any(_network_payload_has_signals(v) for v in node)
    return False


def extract_network_candidates(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for payload in payloads:
        data = payload.get("data")
        if data is None:
            continue
        if not _network_payload_has_signals(data):
            continue
        _walk_json_for_candidates(data, out, "network_json")

    dedup: dict[str, dict[str, Any]] = {}
    for item in out:
        key = json.dumps(item, sort_keys=True, ensure_ascii=True)
        dedup[key] = item

    return list(dedup.values())[:500]


def extract_blocks_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    blocks: list[str] = []

    selectors = [
        "[class*=speaker]",
        "[class*=faculty]",
        "[class*=presenter]",
        "[class*=profile]",
        "[class*=bio]",
        "[class*=agenda]",
        "[class*=session]",
        "article",
        "li",
    ]

    for selector in selectors:
        for node in soup.select(selector):
            if node.find_parent(["header", "nav", "footer"]):
                continue
            text = _clean_line(node.get_text(" ", strip=True))
            lowered = text.lower()
            if len(text) < 25:
                continue
            if _looks_like_nav_noise(text):
                continue
            has_signal = (
                any(h in lowered for h in SPEAKER_HINTS + SESSION_HINTS)
                or ", md" in lowered
                or ", do" in lowered
                or ", od" in lowered
                or PERSON_CREDENTIAL_REGEX.search(text) is not None
                or TIME_MARKER_REGEX.search(text) is not None
            )
            if not has_signal:
                continue
            if len(text) > 1600:
                blocks.extend(_extract_regex_spans(text=text, pattern=PERSON_CREDENTIAL_REGEX, radius=180, label="person_span", max_items=4))
                blocks.extend(_extract_regex_spans(text=text, pattern=TIME_MARKER_REGEX, radius=240, label="session_span", max_items=4))
                continue
            blocks.append(_span_block("dom_card", text))

    full_text = extract_visible_text(html, max_chars=120000)
    blocks.extend(_extract_regex_spans(text=full_text, pattern=PERSON_CREDENTIAL_REGEX, radius=170, label="person_span", max_items=40))
    blocks.extend(_extract_regex_spans(text=full_text, pattern=TIME_MARKER_REGEX, radius=240, label="session_span", max_items=25))

    if not blocks:
        for p in soup.select("p"):
            text = _clean_line(p.get_text(" ", strip=True))
            if len(text) >= 60:
                blocks.append(_span_block("paragraph", text))

    dedup: dict[str, None] = {}
    for block in blocks:
        dedup[block] = None
    return list(dedup.keys())[:300]


def extract_text_from_pdf_bytes(raw_latin1: str) -> str:
    raw_bytes = raw_latin1.encode("latin-1", errors="ignore")
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        texts: list[str] = []
        for page in pdf.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                texts.append(txt)
    return "\n".join(texts)


def extract_pdf_text_with_scan_flag(raw_latin1: str) -> tuple[str, bool]:
    raw_bytes = raw_latin1.encode("latin-1", errors="ignore")
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        texts: list[str] = []
        scanned_pages = 0
        for page in pdf.pages:
            txt = page.extract_text() or ""
            cleaned = txt.strip()
            if cleaned:
                texts.append(cleaned)
            else:
                scanned_pages += 1

    return "\n\n".join(texts), scanned_pages == len(texts) + scanned_pages and scanned_pages > 0


def extract_blocks_from_pdf_text(text: str) -> list[str]:
    if not text.strip():
        return []

    chunks = re.split(r"\n{2,}", text)
    out: list[str] = []
    for chunk in chunks:
        cleaned = _clean_line(chunk)
        if len(cleaned) < 30:
            continue
        lowered = cleaned.lower()
        if any(h in lowered for h in SPEAKER_HINTS + SESSION_HINTS) or ", md" in lowered or ", do" in lowered or ", od" in lowered:
            out.append(f"[BLOCK pdf_chunk]\n{cleaned}")
    return out[:250]


def _walk_json_text(node: Any, found: list[str]) -> None:
    if isinstance(node, dict):
        text_values: list[str] = []
        for value in node.values():
            if isinstance(value, str):
                text_values.append(value)
            else:
                _walk_json_text(value, found)
        joined = _clean_line(" ".join(text_values))
        if len(joined) >= 30:
            found.append(joined)
    elif isinstance(node, list):
        for item in node:
            _walk_json_text(item, found)


def extract_blocks_from_network_json(payload_json_text: str) -> list[str]:
    try:
        payloads = json.loads(payload_json_text)
    except json.JSONDecodeError:
        return []

    found: list[str] = []
    _walk_json_text(payloads, found)

    dedup: dict[str, None] = {}
    for block in found:
        dedup[block] = None
    return list(dedup.keys())[:200]


def extract_session_speaker_pairs(text: str, source_url: str, max_pairs: int = 160) -> list[dict[str, str]]:
    cleaned = _strip_markdown(text)
    if not cleaned:
        return []

    out: list[dict[str, str]] = []
    dedup: set[tuple[str, str]] = set()

    def _append_pair(session_title: str, speaker_name: str, context_snippet: str) -> None:
        session_clean = _clean_line(session_title)[:260]
        speaker_clean = _normalize_extracted_name(speaker_name)[:140]
        snippet_clean = _clean_line(context_snippet)[:520]
        if not session_clean or not speaker_clean:
            return
        if "![" in context_snippet or session_clean.startswith("!["):
            return
        if not _looks_like_session_title(session_clean):
            return
        if not _looks_like_person_name(speaker_clean):
            return
        key = (speaker_clean.lower(), session_clean.lower())
        if key in dedup:
            return
        dedup.add(key)
        out.append(
            {
                "candidate_type": "session_speaker_pair",
                "source_url": source_url,
                "session_title": session_clean,
                "speaker_name_raw": speaker_clean,
                "context_snippet": snippet_clean,
                "text": f"Session: {session_clean}. Speaker: {speaker_clean}. Context: {snippet_clean[:360]}",
            }
        )

    # Pattern for markdown/html list rows where session title is a link and speaker follows.
    for match in MARKDOWN_SESSION_SPEAKER_RE.finditer(text):
        _append_pair(
            match.group("session"),
            match.group("name"),
            match.group(0),
        )
        if len(out) >= max_pairs:
            return out[:max_pairs]

    # HTML-aware extraction for agenda lists.
    if "<li" in text and "</li>" in text:
        soup = BeautifulSoup(text, "lxml")
        current_session: str | None = None
        for node in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"]):
            node_text = _clean_line(node.get_text(" ", strip=True))
            if not node_text:
                continue

            if node.name != "li":
                if len(node_text) <= 280 and SESSION_HEADING_RE.search(node_text):
                    current_session = node_text
                continue

            session_title = None
            anchor = node.find("a")
            emphasis = node.find("em")
            if anchor and _clean_line(anchor.get_text(" ", strip=True)):
                session_title = _clean_line(anchor.get_text(" ", strip=True))
            elif emphasis and _clean_line(emphasis.get_text(" ", strip=True)):
                session_title = _clean_line(emphasis.get_text(" ", strip=True))

            li_text = node_text
            if session_title and li_text.lower().startswith(session_title.lower()):
                li_text = li_text[len(session_title) :].strip(" :-|")

            aff_match = NAME_WITH_AFFILIATION_RE.match(li_text)
            if aff_match and (session_title or current_session):
                _append_pair(session_title or current_session or "Session", aff_match.group("name"), node_text)
                if len(out) >= max_pairs:
                    return out[:max_pairs]
                continue

            if session_title and _looks_like_person_name(li_text):
                _append_pair(session_title, li_text, node_text)
                if len(out) >= max_pairs:
                    return out[:max_pairs]
                continue

    lines: list[str] = []
    for line in re.split(r"\n+", text):
        stripped = _strip_markdown(line)
        if not stripped:
            continue
        parts = [part.strip() for part in re.split(r"\s\*\s", stripped) if part.strip()]
        if parts:
            lines.extend(parts)
        else:
            lines.append(stripped)

    current_session: str | None = None

    for raw_line in lines:
        line = _clean_line(raw_line)
        if len(line) < 12:
            continue

        # Track session context from headings and section titles.
        if len(line) <= 240 and SESSION_HEADING_RE.search(line):
            current_session = line
            continue

        # Pattern: "Name (Affiliation)"
        aff_match = NAME_WITH_AFFILIATION_RE.match(line)
        if aff_match and current_session:
            _append_pair(current_session, aff_match.group("name"), line)
            if len(out) >= max_pairs:
                break
            continue

        # Pattern: "Session ... : Name" or "... ) Name" when separator is explicit.
        tail_match = SPEAKER_PAIR_NAME_TAIL_RE.search(line)
        if tail_match:
            session_title = _clean_line(tail_match.group("session"))
            if len(session_title) >= 12 and len(session_title) <= 260:
                _append_pair(session_title, tail_match.group("name"), line)
                if len(out) >= max_pairs:
                    break
                continue

        # Pattern: "Session title with colon ... Speaker Name"
        colon_match = SPEAKER_PAIR_COLON_RE.search(line)
        if colon_match:
            _append_pair(colon_match.group("session"), colon_match.group("name"), line)
            if len(out) >= max_pairs:
                break

    return out[:max_pairs]
