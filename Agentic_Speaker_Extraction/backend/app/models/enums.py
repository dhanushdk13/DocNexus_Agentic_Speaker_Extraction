from enum import Enum


class ConferenceYearStatus(str, Enum):
    pending = "pending"
    running = "running"
    complete = "complete"
    blocked = "blocked"
    partial = "partial"
    error = "error"


class SourceCategory(str, Enum):
    official_speakers = "official_speakers"
    official_program = "official_program"
    pdf_program = "pdf_program"
    platform = "platform"
    recap = "recap"
    unknown = "unknown"


class SourceMethod(str, Enum):
    http_static = "http_static"
    playwright_dom = "playwright_dom"
    playwright_network = "playwright_network"
    pdf_text = "pdf_text"


class FetchStatus(str, Enum):
    new = "new"
    fetched = "fetched"
    blocked = "blocked"
    error = "error"
    skipped = "skipped"


class ExtractionArtifactType(str, Enum):
    clean_text = "clean_text"
    candidate_blocks = "candidate_blocks"
    llm_output = "llm_output"
    pdf_text = "pdf_text"
    network_json_sample = "network_json_sample"


class RunStatus(str, Enum):
    pending = "pending"
    running = "running"
    complete = "complete"
    partial = "partial"
    error = "error"
