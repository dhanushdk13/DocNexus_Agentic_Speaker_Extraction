from __future__ import annotations

from app.db import SessionLocal
from app.services.memory_store import get_template_memory_scores, registrable_domain, update_template_memory


def test_registrable_domain_reduces_subdomain() -> None:
    assert registrable_domain("https://sub.events.example.org/path") == "example.org"


def test_template_memory_update_and_score() -> None:
    domain = "iapac.org"
    template = "/conferences/continuum-{num}"

    with SessionLocal() as db:
        update_template_memory(
            db,
            domain=domain,
            template_key=template,
            intent="archive",
            speaker_hit=True,
            appearance_hit=False,
        )
        update_template_memory(
            db,
            domain=domain,
            template_key=template,
            intent="archive",
            speaker_hit=False,
            appearance_hit=True,
        )
        db.commit()

        scores = get_template_memory_scores(
            db,
            domain=domain,
            template_keys=[template],
            decay_days=30,
            min_visits=1,
        )

    assert template in scores
    assert 0.0 <= scores[template] <= 1.0
    assert scores[template] > 0.0
