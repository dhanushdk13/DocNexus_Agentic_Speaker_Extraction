from __future__ import annotations

import pytest

from app.config import Settings
from app.services import physician_enrichment
from app.services.physician_enrichment import (
    EnrichmentDebug,
    IdentityResolutionOutput,
    ProfileSynthesisOutput,
)


@pytest.mark.asyncio
async def test_enrichment_success_populates_profile(monkeypatch) -> None:
    async def fake_npi(settings, *, full_name: str, timeout: float):  # noqa: ANN001
        return [
            physician_enrichment.EvidenceHit(
                source="npi",
                url="https://npiregistry.cms.hhs.gov/provider-view/123",
                title=full_name,
                snippet="Infectious Disease | Clinic",
            )
        ]

    async def fake_empty(*args, **kwargs):  # noqa: ANN001, ANN002
        return []

    async def fake_identity(*args, **kwargs):  # noqa: ANN001, ANN002
        return IdentityResolutionOutput(
            ambiguous=False,
            confidence=0.91,
            same_person=True,
            identity_signature="rupa-patel-hiv",
            reason="matched evidence",
            selected_evidence_urls=["https://npiregistry.cms.hhs.gov/provider-view/123"],
        )

    async def fake_profile(*args, **kwargs):  # noqa: ANN001, ANN002
        return ProfileSynthesisOutput(
            full_name_normalized="Rupa Patel",
            designation_normalized="MD",
            specialty="Infectious Disease",
            affiliation="HIV Prevention Institute",
            location="New York, NY",
            education="NYU",
            bio_short="HIV specialist and conference speaker.",
            profile_url="https://hiv.example.org/rupa-patel",
            photo_url="https://hiv.example.org/images/rupa-patel.jpg",
            photo_source_url="https://hiv.example.org/rupa-patel",
            bio_source_url="https://hiv.example.org/rupa-patel",
            confidence=0.89,
        )

    monkeypatch.setattr(physician_enrichment, "_provider_npi", fake_npi)
    monkeypatch.setattr(physician_enrichment, "_provider_pubmed", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_openalex", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_duckduckgo", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_serpapi", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_identity_pass", fake_identity)
    monkeypatch.setattr(physician_enrichment, "_profile_pass", fake_profile)

    settings = Settings(postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper")
    result = await physician_enrichment.enrich_physician_profile(
        settings,
        full_name="Rupa Patel",
        conference_name="Continuum",
        year=2025,
        session_title="Person-Centered HIV Care",
        designation_hint="MD",
        affiliation_hint=None,
        location_hint=None,
    )

    assert result.ambiguous is False
    assert result.same_person is True
    assert result.specialty == "Infectious Disease"
    assert result.bio_short == "HIV specialist and conference speaker."
    assert result.photo_url == "https://hiv.example.org/images/rupa-patel.jpg"
    assert result.profile_url == "https://hiv.example.org/rupa-patel"


@pytest.mark.asyncio
async def test_enrichment_ambiguous_identity_returns_no_profile(monkeypatch) -> None:
    async def fake_npi(settings, *, full_name: str, timeout: float):  # noqa: ANN001
        return [
            physician_enrichment.EvidenceHit(
                source="npi",
                url="https://npiregistry.cms.hhs.gov/provider-view/123",
                title=full_name,
                snippet="Potentially multiple people with same name",
            )
        ]

    async def fake_empty(*args, **kwargs):  # noqa: ANN001, ANN002
        return []

    async def fake_identity(*args, **kwargs):  # noqa: ANN001, ANN002
        return IdentityResolutionOutput(
            ambiguous=True,
            confidence=0.46,
            same_person=False,
            identity_signature="rupa-patel-ambiguous",
            reason="conflicting specialties",
            selected_evidence_urls=[],
        )

    monkeypatch.setattr(physician_enrichment, "_provider_npi", fake_npi)
    monkeypatch.setattr(physician_enrichment, "_provider_pubmed", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_openalex", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_duckduckgo", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_serpapi", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_identity_pass", fake_identity)

    settings = Settings(postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper")
    result = await physician_enrichment.enrich_physician_profile(
        settings,
        full_name="Rupa Patel",
        conference_name="AAN",
        year=2026,
        session_title="Neurology Update",
        designation_hint="MD",
        affiliation_hint=None,
        location_hint=None,
    )

    assert result.ambiguous is True
    assert result.same_person is False
    assert result.bio_short is None
    assert result.photo_url is None


@pytest.mark.asyncio
async def test_enrichment_invalid_photo_url_is_dropped(monkeypatch) -> None:
    async def fake_npi(settings, *, full_name: str, timeout: float):  # noqa: ANN001
        return [
            physician_enrichment.EvidenceHit(
                source="npi",
                url="https://npiregistry.cms.hhs.gov/provider-view/999",
                title=full_name,
                snippet="Physician profile",
            )
        ]

    async def fake_empty(*args, **kwargs):  # noqa: ANN001, ANN002
        return []

    async def fake_identity(*args, **kwargs):  # noqa: ANN001, ANN002
        return IdentityResolutionOutput(
            ambiguous=False,
            confidence=0.90,
            same_person=True,
            identity_signature="jose-santos",
            reason="matched",
            selected_evidence_urls=["https://npiregistry.cms.hhs.gov/provider-view/999"],
        )

    async def fake_profile(*args, **kwargs):  # noqa: ANN001, ANN002
        return ProfileSynthesisOutput(
            full_name_normalized="Jose Santos",
            designation_normalized="MD",
            specialty="Neurology",
            bio_short="Neurologist",
            profile_url="https://example.org/jose-santos",
            photo_url="https://example.org/jose-santos-profile",  # intentionally non-image-like
            photo_source_url="https://example.org/jose-santos",
            confidence=0.88,
        )

    monkeypatch.setattr(physician_enrichment, "_provider_npi", fake_npi)
    monkeypatch.setattr(physician_enrichment, "_provider_pubmed", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_openalex", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_duckduckgo", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_provider_serpapi", fake_empty)
    monkeypatch.setattr(physician_enrichment, "_identity_pass", fake_identity)
    monkeypatch.setattr(physician_enrichment, "_profile_pass", fake_profile)

    settings = Settings(postgres_url="postgresql://dhanush:password@localhost:5432/test_scraper")
    result = await physician_enrichment.enrich_physician_profile(
        settings,
        full_name="Jose Santos",
        conference_name="AAN Annual Meeting",
        year=2026,
        session_title="Clinical Session",
        designation_hint="MD",
        affiliation_hint=None,
        location_hint=None,
    )

    assert result.ambiguous is False
    assert result.photo_url is None
    assert result.profile_url == "https://example.org/jose-santos"
    assert result.debug.provider_counts["npi"] == 1


def test_enrichment_debug_model_defaults() -> None:
    debug = EnrichmentDebug()
    assert debug.used_fallback is False
    assert debug.provider_counts == {}
