"use client";

import { useParams, useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import { apiGet } from "../../../components/api";
import { Physician } from "../../../components/types";

export default function PhysicianDetailPage() {
  const params = useParams<{ physicianId: string }>();
  const searchParams = useSearchParams();
  const physicianId = Number(params.physicianId);
  const fromConferenceId = searchParams.get("fromConferenceId");
  const fromYear = searchParams.get("fromYear");
  const [physician, setPhysician] = useState<Physician | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function load() {
      if (!physicianId || Number.isNaN(physicianId)) {
        setError("Invalid physician id");
        setLoading(false);
        return;
      }
      try {
        const query = new URLSearchParams();
        if (fromConferenceId) {
          query.set("fromConferenceId", fromConferenceId);
        }
        if (fromYear) {
          query.set("fromYear", fromYear);
        }
        const suffix = query.toString() ? `?${query.toString()}` : "";
        const response = await apiGet<Physician>(`/physicians/${physicianId}${suffix}`);
        setPhysician(response);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load physician detail");
      } finally {
        setLoading(false);
      }
    }
    void load();
  }, [physicianId, fromConferenceId, fromYear]);

  const groupedAppearances = useMemo(() => {
    if (!physician) {
      return [];
    }
    const map = new Map<
      string,
      {
        conferenceId: number;
        conferenceName: string;
        year: number;
        appearances: typeof physician.appearances;
      }
    >();
    for (const appearance of physician.appearances) {
      const key = `${appearance.conference_id}::${appearance.year}`;
      const current = map.get(key);
      if (current) {
        current.appearances.push(appearance);
      } else {
        map.set(key, {
          conferenceId: appearance.conference_id,
          conferenceName: appearance.conference_name,
          year: appearance.year,
          appearances: [appearance],
        });
      }
    }
    return Array.from(map.values()).sort((a, b) => b.year - a.year || a.conferenceName.localeCompare(b.conferenceName));
  }, [physician]);

  const detailBlocks = useMemo(() => {
    if (!physician) {
      return [];
    }
    const blocks: Array<{ label: string; value: string }> = [];
    if (physician.primary_designation) {
      blocks.push({ label: "Designation", value: physician.primary_designation });
    }
    if (physician.primary_specialty) {
      blocks.push({ label: "Specialty", value: physician.primary_specialty });
    }
    if (physician.primary_affiliation) {
      blocks.push({ label: "Affiliation", value: physician.primary_affiliation });
    }
    if (physician.primary_location) {
      blocks.push({ label: "Location", value: physician.primary_location });
    }
    if (physician.primary_education) {
      blocks.push({ label: "Education", value: physician.primary_education });
    }
    if (physician.bio_short) {
      blocks.push({ label: "Bio", value: physician.bio_short });
    }
    return blocks;
  }, [physician]);

  if (loading) {
    return <section className="panel">Loading physician detail...</section>;
  }
  if (error) {
    return <section className="panel error">{error}</section>;
  }
  if (!physician) {
    return <section className="panel">Physician not found.</section>;
  }

  return (
    <div className="page-stack">
      <section className="panel">
        <div className="physician-profile">
          <div className="physician-main">
            <h2>{physician.full_name}</h2>
            <div className="detail-grid">
              {detailBlocks.length > 0 ? (
                detailBlocks.map((block) => (
                  <div className="info-block" key={block.label}>
                    <span className="info-label">{block.label}</span>
                    <span className="info-value">{block.value}</span>
                  </div>
                ))
              ) : (
                <p className="small">Profile details are not available yet.</p>
              )}
            </div>
          </div>
        </div>
      </section>

      <section className="panel">
        <div className="panel-head">
          <h3>Conferences and Sessions</h3>
          <p className="small">{physician.appearances.length} appearance record(s)</p>
        </div>
        <ul className="clean">
          {groupedAppearances.map((group) => {
            const sessions = group.appearances.filter(
              (appearance, index, arr) =>
                Boolean(appearance.session_title) &&
                arr.findIndex(
                  (item) =>
                    item.session_title === appearance.session_title &&
                    item.role === appearance.role &&
                    item.conference_year_id === appearance.conference_year_id,
                ) === index,
            );
            const isHighlighted =
              (physician.highlight_conference_id && physician.highlight_conference_id === group.conferenceId) ||
              (physician.highlight_year && physician.highlight_year === group.year);

            return (
              <li key={`${group.conferenceId}-${group.year}`} className={isHighlighted ? "highlighted" : ""}>
                <strong>
                  {group.conferenceName} • {group.year}
                </strong>
                {sessions.length === 0 ? null : (
                  <ul className="clean" style={{ marginTop: "0.35rem" }}>
                    {sessions.map((appearance) => (
                      <li key={appearance.id}>
                        {appearance.session_title ? <div>{appearance.session_title}</div> : null}
                        {appearance.role ? <div className="small">{appearance.role}</div> : null}
                        {appearance.talk_brief_extracted || appearance.talk_brief_generated ? (
                          <div className="small">{appearance.talk_brief_extracted || appearance.talk_brief_generated}</div>
                        ) : null}
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            );
          })}
        </ul>
      </section>
    </div>
  );
}
