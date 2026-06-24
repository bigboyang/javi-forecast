"""Incident vector store backed by ChromaDB.

Stores anomaly predictions as 'incidents' and supports cosine-similarity
search to find past incidents similar to the current one.

ChromaDB is optional – if not installed the store falls back to a simple
in-memory list (most-recent ordering, no vector similarity).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

_COLLECTION_NAME = "apm_incidents"


@dataclass
class Incident:
    id: str
    service_name: str
    metric_name: str
    severity: str
    predicted_value: float
    expected_value: float
    z_score: float
    timestamp: datetime
    description: str


class IncidentStore:
    """Persist and retrieve anomaly incidents.

    Uses ChromaDB for vector-similarity search when available.
    Falls back to an in-memory list otherwise.
    """

    def __init__(self, persist_directory: str = "/tmp/javi_incidents") -> None:
        self._memory: list[Incident] = []
        self._chroma_collection = None

        try:
            import chromadb  # noqa: F401 – lazy import to keep boot fast

            client = chromadb.PersistentClient(path=persist_directory)
            self._chroma_collection = client.get_or_create_collection(
                name=_COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
            logger.info(
                "IncidentStore: ChromaDB initialised at %s (count=%d)",
                persist_directory,
                self._chroma_collection.count(),
            )
        except ImportError:
            logger.warning(
                "chromadb not installed – IncidentStore using in-memory fallback. "
                "Install with: pip install chromadb>=0.4.0"
            )
        except Exception as exc:
            logger.error(
                "IncidentStore: ChromaDB init failed (%s) – using in-memory fallback", exc
            )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def add_incident(
        self,
        service_name: str,
        metric_name: str,
        severity: str,
        predicted_value: float,
        expected_value: float,
        z_score: float,
        timestamp: datetime | None = None,
    ) -> Incident:
        """Store an anomaly incident and return it."""
        if timestamp is None:
            timestamp = datetime.now(tz=UTC)

        incident_id = str(uuid.uuid4())
        description = (
            f"service={service_name} metric={metric_name} severity={severity} "
            f"predicted={predicted_value:.4f} expected={expected_value:.4f} "
            f"z_score={z_score:.2f} at={timestamp.isoformat()}"
        )

        incident = Incident(
            id=incident_id,
            service_name=service_name,
            metric_name=metric_name,
            severity=severity,
            predicted_value=predicted_value,
            expected_value=expected_value,
            z_score=z_score,
            timestamp=timestamp,
            description=description,
        )
        self._memory.append(incident)

        if self._chroma_collection is not None:
            try:
                self._chroma_collection.add(
                    ids=[incident_id],
                    documents=[description],
                    metadatas=[
                        {
                            "service_name": service_name,
                            "metric_name": metric_name,
                            "severity": severity,
                            "z_score": str(z_score),
                            "timestamp": timestamp.isoformat(),
                        }
                    ],
                )
            except Exception as exc:
                logger.error("IncidentStore.add ChromaDB write failed: %s", exc)

        logger.debug("Incident stored id=%s service=%s", incident_id, service_name)
        return incident

    # ------------------------------------------------------------------
    # Read / search
    # ------------------------------------------------------------------

    def search_similar(
        self,
        service_name: str,
        metric_name: str,
        top_k: int = 5,
    ) -> list[Incident]:
        """Return up to *top_k* incidents most similar to the current context.

        Uses ChromaDB cosine similarity when available, else returns the
        most recent incidents for the same service.
        """
        query = f"service={service_name} metric={metric_name}"

        if self._chroma_collection is not None:
            count = self._chroma_collection.count()
            if count == 0:
                return []
            try:
                results = self._chroma_collection.query(
                    query_texts=[query],
                    n_results=min(top_k, count),
                    include=["documents", "metadatas"],
                )
                incidents: list[Incident] = []
                docs = results.get("documents", [[]])[0]
                metas = results.get("metadatas", [[]])[0]
                for doc, meta in zip(docs, metas, strict=True):
                    ts_str = meta.get("timestamp", "")
                    try:
                        ts = datetime.fromisoformat(ts_str)
                    except ValueError:
                        ts = datetime.now(tz=UTC)
                    incidents.append(
                        Incident(
                            id="",
                            service_name=meta.get("service_name", ""),
                            metric_name=meta.get("metric_name", ""),
                            severity=meta.get("severity", ""),
                            predicted_value=0.0,
                            expected_value=0.0,
                            z_score=float(meta.get("z_score", 0.0)),
                            timestamp=ts,
                            description=doc,
                        )
                    )
                return incidents
            except Exception as exc:
                logger.error("IncidentStore.search ChromaDB query failed: %s", exc)

        # Memory fallback: filter by service, return most recent
        relevant = [i for i in self._memory if i.service_name == service_name]
        return sorted(relevant, key=lambda x: x.timestamp, reverse=True)[:top_k]

    def get_all(self, service_name: str | None = None) -> list[Incident]:
        """Return all stored incidents, optionally filtered by service."""
        if service_name:
            return [i for i in self._memory if i.service_name == service_name]
        return list(self._memory)

    def count(self) -> int:
        if self._chroma_collection is not None:
            try:
                return self._chroma_collection.count()
            except Exception:
                pass
        return len(self._memory)
