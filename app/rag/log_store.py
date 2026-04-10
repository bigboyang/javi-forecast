"""Log vector store backed by ChromaDB.

Stores OTel log records as vector-embedded documents and supports
semantic search to surface logs relevant to a natural-language query.

ChromaDB is optional – if not installed the store falls back to a simple
in-memory deque (most-recent ordering, keyword substring search).
"""

from __future__ import annotations

import logging
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Deque, List, Optional

logger = logging.getLogger(__name__)

_COLLECTION_NAME = "apm_logs"
_MAX_MEMORY = 10_000   # max in-memory log records


@dataclass
class LogRecord:
    id: str
    service_name: str
    severity: str
    body: str
    timestamp: datetime
    trace_id: Optional[str]
    document: str            # the text that was embedded / indexed


class LogStore:
    """Persist and retrieve OTel log records for semantic search.

    Uses ChromaDB for vector-similarity search when available.
    Falls back to an in-memory deque with substring matching otherwise.
    """

    def __init__(self, persist_directory: str = "/tmp/javi_logs") -> None:
        self._memory: Deque[LogRecord] = deque(maxlen=_MAX_MEMORY)
        self._chroma_collection = None

        try:
            import chromadb  # noqa: F401

            client = chromadb.PersistentClient(path=persist_directory)
            self._chroma_collection = client.get_or_create_collection(
                name=_COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
            logger.info(
                "LogStore: ChromaDB initialised at %s (count=%d)",
                persist_directory,
                self._chroma_collection.count(),
            )
        except ImportError:
            logger.warning(
                "chromadb not installed – LogStore using in-memory fallback. "
                "Install with: pip install chromadb>=0.4.0"
            )
        except Exception as exc:
            logger.error(
                "LogStore: ChromaDB init failed (%s) – using in-memory fallback", exc
            )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def add_log(
        self,
        service_name: str,
        severity: str,
        body: str,
        timestamp: Optional[datetime] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
    ) -> LogRecord:
        """Store a single log record."""
        if timestamp is None:
            timestamp = datetime.now(tz=timezone.utc)

        log_id = str(uuid.uuid4())
        # Build a rich text document for embedding / search
        document = (
            f"service={service_name} severity={severity} "
            f"trace={trace_id or 'n/a'} at={timestamp.isoformat()} | {body}"
        )

        record = LogRecord(
            id=log_id,
            service_name=service_name,
            severity=severity,
            body=body,
            timestamp=timestamp,
            trace_id=trace_id,
            document=document,
        )
        self._memory.append(record)

        if self._chroma_collection is not None:
            try:
                self._chroma_collection.add(
                    ids=[log_id],
                    documents=[document],
                    metadatas=[
                        {
                            "service_name": service_name,
                            "severity": severity,
                            "trace_id": trace_id or "",
                            "span_id": span_id or "",
                            "timestamp": timestamp.isoformat(),
                        }
                    ],
                )
            except Exception as exc:
                logger.error("LogStore.add ChromaDB write failed: %s", exc)

        logger.debug("Log stored id=%s service=%s severity=%s", log_id, service_name, severity)
        return record

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        service_name: Optional[str] = None,
        top_k: int = 10,
    ) -> List[LogRecord]:
        """Return up to *top_k* log records most relevant to *query*.

        Uses ChromaDB vector search when available.  Falls back to
        case-insensitive substring search over the in-memory buffer.
        """
        if self._chroma_collection is not None:
            count = self._chroma_collection.count()
            if count == 0:
                return []
            try:
                where = {"service_name": service_name} if service_name else None
                kwargs = dict(
                    query_texts=[query],
                    n_results=min(top_k, count),
                    include=["documents", "metadatas"],
                )
                if where:
                    kwargs["where"] = where
                results = self._chroma_collection.query(**kwargs)
                records: List[LogRecord] = []
                docs = results.get("documents", [[]])[0]
                metas = results.get("metadatas", [[]])[0]
                for doc, meta in zip(docs, metas):
                    ts_str = meta.get("timestamp", "")
                    try:
                        ts = datetime.fromisoformat(ts_str)
                    except ValueError:
                        ts = datetime.now(tz=timezone.utc)
                    records.append(
                        LogRecord(
                            id="",
                            service_name=meta.get("service_name", ""),
                            severity=meta.get("severity", ""),
                            body=doc,
                            timestamp=ts,
                            trace_id=meta.get("trace_id") or None,
                            document=doc,
                        )
                    )
                return records
            except Exception as exc:
                logger.error("LogStore.search ChromaDB query failed: %s", exc)

        # Memory fallback: substring match, newest first
        q_lower = query.lower()
        candidates = [
            r for r in self._memory
            if q_lower in r.body.lower()
            and (service_name is None or r.service_name == service_name)
        ]
        return sorted(candidates, key=lambda r: r.timestamp, reverse=True)[:top_k]

    def get_recent(
        self,
        service_name: Optional[str] = None,
        since_minutes: int = 60,
        limit: int = 100,
    ) -> List[LogRecord]:
        """Return recent log records, optionally filtered by service."""
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=since_minutes)
        results = [
            r for r in self._memory
            if r.timestamp >= cutoff
            and (service_name is None or r.service_name == service_name)
        ]
        return sorted(results, key=lambda r: r.timestamp, reverse=True)[:limit]

    def count(self) -> int:
        if self._chroma_collection is not None:
            try:
                return self._chroma_collection.count()
            except Exception:
                pass
        return len(self._memory)
