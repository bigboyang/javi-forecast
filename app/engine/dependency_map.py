"""Service dependency map.

Stores a directed graph of service dependencies inferred from
Granger causality tests.  Edges represent: service A's error_rate
Granger-causes service B's error_rate at a given significance level.

Thread-safe via threading.Lock so it can be updated from thread-pool
executor workers (statsmodels is CPU-bound) and read from the asyncio
event loop (API handlers).
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class DependencyEdge:
    """A directed causal edge: source → target."""

    source: str          # upstream service (cause)
    target: str          # downstream service (effect)
    p_value: float       # Granger test p-value (lower = stronger evidence)
    max_lag: int         # lag at which causality was strongest
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class DependencyMap:
    """Thread-safe directed causal dependency graph.

    Edges are keyed by (source, target).  A new test result replaces any
    existing edge for that pair; if the new p-value exceeds the threshold
    the edge is removed (relationship no longer significant).

    Parameters
    ----------
    p_value_threshold:
        Maximum p-value for an edge to be retained (default: 0.05).
    """

    def __init__(self, p_value_threshold: float = 0.05) -> None:
        self.p_value_threshold = p_value_threshold
        self._edges: Dict[Tuple[str, str], DependencyEdge] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write API (called from executor threads)
    # ------------------------------------------------------------------

    def update(
        self,
        source: str,
        target: str,
        p_value: float,
        max_lag: int,
    ) -> None:
        """Record or remove a causal relationship based on p_value."""
        with self._lock:
            if p_value <= self.p_value_threshold:
                self._edges[(source, target)] = DependencyEdge(
                    source=source,
                    target=target,
                    p_value=p_value,
                    max_lag=max_lag,
                )
            else:
                self._edges.pop((source, target), None)

    # ------------------------------------------------------------------
    # Read API (called from event loop and API handlers)
    # ------------------------------------------------------------------

    def get_causes(self, service: str) -> List[DependencyEdge]:
        """Return edges where *service* is the downstream (effect/target)."""
        with self._lock:
            return [e for e in self._edges.values() if e.target == service]

    def get_effects(self, service: str) -> List[DependencyEdge]:
        """Return edges where *service* is the upstream (cause/source)."""
        with self._lock:
            return [e for e in self._edges.values() if e.source == service]

    def get_all_edges(self) -> List[DependencyEdge]:
        """Return a snapshot of all current edges."""
        with self._lock:
            return list(self._edges.values())

    def get_root_causes(self, service: str) -> List[str]:
        """Return upstream root-cause services for *service*.

        Performs a BFS traversal upstream from *service* and returns
        services that either have no further upstream cause (leaf nodes)
        or are direct causes if no true root is found.
        """
        with self._lock:
            visited: Set[str] = set()
            queue: List[str] = [service]
            roots: List[str] = []

            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                causes = [
                    e.source
                    for e in self._edges.values()
                    if e.target == current
                ]
                if not causes and current != service:
                    roots.append(current)
                else:
                    for c in causes:
                        if c not in visited:
                            queue.append(c)

            # Fallback: return direct causes if BFS found no pure root nodes
            if not roots:
                roots = list({
                    e.source
                    for e in self._edges.values()
                    if e.target == service
                })
            return roots
