"""The *business logic* shared by many demos: a tiny weather-station network.

Keeping the same domain across paradigms makes the point of Unit 0 visible:
the logic stays the same; only the way it is distributed changes
(message passing, sockets, RPC, REST, GraphQL, gRPC, microservices, FaaS...).
"""

from __future__ import annotations

import statistics
import threading
from dataclasses import asdict, dataclass, field
from typing import Any

INITIAL_READINGS: dict[str, list[float]] = {
    "bilbao": [17.5, 18.0, 19.2],
    "madrid": [24.1, 26.3, 25.0],
    "barcelona": [22.4, 23.0],
    "oslo": [8.2, 7.9, 9.1],
}


@dataclass
class Station:
    """A weather station and its temperature readings (°C)."""

    city: str
    readings: list[float] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        """Return aggregated statistics for the station."""
        r = self.readings
        return {
            "city": self.city,
            "count": len(r),
            "last": r[-1] if r else None,
            "mean": round(statistics.fmean(r), 2) if r else None,
            "min": min(r) if r else None,
            "max": max(r) if r else None,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dict (JSON friendly)."""
        return asdict(self)


class WeatherService:
    """Thread-safe in-memory weather service (the 'server-side object')."""

    def __init__(self) -> None:
        """Populate the service with :data:`INITIAL_READINGS`."""
        self._lock = threading.Lock()
        self._stations: dict[str, Station] = {
            c: Station(c, list(r)) for c, r in INITIAL_READINGS.items()
        }

    def cities(self) -> list[str]:
        """List known cities."""
        with self._lock:
            return sorted(self._stations)

    def get_temperature(self, city: str) -> float:
        """Return the latest temperature for ``city``.

        Raises:
            KeyError: If the city is unknown.
        """
        with self._lock:
            return self._stations[city.lower()].readings[-1]

    def report(self, city: str, temperature: float) -> dict[str, Any]:
        """Store a new reading, creating the station if needed.

        Returns:
            Station summary after the update.
        """
        with self._lock:
            st = self._stations.setdefault(city.lower(), Station(city.lower()))
            st.readings.append(float(temperature))
            return st.summary()

    def summary(self, city: str) -> dict[str, Any]:
        """Return aggregated statistics for ``city``."""
        with self._lock:
            return self._stations[city.lower()].summary()

    def station(self, city: str) -> Station:
        """Return the :class:`Station` object for ``city``."""
        with self._lock:
            return self._stations[city.lower()]
