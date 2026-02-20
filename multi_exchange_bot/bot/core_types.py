from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional


CanonicalSymbol = str  # BASE/QUOTE
OrderSymbol = str
TradeUrlSymbol = str

ListingStatus = Literal["candidate", "verified"]
ListingSource = Literal["scanner", "announcement", "manual"]
ScanMode = Literal["NORMAL", "ALARM", "HOT"]


@dataclass
class ListingEvent:
    exchange_id: str
    canonical_symbol: CanonicalSymbol
    detected_at: str
    market_type: str = "spot"
    status: ListingStatus = "candidate"
    order_symbol: Optional[OrderSymbol] = None
    trade_url: Optional[str] = None
    source: ListingSource = "scanner"
    run_id: str = ""
    title: str = ""
    url: str = ""
    source_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchangeId": self.exchange_id,
            "canonicalSymbol": self.canonical_symbol,
            "detectedAt": self.detected_at,
            "marketType": self.market_type,
            "status": self.status,
            "orderSymbol": self.order_symbol,
            "tradeUrl": self.trade_url,
            "source": self.source,
            "runId": self.run_id,
            "title": self.title,
            "url": self.url,
            "sourceType": self.source_type,
        }


@dataclass
class ExchangeScanStats:
    mode: ScanMode = "NORMAL"
    current_interval_ms: int = 300000
    market_count: int = 0
    candidate_new_count: int = 0
    verified_new_count: int = 0
    fetch_duration_ms: int = 0
    last_success_at: str = ""
    last_error_type: str = ""
    last_error_message: str = ""
    consecutive_errors: int = 0
    rate_limit_hits: int = 0
    backoff_until: int = 0
    backoff_level: int = 0
    in_flight: bool = False
    next_run_at_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "currentIntervalMs": self.current_interval_ms,
            "marketCount": self.market_count,
            "candidateNewCount": self.candidate_new_count,
            "verifiedNewCount": self.verified_new_count,
            "fetchDurationMs": self.fetch_duration_ms,
            "lastSuccessAt": self.last_success_at,
            "lastErrorType": self.last_error_type,
            "lastErrorMessage": self.last_error_message,
            "consecutiveErrors": self.consecutive_errors,
            "rateLimitHits": self.rate_limit_hits,
            "backoffUntil": self.backoff_until,
            "backoffLevel": self.backoff_level,
            "inFlight": self.in_flight,
            "nextRunAtMs": self.next_run_at_ms,
        }


@dataclass
class ScanStats:
    run_id: str = ""
    started_at: str = ""
    ended_at: str = ""
    duration_ms: int = 0
    per_exchange: dict[str, ExchangeScanStats] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "runId": self.run_id,
            "startedAt": self.started_at,
            "endedAt": self.ended_at,
            "durationMs": self.duration_ms,
            "perExchange": {k: v.to_dict() for k, v in self.per_exchange.items()},
        }
