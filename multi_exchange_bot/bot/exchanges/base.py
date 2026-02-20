from abc import ABC, abstractmethod
from typing import Optional

class Exchange(ABC):
    name: str

    @abstractmethod
    def market_buy_quote(self, symbol: str, quote_qty: str, timeout_sec: Optional[float] = None) -> dict: ...
    @abstractmethod
    def market_sell_base(self, symbol: str, base_qty: str, timeout_sec: Optional[float] = None) -> dict: ...
    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str: ...
    @abstractmethod
    def probe_order_rtt(self, symbol: str, quote_qty: str) -> dict: ...

    def warmup_connection(self, **kwargs) -> dict:
        return {"ok": False, "message": "warmup_not_supported"}
