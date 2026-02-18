from .binance import BinanceExchange
from .bitget import BitgetExchange
from .gate import GateExchange
from .kucoin import KucoinExchange
from .mexc import MexcExchange


def build_exchanges():
    return [
        GateExchange(),
        MexcExchange(),
        KucoinExchange(),
        BitgetExchange(),
        BinanceExchange(),
    ]


__all__ = ["build_exchanges"]
