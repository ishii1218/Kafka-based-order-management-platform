from __future__ import annotations

import math
from dataclasses import dataclass


class TemporaryProcessingError(RuntimeError):
    """A recoverable failure that should be retried."""


class PermanentProcessingError(ValueError):
    """A bad message that should go directly to the DLQ."""


@dataclass
class RunningAverage:
    count: int = 0
    total: float = 0.0

    def add(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.average

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


class OrderProcessor:
    """Validates orders and supplies deterministic failures for the live demo."""

    def __init__(self) -> None:
        self._attempts: dict[str, int] = {}

    def process(self, order: dict) -> None:
        order_id = order.get("orderId")
        product = order.get("product")
        price = order.get("price")

        if not isinstance(order_id, str) or not order_id.strip():
            raise PermanentProcessingError("orderId must be a non-empty string")
        if not isinstance(product, str) or not product.strip():
            raise PermanentProcessingError("product must be a non-empty string")
        if not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
            raise PermanentProcessingError("price must be a finite positive number")

        attempt = self._attempts.get(order_id, 0) + 1
        self._attempts[order_id] = attempt

        if product == "PERMANENT_FAIL":
            raise PermanentProcessingError("simulated invalid product")
        if product == "ALWAYS_TEMP_FAIL":
            raise TemporaryProcessingError("simulated dependency remains unavailable")
        if product == "TEMPORARY_FAIL" and attempt <= 2:
            raise TemporaryProcessingError(
                f"simulated dependency unavailable on attempt {attempt}"
            )

