import pytest

from app.avro_codec import decode_avro, encode_avro
from app.processing import (
    OrderProcessor,
    PermanentProcessingError,
    RunningAverage,
    TemporaryProcessingError,
)


def test_order_avro_round_trip():
    order = {"orderId": "1001", "product": "Item1", "price": 19.5}
    payload = encode_avro(order, "order.avsc")
    assert isinstance(payload, bytes)
    assert decode_avro(payload, "order.avsc") == order


def test_running_average():
    average = RunningAverage()
    assert average.add(10.0) == 10.0
    assert average.add(20.0) == 15.0
    assert average.count == 2


def test_temporary_failure_recovers_on_third_attempt():
    processor = OrderProcessor()
    order = {"orderId": "1", "product": "TEMPORARY_FAIL", "price": 25.0}
    with pytest.raises(TemporaryProcessingError):
        processor.process(order)
    with pytest.raises(TemporaryProcessingError):
        processor.process(order)
    processor.process(order)


def test_permanent_failure_is_not_retryable():
    processor = OrderProcessor()
    with pytest.raises(PermanentProcessingError):
        processor.process({"orderId": "2", "product": "PERMANENT_FAIL", "price": 25.0})


@pytest.mark.parametrize("price", [0, -1, float("inf"), float("nan")])
def test_invalid_price_is_permanent(price):
    processor = OrderProcessor()
    with pytest.raises(PermanentProcessingError):
        processor.process({"orderId": "3", "product": "Book", "price": price})
