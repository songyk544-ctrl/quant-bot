import io
import unittest
import zipfile

from services.stock_universe_service import KIS_MASTER_SPECS, _parse_market_master


def _build_master_zip(market_name, code, name, price, market_cap):
    spec = KIS_MASTER_SPECS[market_name]
    fields = ["0" * width for width in spec["widths"]]
    fields[spec["price_index"]] = str(price).rjust(spec["widths"][spec["price_index"]], "0")
    fields[spec["market_cap_index"]] = str(market_cap).rjust(
        spec["widths"][spec["market_cap_index"]], "0"
    )
    tail = "".join(fields)
    head = code.ljust(9) + "KR7000000000" + name
    payload = (head + tail + "\n").encode("cp949")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(f"{market_name.lower()}_code.mst", payload)
    return buffer.getvalue()


class StockUniverseServiceTest(unittest.TestCase):
    def test_parses_kospi_fixed_width_master(self):
        content = _build_master_zip("KOSPI", "005930", "삼성전자", 85000, 5000000)
        frame = _parse_market_master(content, "KOSPI")
        row = frame.iloc[0]
        self.assertEqual(row["종목코드"], "005930")
        self.assertEqual(row["종목명"], "삼성전자")
        self.assertEqual(row["현재가"], 85000)
        self.assertEqual(row["시가총액"], 5000000)

    def test_parses_kosdaq_fixed_width_master(self):
        content = _build_master_zip("KOSDAQ", "035900", "JYP Ent.", 72000, 25000)
        frame = _parse_market_master(content, "KOSDAQ")
        row = frame.iloc[0]
        self.assertEqual(row["종목코드"], "035900")
        self.assertEqual(row["현재가"], 72000)
        self.assertEqual(row["시가총액"], 25000)


if __name__ == "__main__":
    unittest.main()
