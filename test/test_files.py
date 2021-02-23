import sys
from datetime import datetime

sys.path.append(".")
from pydeltalake.lib import DeltaLake


def test_get_files_with_checkpoint():
    dl = DeltaLake("test/data/delta-0.2.0")
    res = {
        "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
        "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        "part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet",
    }
    files = dl.files()
    assert files == res


def test_get_files_without_checkpoint():
    dl = DeltaLake("test/data/delta_test")
    res = {
        "part-00000-f6a39956-2c3f-4c64-9a04-1393083bc46b-c000.snappy.parquet",
        "part-00001-5dc4e564-f217-4659-94db-cbf9264ea068-c000.snappy.parquet",
    }
    files = dl.files()
    assert files == res


def test_get_files_with_time_travel():
    timestamp = 1564524298
    time_travel = datetime.fromtimestamp(
        timestamp
    )  # datetime.datetime(2019, 7, 31, 0, 4, 58)
    dl = DeltaLake("test/data/delta-0.2.0", time_travel=time_travel)
    res = {
        "part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet",
        "part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet",
        "part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet",
        "part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet",
        "part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet",
        "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet",
    }
    files = dl.files()
    assert files == res
