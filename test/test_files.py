import os
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
