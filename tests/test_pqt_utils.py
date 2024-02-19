import os
import glob
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from pqt.utils import parquetToDataFrame


def cleanUpPqtFiles():
    for f in glob.glob("test_*.parquet"):
        try:
            os.remove(f)
        except IOError:
            pass


samplePqt = "test_utils.parquet"


class TestPqtUtils(unittest.TestCase):
    def setUp(self) -> None:
        cleanUpPqtFiles()
        return super().setUpClass()

    def tearDown(self) -> None:
        cleanUpPqtFiles()
        return super().tearDownClass()

    def test_parquetToDataFrame(self):
        dfOut = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        dfOut.to_parquet(samplePqt)

        dfIn = parquetToDataFrame(samplePqt)
        assert_frame_equal(dfIn, dfOut)
