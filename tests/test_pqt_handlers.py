import argparse
import os
import sys
import glob
import unittest
import pandas as pd
from io import StringIO
from pqt.handlers import headCmd, tailCmd, metaCmd, schemaCmd

mockDf = pd.DataFrame({"A": [1, 2, 3], "B": [4.56, 5.67, 6.8910]})

samplePqt = "test_handlers.parquet"


def cleanUpPqtFiles():
    for f in glob.glob("test_*.parquet"):
        try:
            os.remove(f)
        except IOError:
            pass


class TestPqtUtils(unittest.TestCase):
    def setUp(self) -> None:
        cleanUpPqtFiles()
        self.capturedOutput = StringIO()
        sys.stdout = self.capturedOutput
        return super().setUpClass()

    def tearDown(self) -> None:
        cleanUpPqtFiles()
        sys.stdout = sys.__stdout__
        return super().tearDownClass()

    def assertOutput(self, expectedOutput: str):
        self.assertEqual(self.capturedOutput.getvalue().strip(), expectedOutput.strip())

    def assertInOutput(self, expectedOutputPart: str):
        self.assertIn(expectedOutputPart, self.capturedOutput.getvalue())

    def test_headCmd(self):
        mockDf.to_parquet(samplePqt)
        headCmd(
            argparse.Namespace(
                filePath=samplePqt,
                row_count=2,
            )
        )
        self.assertOutput("A     B\n0  1  4.56\n1  2  5.67\n")

    def test_tailmd(self):
        mockDf.to_parquet(samplePqt)
        tailCmd(
            argparse.Namespace(
                filePath=samplePqt,
                row_count=2,
            )
        )
        self.assertOutput("A      B\n1  2  5.670\n2  3  6.891\n")

    def test_metaCmd(self):
        mockDf.to_parquet(samplePqt)
        metaCmd(
            argparse.Namespace(
                filePath=samplePqt,
            )
        )
        self.assertInOutput("num_columns: 2")
        self.assertInOutput("num_rows: 3")
        self.assertInOutput("format_version:")

    def test_schemaCmd(self):
        mockDf.to_parquet(samplePqt)
        schemaCmd(
            argparse.Namespace(
                filePath=samplePqt,
            )
        )
        self.assertInOutput("A: int64")
        self.assertInOutput("B: double")
