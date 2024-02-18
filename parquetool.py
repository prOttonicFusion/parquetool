import argparse
import pandas as pd
from pyarrow.parquet import ParquetFile, read_metadata, read_schema
import pyarrow as pa


def parquetToDataFrame(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df


def headCmd(args):
    pf = ParquetFile(args.filePath)
    firstRows = next(pf.iter_batches(batch_size=args.row_count))
    df = pa.Table.from_batches([firstRows]).to_pandas()
    print(df.head(args.row_count))


def tailCmd(args):
    df = parquetToDataFrame(args.filePath)
    print(df.tail(args.row_count))


def metaCmd(args):
    metadata = read_metadata(args.filePath)
    print(metadata)


def schemaCmd(args):
    schema = read_schema(args.filePath)
    print(schema)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    # Head
    headCmdParser = subparsers.add_parser(
        "head", help="View the first N rows of the Parquet"
    )
    headCmdParser.add_argument("filePath", help="Path to the Parquet file")
    headCmdParser.add_argument(
        "-n",
        "--number",
        action="store",
        dest="row_count",
        help="Number of rows to show",
        type=int,
        default=5,
    )
    headCmdParser.set_defaults(func=headCmd)

    # Tail
    tailCmdParser = subparsers.add_parser(
        "tail", help="View the last N rows of the Parquet"
    )
    tailCmdParser.add_argument("filePath", help="Path to the Parquet file")
    tailCmdParser.add_argument(
        "-n",
        "--number",
        action="store",
        dest="row_count",
        help="Number of rows to show",
        type=int,
        default=5,
    )
    tailCmdParser.set_defaults(func=tailCmd)

    # Meta
    metaCmdParser = subparsers.add_parser("meta", help="Print the Parquet metadata")
    metaCmdParser.add_argument("filePath", help="Path to the Parquet file")
    metaCmdParser.set_defaults(func=metaCmd)

    # Schema
    schemaCmdParser = subparsers.add_parser(
        "schema", help="Print the Parquet table schema"
    )
    schemaCmdParser.add_argument("filePath", help="Path to the Parquet file")
    schemaCmdParser.set_defaults(func=schemaCmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
