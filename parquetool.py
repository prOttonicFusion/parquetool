import pqt.handlers as handlers
import argparse


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
    headCmdParser.set_defaults(func=handlers.headCmd)

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
    tailCmdParser.set_defaults(func=handlers.tailCmd)

    # Meta
    metaCmdParser = subparsers.add_parser("meta", help="Print the Parquet metadata")
    metaCmdParser.add_argument("filePath", help="Path to the Parquet file")
    metaCmdParser.set_defaults(func=handlers.metaCmd)

    # Schema
    schemaCmdParser = subparsers.add_parser(
        "schema", help="Print the Parquet table schema"
    )
    schemaCmdParser.add_argument("filePath", help="Path to the Parquet file")
    schemaCmdParser.set_defaults(func=handlers.schemaCmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
