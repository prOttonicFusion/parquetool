import pyarrow as pa
from pqt.utils import parquetToDataFrame
from pyarrow.parquet import ParquetFile, read_metadata, read_schema


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
