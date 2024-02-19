import pyarrow as pa
import pandas as pd
from pyarrow.parquet import ParquetFile, read_metadata, read_schema


def headCmd(args):
    pf = ParquetFile(args.filePath)
    firstRows = next(pf.iter_batches(batch_size=args.row_count))
    df = pa.Table.from_batches([firstRows]).to_pandas()
    print(df.head(args.row_count))


# TODO: show actual tail row indices in the output
def tailCmd(args):
    n = args.row_count
    parquetFile = ParquetFile(args.filePath)
    rowGroupsCount = parquetFile.metadata.num_row_groups
    rowsRead = 0
    data = []

    # Iterate over row groups from last to first
    for rgi in range(rowGroupsCount - 1, -1, -1):
        rowGroup = parquetFile.read_row_group(rgi)
        rowCountInGroup = rowGroup.num_rows

        if rowsRead + rowCountInGroup >= n:
            # Row group contains more rows than we need to read
            start = max(0, rowCountInGroup - (n - rowsRead))
            data.append(rowGroup.slice(start).to_pandas())
            break
        else:
            data.append(rowGroup.to_pandas())
            rowsRead += rowCountInGroup

    # Concat the dataframes in reverse order since we read them from the end
    df = pd.concat(data[::-1], ignore_index=True)

    print(df.tail(args.row_count))


def metaCmd(args):
    metadata = read_metadata(args.filePath)
    print(metadata)


def schemaCmd(args):
    schema = read_schema(args.filePath)
    print(schema)
