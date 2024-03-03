import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq


def headCmd(args):
    pf = pq.ParquetFile(args.filePath)
    firstRows = next(pf.iter_batches(batch_size=args.row_count))
    df = pa.Table.from_batches([firstRows]).to_pandas()
    print(df.head(args.row_count))


def tailCmd(args):
    n = args.row_count
    parquetFile = pq.ParquetFile(args.filePath)
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

    # Shift the df index to match the actual parquet row numbers
    rowCount = parquetFile.metadata.num_rows
    if args.row_count < rowCount:
        df.index += rowCount - n

    print(df.tail(args.row_count))


def metaCmd(args):
    metadata = pq.read_metadata(args.filePath)
    print(metadata)


def schemaCmd(args):
    schema = pq.read_schema(args.filePath)
    print(
        schema.to_string(
            truncate_metadata=False,
            show_field_metadata=args.show_metadata,
            show_schema_metadata=args.show_metadata,
        )
    )
