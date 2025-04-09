import polars as pl


def parquet_columns_naming(df):
    old_headers = df.columns

    new_column_names = [f"column_{i}" for i in range(len(old_headers))]

    header_row = pl.DataFrame([old_headers])
    df = df.rename(dict(zip(df.columns, new_column_names)))
    header_row = header_row.rename(
        dict(zip(header_row.columns, new_column_names))
    )
    result_df = pl.concat([header_row, df], how="vertical")
    return result_df
