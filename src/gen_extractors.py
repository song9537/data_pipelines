import pandas as pd


def parquet_df(filename: str, storage_dir, filetype: str = 'parquet') -> pd.DataFrame:
    """
    Loads the dataframe from the chosen format. Not sure if hdf file loading works, dont really care
    """
    assert filetype in ['parquet', 'hdf'], 'Unsupported file type. Choose either "parquet" or "hdf".'

    if filetype == 'parquet':
        return pd.read_parquet(os.path.join(storage_dir, filename + '.parquet'))
    elif filetype == 'hdf':
        return pd.read_hdf(os.path.join(storage_dir, filename + '.h5'))