import pandas as pd
def extend_time_series_data(data, date_column, v_column, value_columns, freq):# {{{
    """Fill zero values for missing dates, return pandas dataframe
    """
    data = data.reset_index(drop=True)
    date_first = data[date_column].min()
    date_last = data[date_column].max()
    for colname in value_columns:
        data[colname] = pd.to_numeric(data[colname])
    date_range = [pd.date_range(date_first, date_last, freq=freq), data[v_column].unique()]
    print(date_range)
    mux = pd.MultiIndex.from_product(date_range, names=[date_column, v_column])
    result = data.set_index([date_column, v_column]).reindex(mux, fill_value=0).reset_index()
    result = result[['Date'] + value_columns]
    return result
# }}}
def extend_data(datafile, value_columns, freq, outfile):# {{{
    """Fill zero values for missing dates, write the TSV data to the outfile
    """
    df = pd.read_csv(datafile, sep='\t')
    df.insert(0, 'forMulti', ['A']*len(df))
    df['Date'] = pd.to_datetime(df['Date'])
    newdf = extend_time_series_data(df, date_column='Date', v_column='forMulti', value_columns=value_columns, freq=freq)
    newdf.to_csv(outfile, sep='\t', index=False)
#}}}
def date_range_frequency(filename):# {{{
    """Get the frequency of date range"""
    if filename.find('day') != -1:
        freq = 'D'
    elif filename.find('week') != -1:
        freq = 'W-MON'
    elif filename.find('month') != -1:
        freq = 'MS'
    elif filename.find('year') != -1:
        freq = 'AS-JAN'
    else:
        freq = 'D'
    return freq
# }}}
