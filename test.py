import polars as pl

positions_path = "2024-09-01_2025-08-11-grad-positions.csv"

positions_column_mapping = {
    'ReportDate': 'report_date',
    'ClientAccountID': 'client_account_id',
    'AssetClass': 'asset_class',
    'SubCategory': 'sub_category',
    'Description': 'description',
    'CUSIP': 'cusip',
    'ISIN': 'isin',
    'Symbol': 'symbol',
    'MarkPrice': 'mark_price',
    'Quantity': 'quantity',
    'FXRateToBase': 'fx_rate_to_base'
}

df_positions = (
    pl.read_csv(positions_path)
    .filter(pl.col('ClientAccountID').ne('ClientAccountID'))
    .select(positions_column_mapping.keys())
    .rename(positions_column_mapping)
    .with_columns(
        pl.col('report_date').str.strptime(pl.Date, "%Y%m%d"),
        pl.col('mark_price', 'fx_rate_to_base').cast(pl.Float64),
        pl.col('quantity').cast(pl.Int32)
    )
    .with_columns(
        pl.col('mark_price').mul('quantity').alias('value')
    )
)

# print(df_positions)

dividends_path = "2024-09-01_2025-08-11-grad-dividends.csv"

dividends_column_mapping = {
    'ReportDate': 'report_date',
    'ClientAccountID': 'client_account_id',
    'AssetClass': 'asset_class',
    'SubCategory': 'sub_category',
    'Description': 'description',
    'CUSIP': 'cusip',
    'ISIN': 'isin',
    'Symbol': 'symbol',
    'ActionID': 'action_id',
    'ExDate': 'ex_date',
    'PayDate': 'pay_date',
    'Quantity': 'quantity',
    'GrossRate': 'gross_rate',
    'GrossAmount': 'gross_amount',
    'Tax': 'tax',
    'Fee': 'fee',
    'NetAmount': 'net_amount',
}

df_dividends = (
    pl.read_csv(dividends_path)
    .filter(pl.col('ClientAccountID').ne('ClientAccountID'))
    .select(dividends_column_mapping.keys())
    .rename(dividends_column_mapping)
    .with_columns(
        pl.col('ex_date', 'pay_date').replace({"": None})
    )
    .with_columns(
        pl.col('report_date', 'ex_date', 'pay_date').str.strptime(pl.Date, "%Y%m%d"),
        pl.col('quantity').cast(pl.Int32),
        pl.col('gross_rate', 'gross_amount', 'net_amount', 'tax', 'fee').cast(pl.Float64)
    )
)

# print(df_dividends)

trades_path = "2024-09-01_2025-08-11-grad-trades.csv"

trades_column_mapping = {
    'ReportDate': 'report_date',
    'ClientAccountID': 'client_account_id',
    'AssetClass': 'asset_class',
    'SubCategory': 'sub_category',
    'Description': 'description',
    'CUSIP': 'cusip',
    'ISIN': 'isin',
    'Symbol': 'symbol',
    'TradeID': 'trade_id',
    'Quantity': 'quantity',
    'TradePrice': 'trade_price',
    'IBCommission': 'ib_commission',
    'Buy/Sell': 'buy_sell',
}

df_trades = (
    pl.read_csv(trades_path)
    .filter(pl.col('ClientAccountID').ne('ClientAccountID'))
    .select(trades_column_mapping.keys())
    .rename(trades_column_mapping)
    .with_columns(
        pl.col('report_date').str.strptime(pl.Date, "%Y%m%d"),
        pl.col('quantity').cast(pl.Int32),
        pl.col('trade_price', 'ib_commission').cast(pl.Float64)
    )
)

print(df_trades)