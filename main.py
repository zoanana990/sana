from fetcher import StockDataFetcher
from analyzer import StockPatternAnalyzer, StockPattern
from plotter import StockDataPlotter
from datetime import datetime

if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "stock_data"
    }

    stock_no = "2330"
    start_date = datetime(2011, 1, 1)
    end_date = datetime.now()

    # Fetch stock data
    fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
    fetcher.connect_db()

    data = fetcher.get_data_from_db()
    fetcher.disconnect_db()

    # Set user-defined date range for analysis
    user_start_date = "2014-12-01"
    user_end_date = "2015-03-01"

    # Perform pattern analysis
    analyzer = StockPatternAnalyzer(data, user_start_date, user_end_date)
    analysis_result = analyzer.analyze()
    print("Pattern analysis result:", analysis_result)

    # Plot the data with support/resistance lines
    plotter = StockDataPlotter()
    plotter.plot_kline_with_volume(data, user_start_date, user_end_date,
                                   support=analysis_result['support'],
                                   resistance=analysis_result['resistance'])