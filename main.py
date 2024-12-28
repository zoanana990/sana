from fetcher import StockDataFetcher
from datetime import datetime

def update_stock_data(db_config, stock_list, start_date, end_date):
    """更新多支股票的資料"""
    for stock_no in stock_list:
        print(f"\nProcessing stock: {stock_no}")
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        try:
            fetcher.connect_db()
            fetcher.update_stock_data()
        except Exception as e:
            print(f"Error processing stock {stock_no}: {e}")
        finally:
            fetcher.disconnect_db()

if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "stock_data"
    }

    # 要下載的股票清單
    stock_list = ["3293"]  # 可以加入更多股票代碼
    
    # 設定下載區間
    start_date = datetime(2010, 1, 1)
    end_date = datetime.now()

    # # Fetch stock data
    # fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
    # fetcher.connect_db()
    #
    # data = fetcher.get_data_from_db()
    # fetcher.disconnect_db()
    #
    # # Set user-defined date range for analysis
    # user_start_date = "2014-12-01"
    # user_end_date = "2015-03-01"
    #
    # # Perform pattern analysis
    # analyzer = StockPatternAnalyzer(data, user_start_date, user_end_date)
    # analysis_result = analyzer.analyze()
    # print("Pattern analysis result:", analysis_result)
    #
    # # Plot the data with support/resistance lines
    # plotter = StockDataPlotter()
    # plotter.plot_kline_with_volume(data, user_start_date, user_end_date,
    #                                support=analysis_result['support'],
    #                                resistance=analysis_result['resistance'])
    # 執行更新
    update_stock_data(db_config, stock_list, start_date, end_date)