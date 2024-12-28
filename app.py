import sys
import signal
from datetime import datetime, timedelta
import multiprocessing
from fetcher import StockDataFetcher
from analyzer import StockPatternAnalyzer
from plotter import StockDataPlotter
import mysql.connector

def update_worker(stock_no: str, db_config):
    """更新股票資料的 worker"""
    try:
        start_date = datetime(2010, 1, 1)
        end_date = datetime.now()
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        fetcher.connect_db()

        try:
            # 檢查資料庫中是否已有此股票資料
            cursor = fetcher.db_connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM stock_prices 
                WHERE stock_no = %s
            """, (stock_no,))
            count = cursor.fetchone()[0]
            cursor.close()

            if count == 0:
                print(f"No data found for stock {stock_no}, fetching from TWSE...")
                try:
                    fetcher.update_stock_data()
                    print(f"Successfully downloaded data for stock {stock_no}")
                except Exception as e:
                    print(f"Failed to fetch data from TWSE: {e}")
                    print("Response details:", getattr(e, 'response', None))
            else:
                # 檢查最後更新日期
                cursor = fetcher.db_connection.cursor()
                cursor.execute("""
                    SELECT MAX(date) 
                    FROM stock_prices 
                    WHERE stock_no = %s
                """, (stock_no,))
                last_update = cursor.fetchone()[0]
                cursor.close()

                if last_update:
                    # 如果最後更新日期不是今天，則更新資料
                    if last_update.date() < datetime.now().date():
                        print(f"Updating data for stock {stock_no} from {last_update.date()}")
                        fetcher.start_date = last_update + timedelta(days=1)
                        fetcher.update_stock_data()
                    else:
                        print(f"Data for stock {stock_no} is already up to date")
                else:
                    print(f"Updating all data for stock {stock_no}")
                    fetcher.update_stock_data()

        except mysql.connector.Error as e:
            print(f"Database error: {e}")
            print(f"Error code: {e.errno}")
            print(f"SQLSTATE: {e.sqlstate}")
            print(f"Error message: {e.msg}")
        
        fetcher.disconnect_db()
    except Exception as e:
        print(f"Error updating stock {stock_no}: {e}")
        import traceback
        print("Traceback:")
        traceback.print_exc()

def plot_worker(stock_no, start_date, end_date, db_config):
    """繪製股票圖表的 worker"""
    try:
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        fetcher.connect_db()
        data = fetcher.get_data_from_db()
        fetcher.disconnect_db()

        plotter = StockDataPlotter()
        plotter.plot_kline_with_volume(data, start_date, end_date)
    except Exception as e:
        print(f"Error plotting stock {stock_no}: {e}")

def analyze_worker(stock_no, start_date, end_date, db_config):
    """分析股票的 worker"""
    try:
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        fetcher.connect_db()
        data = fetcher.get_data_from_db()
        fetcher.disconnect_db()

        analyzer = StockPatternAnalyzer(data, start_date, end_date)
        analysis_result = analyzer.analyze()
        print(f"\nAnalysis result for {stock_no}:")
        print(f"Support: {analysis_result['support']}")
        print(f"Resistance: {analysis_result['resistance']}")
        print(f"Is consolidation: {analysis_result['is_consolidation']}")
        print(f"Support touches: {analysis_result['support_touches']}")
        print(f"Resistance touches: {analysis_result['resistance_touches']}")
        print(f"Is breakout: {analysis_result['is_breakout']}")
        print(f"Is breakdown: {analysis_result['is_breakdown']}")

        # Plot with analysis results
        plotter = StockDataPlotter()
        plotter.plot_kline_with_volume(
            data, start_date, end_date,
            support=analysis_result['support'],
            resistance=analysis_result['resistance']
        )
    except Exception as e:
        print(f"Error analyzing stock {stock_no}: {e}")

def list_worker(db_config, stock_no=None):
    """列出資料庫中的股票資料概要"""
    try:
        fetcher = StockDataFetcher(db_config, stock_no or "", None, None)
        fetcher.connect_db()
        cursor = fetcher.db_connection.cursor()
        
        if stock_no:
            # 顯示特定股票的詳細資料
            cursor.execute("""
                SELECT date, 
                       volume,
                       turnover,
                       open_price,
                       high_price,
                       low_price,
                       close_price,
                       transaction_count
                FROM stock_prices 
                WHERE stock_no = %s
                ORDER BY date DESC
                LIMIT 10
            """, (stock_no,))
            
            results = cursor.fetchall()
            if not results:
                print(f"\nNo data found for stock {stock_no}")
                return
                
            print(f"\nLatest 10 records for stock {stock_no}:")
            print("Date       | Volume    | Open  | High  | Low   | Close | Trans")
            print("-" * 70)
            for row in results:
                print(f"{row[0]} | {row[1]:9d} | {row[3]:5.2f} | {row[4]:5.2f} | {row[5]:5.2f} | {row[6]:5.2f} | {row[7]:6d}")
            
            # 顯示統計資訊
            cursor.execute("""
                SELECT MIN(date) as first_date,
                       MAX(date) as last_date,
                       COUNT(*) as total_records,
                       MIN(close_price) as min_price,
                       MAX(close_price) as max_price,
                       AVG(close_price) as avg_price
                FROM stock_prices
                WHERE stock_no = %s
            """, (stock_no,))
            
            stats = cursor.fetchone()
            print(f"\nStatistics for {stock_no}:")
            print(f"Date range: {stats[0]} to {stats[1]}")
            print(f"Total records: {stats[2]}")
            print(f"Price range: {stats[3]:.2f} - {stats[4]:.2f} (avg: {stats[5]:.2f})")
            
        else:
            # 顯示所有股票的概要
            cursor.execute("""
                SELECT stock_no, 
                       MIN(date) as first_date, 
                       MAX(date) as last_date,
                       COUNT(*) as total_records,
                       MIN(close_price) as min_price,
                       MAX(close_price) as max_price
                FROM stock_prices
                GROUP BY stock_no
                ORDER BY stock_no
            """)
            
            results = cursor.fetchall()
            if not results:
                print("\nNo data in database")
                return
                
            print("\nStock data in database:")
            print("Stock No | First Date  | Last Date   | Records | Price Range")
            print("-" * 65)
            for row in results:
                print(f"{row[0]:<8} | {row[1]} | {row[2]} | {row[3]:>7} | {row[4]:6.2f} - {row[5]:6.2f}")
            
        cursor.close()
        fetcher.disconnect_db()
            
    except Exception as e:
        print(f"Error listing stocks: {e}")
        import traceback
        traceback.print_exc()

class StockApp:
    def __init__(self):
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "stock_data"
        }
        self.processes = []  # 追踪所有子進程
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\nReceived Ctrl+C, cleaning up...")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        for process in self.processes:
            if process.is_alive():
                print(f"Terminating process {process.pid}")
                process.terminate()
                process.join()
        self.processes = []

    def update_stock(self, stock_no):
        process = multiprocessing.Process(
            target=update_worker,
            args=(stock_no, self.db_config)
        )
        self.processes.append(process)
        process.start()
        print(f"Started update process for stock {stock_no}")

    def plot_stock(self, stock_no, start_date=None, end_date=None):
        process = multiprocessing.Process(
            target=plot_worker,
            args=(stock_no, start_date, end_date, self.db_config)
        )
        self.processes.append(process)
        process.start()
        print(f"Started plotting process for stock {stock_no}")

    def analyze_stock(self, stock_no, start_date=None, end_date=None):
        process = multiprocessing.Process(
            target=analyze_worker,
            args=(stock_no, start_date, end_date, self.db_config)
        )
        self.processes.append(process)
        process.start()
        print(f"Started analysis process for stock {stock_no}")

    def parse_date(self, date_str):
        if date_str:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                print(f"Invalid date format: {date_str}. Please use YYYY-MM-DD")
                return None
        return None

    def run(self):
        print("Welcome to Stock Analysis App")
        print("Available commands:")
        print("  update <stock_number>")
        print("  plot <stock_number> [start_date] [end_date]")
        print("  analyze <stock_number> [start_date] [end_date]")
        print("  list")
        print("  exit")
        print("Date format: YYYY-MM-DD")

        while True:
            try:
                command = input("\n> ").strip().split()
                if not command:
                    continue

                if command[0] == "exit":
                    print("Cleaning up and exiting...")
                    self.cleanup()
                    break

                elif command[0] == "update":
                    if len(command) != 2:
                        print("Usage: update <stock_number>")
                        continue
                    self.update_stock(command[1])

                elif command[0] == "plot":
                    if len(command) < 2 or len(command) > 4:
                        print("Usage: plot <stock_number> [start_date] [end_date]")
                        continue
                    start_date = self.parse_date(command[2]) if len(command) > 2 else None
                    end_date = self.parse_date(command[3]) if len(command) > 3 else None
                    self.plot_stock(command[1], start_date, end_date)

                elif command[0] == "analyze":
                    if len(command) < 2 or len(command) > 4:
                        print("Usage: analyze <stock_number> [start_date] [end_date]")
                        continue
                    start_date = self.parse_date(command[2]) if len(command) > 2 else None
                    end_date = self.parse_date(command[3]) if len(command) > 3 else None
                    self.analyze_stock(command[1], start_date, end_date)

                elif command[0] == "list":
                    if len(command) > 2:
                        print("Usage: list [stock_number]")
                        continue
                    stock_no = command[1] if len(command) == 2 else None
                    self.list_stocks(stock_no)

                else:
                    print("Unknown command. Available commands:")
                    print("  update <stock_number>")
                    print("  plot <stock_number> [start_date] [end_date]")
                    print("  analyze <stock_number> [start_date] [end_date]")
                    print("  list")
                    print("  exit")

            except Exception as e:
                print(f"Error: {e}")

    def list_stocks(self, stock_no=None):
        process = multiprocessing.Process(
            target=list_worker,
            args=(self.db_config, stock_no)
        )
        self.processes.append(process)
        process.start()
        print("Started listing process")

if __name__ == "__main__":
    app = StockApp()
    app.run() 