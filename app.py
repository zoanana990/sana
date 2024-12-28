import sys
import signal
from datetime import datetime, timedelta
import multiprocessing
import readline
from fetcher import StockDataFetcher
from analyzer import StockPatternAnalyzer
from plotter import StockDataPlotter
import mysql.connector
import platform

def update_worker(stock_no: str, db_config, debug_mode=False):
    """Worker for updating stock data"""
    def log(message):
        if debug_mode:
            print(message)
            
    try:
        start_date = datetime(2010, 1, 1)
        end_date = datetime.now()
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        fetcher.connect_db()

        try:
            # Check if stock data exists in database
            cursor = fetcher.db_connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM stock_prices 
                WHERE stock_no = %s
            """, (stock_no,))
            count = cursor.fetchone()[0]
            cursor.close()

            if count == 0:
                log(f"No data found for stock {stock_no}, fetching from TWSE...")
                try:
                    fetcher.update_stock_data(debug_mode)
                    log(f"Successfully downloaded data for stock {stock_no}")
                except Exception as e:
                    log(f"Failed to fetch data from TWSE: {e}")
                    log("Response details: " + str(getattr(e, 'response', None)))
            else:
                # Check last update date
                cursor = fetcher.db_connection.cursor()
                cursor.execute("""
                    SELECT MAX(date) 
                    FROM stock_prices 
                    WHERE stock_no = %s
                """, (stock_no,))
                last_update = cursor.fetchone()[0]
                cursor.close()

                if last_update:
                    # Update data if last update is not today
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
    """Worker for plotting stock charts"""
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
    """Worker for analyzing stock patterns"""
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
    """Worker for listing stock data summary"""
    try:
        fetcher = StockDataFetcher(db_config, stock_no or "", None, None)
        fetcher.connect_db()
        cursor = fetcher.db_connection.cursor()
        
        if stock_no:
            # Show detailed data for specific stock
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
            
            # Show statistics
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
            # Show summary for all stocks
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
        self.process_info = {}  # 儲存進程資訊
        self.debug_mode = False  # debug 模式開關
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # 設置命令歷史
        self.history_file = ".stock_app_history"
        try:
            readline.read_history_file(self.history_file)
            readline.set_history_length(1000)  # 設置歷史記錄最大數量
        except FileNotFoundError:
            pass
        
        # 根據平台設置 readline
        if platform.system() == 'Windows':
            try:
                import pyreadline3
            except ImportError:
                print("Tip: Install pyreadline3 for command history support:")
                print("pip install pyreadline3")
        
    def __del__(self):
        # 保存命令歷史
        try:
            readline.write_history_file(self.history_file)
        except Exception as e:
            print(f"Error saving history: {e}")

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

    def log(self, message):
        """根據 debug 模式決定是否輸出日誌"""
        if self.debug_mode:
            print(message)

    def update_stock(self, stock_no):
        process = multiprocessing.Process(
            target=update_worker,
            args=(stock_no, self.db_config, self.debug_mode)  # 傳遞 debug 模式
        )
        self.processes.append(process)
        process.start()
        self.process_info[process.pid] = {
            'type': 'update',
            'stock_no': stock_no,
            'start_time': datetime.now(),
            'status': 'running'
        }
        print(f"Started update process for stock {stock_no} (PID: {process.pid})")

    def check_processes(self):
        """檢查所有進程的狀態並更新資訊"""
        active_processes = []
        for process in self.processes:
            if process.is_alive():
                active_processes.append(process)
            else:
                if process.pid in self.process_info:
                    self.process_info[process.pid]['status'] = 'completed'
                    if not self.debug_mode:
                        print(f"Process {process.pid} ({self.process_info[process.pid]['type']}) completed")
        self.processes = active_processes

    def show_status(self):
        """顯示所有進程的狀態"""
        self.check_processes()
        print("\nCurrent processes status:")
        print("PID      | Type     | Stock | Start Time          | Status")
        print("-" * 60)
        for pid, info in self.process_info.items():
            start_time = info['start_time'].strftime('%Y-%m-%d %H:%M:%S')
            print(f"{pid:<8} | {info['type']:<8} | {info['stock_no']:<5} | {start_time} | {info['status']}")

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
        print(" - update <stock_number>")
        print(" - plot <stock_number> [start_date] [end_date]")
        print(" - analyze <stock_number> [start_date] [end_date]")
        print(" - list [stock_number]")
        print(" - debug on|off")
        print(" - status")
        print(" - exit")
        print("Date format: YYYY-MM-DD")
        print("\nTip: Use Up/Down arrows to navigate command history")

        while True:
            try:
                command = input("\n> ").strip()
                if command:
                    readline.add_history(command)
                
                command = command.split()
                if not command:
                    continue

                if command[0] == "exit":
                    print("Cleaning up and exiting...")
                    self.cleanup()
                    break

                elif command[0] == "debug":
                    if len(command) != 2 or command[1] not in ['on', 'off']:
                        print("Usage: debug on|off")
                        continue
                    self.debug_mode = (command[1] == 'on')
                    print(f"Debug mode: {'on' if self.debug_mode else 'off'}")

                elif command[0] == "status":
                    self.show_status()

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
                    print("  debug on|off")
                    print("  status")
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