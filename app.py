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
import pandas as pd

def update_worker(stock_no: str, db_config, debug_mode=False):
    """
    Worker for updating stock data
    """
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
            cursor.execute(fetcher.queries['basic']['Check stock exists'], (stock_no,))
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
                cursor.execute(fetcher.queries['basic']['Get last update date for stock'], (stock_no,))
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
        print()
        sys.stdout.flush()
    except Exception as e:
        print(f"Error updating stock {stock_no}: {e}")
        import traceback
        print("Traceback:")
        traceback.print_exc()
        sys.stdout.flush()

def plot_worker(stock_no, start_date, end_date, db_config, period='D'):
    """
    Worker for plotting stock charts
    period: 'D' for daily, 'W' for weekly, 'M' for monthly
    """
    try:
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        fetcher.connect_db()
        data = fetcher.get_aggregated_data_from_db(period)
        fetcher.disconnect_db()

        period_text = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly'}[period]
        plotter = StockDataPlotter()
        plotter.plot_kline_with_volume(data, start_date, end_date, 
                                     title=f'{period_text} K-Line Chart - {stock_no}')
        sys.stdout.flush()
    except Exception as e:
        print(f"Error plotting stock {stock_no}: {e}")
        sys.stdout.flush()

def analyze_worker(stock_no, start_date, end_date, db_config, period='D'):
    """
    Worker for analyzing stock patterns
    period: 'D' for daily, 'W' for weekly, 'M' for monthly
    """
    try:
        fetcher = StockDataFetcher(db_config, stock_no, start_date, end_date)
        fetcher.connect_db()
        data = fetcher.get_aggregated_data_from_db(period)
        fetcher.disconnect_db()

        period_text = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly'}[period]
        print(f"\n{period_text} Analysis result for {stock_no}:")

        analyzer = StockPatternAnalyzer(data, start_date, end_date)
        analysis_result = analyzer.analyze()
        print(f"Support: {analysis_result['support']}")
        print(f"Resistance: {analysis_result['resistance']}")
        print(f"Is consolidation: {analysis_result['is_consolidation']}")
        print(f"Support touches: {analysis_result['support_touches']}")
        print(f"Resistance touches: {analysis_result['resistance_touches']}")
        print(f"Is breakout: {analysis_result['is_breakout']}")
        print(f"Is breakdown: {analysis_result['is_breakdown']}")

        plotter = StockDataPlotter()
        plotter.plot_kline_with_volume(
            data, start_date, end_date,
            support=analysis_result['support'],
            resistance=analysis_result['resistance'],
            title=f'{period_text} K-Line Chart with Analysis - {stock_no}'
        )

        sys.stdout.flush()
    except Exception as e:
        print(f"Error analyzing stock {stock_no}: {e}")
        sys.stdout.flush()

def list_worker(db_config, stock_no=None, start_date=None, end_date=None, period='D'):
    """
    Worker for listing stock data summary
    period: 'D' for daily, 'W' for weekly, 'M' for monthly
    """
    try:
        fetcher = StockDataFetcher(db_config, stock_no or "", None, None)
        fetcher.connect_db()
        
        if stock_no:
            # Get aggregated data using the new method
            data = fetcher.get_aggregated_data_from_db(period)
            
            # Filter by date range if provided
            if start_date:
                data = data[data['date'] >= pd.to_datetime(start_date)]
            if end_date:
                data = data[data['date'] <= pd.to_datetime(end_date)]
            
            if data.empty:
                print(f"\nNo data found for stock {stock_no}")
                return
            
            # Display header
            period_text = {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly'}[period]
            print(f"\n{period_text} Trading Records for Stock {stock_no}:")
            if start_date and end_date:
                print(f"Period: {start_date} to {end_date}")
            else:
                print("Latest records:")
            
            # Display data table
            print("\nDate       | Volume      | Open  | High  | Low   | Close")
            print("-" * 65)
            
            # Sort by date in descending order and limit to 10 if no date range
            data = data.sort_values('date', ascending=False)
            if not (start_date and end_date):
                data = data.head(10)
            
            # Display records with appropriate date format
            date_format = '%Y-%m' if period == 'M' else '%Y-%m-%d'
            for _, row in data.iterrows():
                date_str = row['date'].strftime(date_format)
                # Add padding for monthly format to align with header
                if period == 'M':
                    date_str = f"{date_str}    "
                print(f"{date_str} | {row['volume']:11,.0f} | "
                      f"{float(row['open_price']):5.2f} | {float(row['high_price']):5.2f} | "
                      f"{float(row['low_price']):5.2f} | {float(row['close_price']):5.2f}")
            
            # Display summary statistics
            print(f"\n{period_text} Summary Statistics:")
            print("-" * 40)
            
            print(f"Total {period_text} Periods: {len(data)}")
            print(f"Price Range: {float(data['low_price'].min()):.2f} - {float(data['high_price'].max()):.2f}")
            print(f"Average Closing Price: {float(data['close_price'].mean()):.2f}")
            print(f"Total Volume: {data['volume'].sum():,.0f}")
            print(f"Average {period_text} Volume: {data['volume'].mean():,.0f}")
            
        else:
            # Show summary for all stocks
            cursor = fetcher.db_connection.cursor()
            cursor.execute(fetcher.queries['basic']['List all stocks summary'])
            
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
        sys.stdout.flush()
            
    except Exception as e:
        print(f"Error listing stocks: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()

class StockApp:
    def __init__(self):
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "",
            "database": "stock_data"
        }
        self.processes = []
        self.process_info = {}
        self.debug_mode = False # debug mode is closed by default
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Command history, like the
        self.history_file = ".stock_app_history"
        try:
            readline.read_history_file(self.history_file)

            # The maxima command history
            readline.set_history_length(1000)
        except FileNotFoundError:
            pass
        
        # Config readline depends on platform, in windows, we use pyreadline3
        # normally, we used readline package
        if platform.system() == 'Windows':
            try:
                import pyreadline3
            except ImportError:
                print("Tip: Install pyreadline3 for command history support:")
                print("pip install pyreadline3")
        
    def __del__(self) -> None:
        # 保存命令歷史
        try:
            readline.write_history_file(self.history_file)
        except Exception as e:
            print(f"Error saving history: {e}")

    def signal_handler(self, signum, frame) -> None:
        print("\nReceived Ctrl+C, cleaning up...")
        self.cleanup()
        sys.exit(0)

    def cleanup(self) -> None:
        """
        Terminate all subprocess and this main process
        """
        for process in self.processes:
            if process.is_alive():
                print(f"Terminating process {process.pid}")
                process.terminate()
                process.join()
        self.processes = []

    def log(self, message) -> None:
        """
        according to debug mode to determine the log is shown or not
        """
        if self.debug_mode:
            print(message)

    def update_stock(self, stock_no) -> None:
        process = multiprocessing.Process(
            target=update_worker,
            args=(stock_no, self.db_config, self.debug_mode)
        )
        self.processes.append(process)
        process.start()
        self.process_info[process.pid] = {
            'type': 'update',
            'stock_no': stock_no,
            'start_time': datetime.now(),
            'status': 'running'
        }
        print(f"Started update process (PID: {process.pid})")

    def check_processes(self) -> None:
        """
        Check the process status
        """
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

    def show_status(self) -> None:
        """
        show all pid and the corresponding status
        """
        self.check_processes()
        print("\nCurrent processes status:")
        print("PID      | Type     | Stock | Start Time          | Status")
        print("-" * 60)
        for pid, info in self.process_info.items():
            start_time = info['start_time'].strftime('%Y-%m-%d %H:%M:%S')
            print(f"{pid:<8} | {info['type']:<8} | {info['stock_no']:<5} | {start_time} | {info['status']}")

    def plot_stock(self, stock_no, start_date=None, end_date=None, period='D') -> None:
        process = multiprocessing.Process(
            target=plot_worker,
            args=(stock_no, start_date, end_date, self.db_config, period)
        )
        self.processes.append(process)
        process.start()
        self.process_info[process.pid] = {
            'type': 'plot',
            'stock_no': stock_no,
            'start_time': datetime.now(),
            'status': 'running'
        }
        print(f"Started plotting process (PID: {process.pid})")

    def analyze_stock(self, stock_no, start_date=None, end_date=None, period='D') -> None:
        process = multiprocessing.Process(
            target=analyze_worker,
            args=(stock_no, start_date, end_date, self.db_config, period)
        )
        self.processes.append(process)
        process.start()
        self.process_info[process.pid] = {
            'type': 'analyze',
            'stock_no': stock_no,
            'start_time': datetime.now(),
            'status': 'running'
        }
        print(f"Started analysis process (PID: {process.pid})")

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
        print(" - list [stock_number] [start_date] [end_date]")
        print(" - debug on|off")
        print(" - status")
        print(" - exit")
        print("Date format: YYYY-MM-DD")
        print("\nTip: Use Up/Down arrows to navigate command history")

        while True:
            try:
                # Check process status before showing prompt
                self.check_processes()

                # linux command style input
                command = input("\n> ").strip()

                # record the history command
                if command:
                    readline.add_history(command)
                
                command = command.split()
                if not command:
                    continue

                if command[0] == "exit":
                    print("Cleaning up and exiting...")
                    self.cleanup()
                    break

                # Set the log is on or off
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

                elif command[0] in ["plot", "analyze"]:
                    # Extract period flag if present
                    period = 'D'  # default daily
                    if '-m' in command:
                        period = 'M'
                        command.remove('-m')
                    elif '-w' in command:
                        period = 'W'
                        command.remove('-w')

                    if len(command) < 2 or len(command) > 4:
                        print(f"Usage: {command[0]} <stock_number> [start_date] [end_date] [-m|-w]")
                        continue

                    start_date = self.parse_date(command[2]) if len(command) > 2 else None
                    end_date = self.parse_date(command[3]) if len(command) > 3 else None
                    
                    if command[0] == "plot":
                        self.plot_stock(command[1], start_date, end_date, period)
                    else:  # analyze
                        self.analyze_stock(command[1], start_date, end_date, period)

                elif command[0] == "list":
                    # Extract period flag if present
                    period = 'D'  # default daily
                    if '-m' in command:
                        period = 'M'
                        command.remove('-m')
                    elif '-w' in command:
                        period = 'W'
                        command.remove('-w')

                    if len(command) > 4:
                        print("Usage: list [stock_number] [start_date] [end_date] [-m|-w]")
                        continue
                        
                    stock_no = command[1] if len(command) > 1 else None
                    start_date = self.parse_date(command[2]) if len(command) > 2 else None
                    end_date = self.parse_date(command[3]) if len(command) > 3 else None
                    self.list_stocks(stock_no, start_date, end_date, period)

                else:
                    print("Unknown command. Available commands:")
                    print("  update <stock_number>")
                    print("  plot <stock_number> [start_date] [end_date]")
                    print("  analyze <stock_number> [start_date] [end_date]")
                    print("  list [stock_number] [start_date] [end_date]")
                    print("  debug on|off")
                    print("  status")
                    print("  exit")

            except Exception as e:
                print(f"Error: {e}")

    def list_stocks(self, stock_no=None, start_date=None, end_date=None, period='D'):
        process = multiprocessing.Process(
            target=list_worker,
            args=(self.db_config, stock_no, start_date, end_date, period)
        )
        self.processes.append(process)
        process.start()
        self.process_info[process.pid] = {
            'type': 'list',
            'stock_no': stock_no or 'all',
            'start_time': datetime.now(),
            'status': 'running'
        }
        print(f"Started listing process (PID: {process.pid})")