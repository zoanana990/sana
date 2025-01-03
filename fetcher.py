import mysql.connector
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import os
import yfinance as yf

class SQLLoader:
    @staticmethod
    def load_query(filename):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(base_dir, 'stock', 'sql', 'queries', filename)
        
        try:
            with open(file_path, 'r') as f:
                # Split the file content by -- and get non-empty queries
                queries = [q.strip() for q in f.read().split('--') if q.strip()]
                # Create a dictionary of queries if there are multiple queries in the file
                if len(queries) > 1:
                    # The first line of each query is the query name
                    return {q.split('\n')[0].strip(): '\n'.join(q.split('\n')[1:]).strip() 
                           for q in queries}
                return queries[0]
        except FileNotFoundError:
            alternative_path = os.path.join(base_dir, 'sql', 'queries', filename)
            with open(alternative_path, 'r') as f:
                queries = [q.strip() for q in f.read().split('--') if q.strip()]
                if len(queries) > 1:
                    return {q.split('\n')[0].strip(): '\n'.join(q.split('\n')[1:]).strip() 
                           for q in queries}
                return queries[0]

class StockDataFetcher:
    def __init__(self, db_config, stock_no, start_date, end_date):
        self.db_config = db_config
        self.stock_no = stock_no
        self.start_date = start_date
        self.end_date = end_date
        self.db_connection = None
        self.twse_url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        self.tpex_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
        self.mops_url = "https://mops.twse.com.tw/mops/web/t05st10_ifrs"  # 營收資料的URL
        
        # Load SQL queries
        try:
            self.queries = {
                'basic': SQLLoader.load_query('basic.sql'),
                'monthly': SQLLoader.load_query('monthly.sql'),
                'weekly': SQLLoader.load_query('weekly.sql'),
                'income': SQLLoader.load_query('income.sql')
            }
        except Exception as e:
            print(f"Error loading SQL queries: {e}")
            raise

    def connect_db(self):
        try:
            self.db_connection = mysql.connector.connect(**self.db_config)
            print("Successfully connected to the database!")
            
            # Create database and table if not exists
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            schema_path = os.path.join(base_dir, 'stock', 'sql', 'schema.sql')
            
            try:
                with open(schema_path, 'r') as sql_file:
                    cursor = self.db_connection.cursor()
                    sql_commands = sql_file.read().split(';')
                    for command in sql_commands:
                        if command.strip():
                            cursor.execute(command)
                    self.db_connection.commit()
                    cursor.close()
            except FileNotFoundError:
                schema_path = os.path.join(base_dir, 'sql', 'schema.sql')
                with open(schema_path, 'r') as sql_file:
                    cursor = self.db_connection.cursor()
                    sql_commands = sql_file.read().split(';')
                    for command in sql_commands:
                        if command.strip():
                            cursor.execute(command)
                    self.db_connection.commit()
                    cursor.close()
                
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            raise

    def fetch_stock_data(self, date_str):
        """
        Fetch the data from twse or tpex (some bug in tpex)
        """
        # Fetch the data from twse first
        data = self.fetch_twse_data(date_str)
        if data:
            print(f"Found {self.stock_no} in TWSE market")
            return data
            
        # if there is no data from twse, fetch the data from tpex
        data = self.fetch_tpex_data(date_str)
        if data:
            print(f"Found {self.stock_no} in TPEx market")
            return data
            
        print(f"Stock {self.stock_no} not found in either market")
        return None

    def fetch_twse_data(self, date_str):
        """
        request data from twse
        """
        params = {
            'response': 'json',
            'date': date_str,
            'stockNo': self.stock_no
        }
        
        try:
            response = requests.get(self.twse_url, params=params)
            if response.status_code != 200:
                return None
            
            data = response.json()
            if data.get('stat') != 'OK':
                return None
                
            return data.get('data', [])
            
        except Exception as e:
            print(f"TWSE request failed: {e}")
            return None

    def fetch_tpex_data(self, date_str):
        """
        Fetch data from yfinance for TPEx stocks
        """
        try:
            # Convert stock number to Taiwan stock symbol format
            symbol = f"{self.stock_no}.TW"  # For TWSE stocks
            if not self.fetch_twse_data(date_str):  # If not found in TWSE
                symbol = f"{self.stock_no}.TWO"  # For TPEx stocks
            
            # Parse date string to datetime
            date_parts = [int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])]
            start_date = datetime(*date_parts)
            end_date = (start_date.replace(day=1) + timedelta(days=32)).replace(day=1)
            
            # Fetch data from yfinance
            stock = yf.Ticker(symbol)
            hist = stock.history(start=start_date, end=end_date)
            
            if hist.empty:
                return None
            
            # Convert yfinance data to TWSE format
            converted_data = []
            for index, row in hist.iterrows():
                date_str = f"{index.year-1911}/{index.month}/{index.day}"  # Convert to ROC calendar
                converted_row = [
                    date_str,
                    str(int(row['Volume'])),
                    str(int(row['Volume'] * row['Close'])),  # Approximate turnover
                    str(row['Open']),
                    str(row['High']),
                    str(row['Low']),
                    str(row['Close']),
                    str(round(row['Close'] - row['Open'], 2)),  # Price change
                    str(int(row['Volume'] / 1000))  # Approximate transaction count
                ]
                converted_data.append(converted_row)
            
            return converted_data
            
        except Exception as e:
            print(f"Error fetching from yfinance: {e}")
            return None

    def get_last_update_date(self):
        """
        get the last update date in SQL
        """
        cursor = self.db_connection.cursor()
        cursor.execute(self.queries['basic']['Get last update date'], (self.stock_no,))
        last_date = cursor.fetchone()[0]
        cursor.close()
        return last_date

    def update_stock_data(self):
        """
        Download and update data
        """
        last_update = self.get_last_update_date()
        if last_update:
            start_date = last_update + timedelta(days=1)
            print(f"Continuing from last update date: {start_date}")
        else:
            start_date = self.start_date
            print(f"Starting fresh download from: {start_date}")

        current_date = start_date
        end_date = self.end_date.date() if isinstance(self.end_date, datetime) else self.end_date
        
        while current_date <= end_date:
            first_day = current_date.replace(day=1)
            date_str = first_day.strftime('%Y%m%d')
            print(f"Fetching data for {self.stock_no} - {date_str}")
            
            try:
                monthly_data = self.fetch_stock_data(date_str)
                if monthly_data:
                    try:
                        self.insert_data(monthly_data)
                        print(f"Successfully inserted data for {self.stock_no} - {date_str}")
                    except Exception as e:
                        print(f"Error inserting data: {e}")
                else:
                    print(f"No data available for {self.stock_no} in {date_str}")
            except Exception as e:
                print(f"Error fetching data: {e}")
            
            time.sleep(3)
            current_date = (first_day + timedelta(days=32)).replace(day=1)

    def disconnect_db(self):
        if self.db_connection:
            self.db_connection.close()
            print("Database connection closed.")

    def insert_data(self, records):
        """
        Insert data to SQL
        """
        cursor = self.db_connection.cursor()
        
        for record in records:
            try:
                # Convert R.O.C. to A.D.
                date_parts = record[0].split('/')
                year = int(date_parts[0]) + 1911
                month = int(date_parts[1])
                day = int(date_parts[2])
                date = datetime(year, month, day)

                volume = int(record[1].replace(",", ""))
                turnover = int(record[2].replace(",", ""))
                open_price = float(record[3].replace(",", "")) if record[3] != "--" else None
                high_price = float(record[4].replace(",", "")) if record[4] != "--" else None
                low_price = float(record[5].replace(",", "")) if record[5] != "--" else None
                close_price = float(record[6].replace(",", "")) if record[6] != "--" else None
                price_change = record[7]
                transaction_count = int(record[8].replace(",", ""))

                cursor.execute(self.queries['basic']['Insert or update stock data'], 
                             (self.stock_no, date, volume, turnover, open_price, high_price,
                              low_price, close_price, price_change, transaction_count))
                
            except Exception as e:
                print(f"Error processing record {record}: {e}")
                continue

        self.db_connection.commit()
        cursor.close()

    def get_data_from_db(self):
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute(self.queries['basic']['Get data from db'], (self.stock_no,))
        data = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        return df

    def get_aggregated_data_from_db(self, period='D'):
        cursor = self.db_connection.cursor(dictionary=True)
        
        if period == 'D':
            cursor.execute(self.queries['basic']['Get daily data'], 
                         (self.stock_no,))
        elif period == 'M':
            cursor.execute(self.queries['monthly'], 
                         (self.stock_no, self.stock_no, self.stock_no))
        else:  # Weekly
            cursor.execute(self.queries['weekly'], 
                         (self.stock_no, self.stock_no, self.stock_no, self.stock_no))
        
        data = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(data)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df

    def fetch_income_data(self, year, month):
        """
        Fetch income data from yfinance
        """
        try:
            symbol = f"{self.stock_no}.TW"  # Try TWSE first
            stock = yf.Ticker(symbol)
            
            if not stock.info:  # If not found in TWSE
                symbol = f"{self.stock_no}.TWO"  # Try TPEx
                stock = yf.Ticker(symbol)
            
            # Get quarterly financials
            financials = stock.quarterly_financials
            
            if financials.empty:
                return None
            
            # Create data dictionary
            data = {
                'date': datetime(year, month, 1),
                'revenue': financials.loc['Total Revenue'].iloc[0] if 'Total Revenue' in financials.index else None,
                'profit': financials.loc['Net Income'].iloc[0] if 'Net Income' in financials.index else None
            }
            
            return data
            
        except Exception as e:
            print(f"Error fetching income data: {e}")
            return None

    def update_income_data(self):
        """
        Update income data using yfinance
        """
        last_update = self.get_last_income_update()
        if last_update:
            start_date = last_update + timedelta(days=1)
        else:
            start_date = self.start_date

        current_date = start_date
        end_date = self.end_date.date() if isinstance(self.end_date, datetime) else self.end_date
        
        while current_date <= end_date:
            try:
                data = self.fetch_income_data(current_date.year, current_date.month)
                if data:
                    self.insert_income_data(data)
                    print(f"Successfully inserted income data for {self.stock_no} - {current_date.strftime('%Y-%m')}")
                else:
                    print(f"No income data available for {self.stock_no} in {current_date.strftime('%Y-%m')}")
            except Exception as e:
                print(f"Error updating income data: {e}")
            
            current_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)

    def get_last_income_update(self):
        """
        get the last income data update
        """
        cursor = self.db_connection.cursor()
        cursor.execute(self.queries['income']['Get last income update'], (self.stock_no,))
        last_date = cursor.fetchone()[0]
        cursor.close()
        return last_date

    def insert_income_data(self, data):
        """
        insert the income data into the database
        """
        cursor = self.db_connection.cursor()
        cursor.execute(self.queries['income']['Insert or update income data'],
                      (self.stock_no, data['date'], data['revenue'], data['profit']))
        self.db_connection.commit()
        cursor.close()

    def get_income_data_from_db(self):
        """
        Fetch the data from database
        """
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute(self.queries['income']['Get monthly income data'], (self.stock_no,))
        data = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(data)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df