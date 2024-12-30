import mysql.connector
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import os

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
        
        # Load SQL queries
        try:
            self.queries = {
                'basic': SQLLoader.load_query('basic.sql'),
                'monthly': SQLLoader.load_query('monthly.sql'),
                'weekly': SQLLoader.load_query('weekly.sql')
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

    # FIXME: there are some bug cannot fetch the data from tpex
    def fetch_tpex_data(self, date_str):
        """
        Fetch data from tpex
        """
        pass
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
        
        while current_date <= self.end_date:
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