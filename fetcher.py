import mysql.connector
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import json

class StockDataFetcher:
    def __init__(self, db_config, stock_no, start_date, end_date):
        self.db_config = db_config
        self.stock_no = stock_no
        self.start_date = start_date
        self.end_date = end_date
        self.db_connection = None
        self.base_url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"

    def connect_db(self):
        try:
            self.db_connection = mysql.connector.connect(**self.db_config)
            print("Successfully connected to the database!")
            
            # Create database and table if not exists
            with open('stock.sql', 'r') as sql_file:
                cursor = self.db_connection.cursor()
                # Split SQL commands and execute them separately
                sql_commands = sql_file.read().split(';')
                for command in sql_commands:
                    if command.strip():
                        cursor.execute(command)
                self.db_connection.commit()
                cursor.close()
                
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            raise

    def fetch_twse_data(self, date_str):
        """從 TWSE 抓取某月的股票資料"""
        params = {
            'response': 'json',
            'date': date_str,
            'stockNo': self.stock_no
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            if response.status_code != 200:
                print(f"Error fetching data: status code {response.status_code}")
                return None
            
            data = response.json()
            if data.get('stat') != 'OK':
                print(f"Error in response: {data.get('stat')}")
                return None
                
            return data.get('data', [])
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def get_last_update_date(self):
        """獲取資料庫中最後更新的日期"""
        cursor = self.db_connection.cursor()
        cursor.execute("""
            SELECT MAX(date) 
            FROM stock_prices 
            WHERE stock_no = %s
        """, (self.stock_no,))
        last_date = cursor.fetchone()[0]
        cursor.close()
        return last_date

    def update_stock_data(self):
        """下載並更新股票資料"""
        # 檢查資料庫中最後更新日期
        last_update = self.get_last_update_date()
        if last_update:
            start_date = last_update + timedelta(days=1)
            print(f"Continuing from last update date: {start_date}")
        else:
            start_date = self.start_date
            print(f"Starting fresh download from: {start_date}")

        current_date = start_date
        
        while current_date <= self.end_date:
            # 取得當月第一天
            first_day = current_date.replace(day=1)
            date_str = first_day.strftime('%Y%m%d')
            print(f"Fetching data for {self.stock_no} - {date_str}")
            
            # 下載該月資料
            monthly_data = self.fetch_twse_data(date_str)
            if monthly_data:
                try:
                    self.insert_data(monthly_data)
                    print(f"Successfully inserted data for {self.stock_no} - {date_str}")
                except Exception as e:
                    print(f"Error inserting data: {e}")
            
            # 等待 3 秒避免被 ban
            time.sleep(3)
            
            # 移到下個月
            current_date = (first_day + timedelta(days=32)).replace(day=1)

    def disconnect_db(self):
        if self.db_connection:
            self.db_connection.close()
            print("Database connection closed.")

    def insert_data(self, records):
        """將資料插入資料庫"""
        cursor = self.db_connection.cursor()
        
        for record in records:
            try:
                # 轉換民國年為西元年
                date_parts = record[0].split('/')
                year = int(date_parts[0]) + 1911
                month = int(date_parts[1])
                day = int(date_parts[2])
                date = datetime(year, month, day)

                # 處理數值資料
                volume = int(record[1].replace(",", ""))
                turnover = int(record[2].replace(",", ""))
                open_price = float(record[3].replace(",", "")) if record[3] != "--" else None
                high_price = float(record[4].replace(",", "")) if record[4] != "--" else None
                low_price = float(record[5].replace(",", "")) if record[5] != "--" else None
                close_price = float(record[6].replace(",", "")) if record[6] != "--" else None
                price_change = record[7]
                transaction_count = int(record[8].replace(",", ""))

                # 使用 REPLACE INTO 來處理重複資料
                cursor.execute("""
                    REPLACE INTO stock_prices 
                    (stock_no, date, volume, turnover, open_price, high_price, 
                     low_price, close_price, price_change, transaction_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (self.stock_no, date, volume, turnover, open_price, high_price,
                      low_price, close_price, price_change, transaction_count))
                
            except Exception as e:
                print(f"Error processing record {record}: {e}")
                continue

        self.db_connection.commit()
        cursor.close()

    def get_data_from_db(self):
        cursor = self.db_connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT date, open_price, high_price, low_price, close_price, volume 
            FROM stock_prices 
            WHERE stock_no = %s
            ORDER BY date
        """, (self.stock_no,))
        data = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(data)
        # Ensure 'date' is in datetime format
        df['date'] = pd.to_datetime(df['date'])
        return df