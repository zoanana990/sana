import mysql.connector
import pandas as pd
import requests
import time
from datetime import datetime, timedelta

class StockDataFetcher:
    def __init__(self, db_config, stock_no, start_date, end_date):
        self.db_config = db_config
        self.stock_no = stock_no
        self.start_date = start_date
        self.end_date = end_date
        self.db_connection = None
        self.twse_url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        self.tpex_url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"

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

    def fetch_stock_data(self, date_str):
        """嘗試從上市和上櫃市場抓取股票資料"""
        # 先嘗試從上市市場抓取
        data = self.fetch_twse_data(date_str)
        if data:
            print(f"Found {self.stock_no} in TWSE market")
            return data
            
        # 如果上市市場沒有，嘗試從上櫃市場抓取
        data = self.fetch_tpex_data(date_str)
        if data:
            print(f"Found {self.stock_no} in TPEx market")
            return data
            
        print(f"Stock {self.stock_no} not found in either market")
        return None

    def fetch_twse_data(self, date_str):
        """從台灣證券交易所抓取資料"""
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
        """從證券櫃檯買賣中心抓取資料"""
        # 轉換日期格式 (yyyymmdd -> yyy/mm)
        year = int(date_str[:4]) - 1911  # 轉換為民國年
        month = date_str[4:6]
        
        params = {
            'response': 'json',
            'date': f'{year}/{month}/01',
            'stockNo': self.stock_no
        }
        
        url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()  # 如果回應不是 200 會拋出異常
            
            # 印出回應內容以便偵錯
            print(f"TPEx response: {response.text[:200]}...")
            
            data = response.json()
            if not data.get('data'):
                return None

            # 轉換資料格式以匹配 TWSE 格式
            converted_data = []
            for row in data['data']:
                try:
                    # 檢查是否有空值或無效值
                    if '--' in row or not row[0]:  # 確保有日期
                        continue
                    
                    converted_data.append([
                        f"{year}/{row[0]}",  # 日期 (加上年份)
                        row[1],  # 成交股數
                        row[2],  # 成交金額
                        row[3],  # 開盤價
                        row[4],  # 最高價
                        row[5],  # 最低價
                        row[6],  # 收盤價
                        row[7],  # 漲跌價差
                        row[8]   # 成交筆數
                    ])
                except Exception as e:
                    print(f"Error converting row {row}: {e}")
                    continue
            
            return converted_data
            
        except requests.exceptions.RequestException as e:
            print(f"TPEx request failed: {e}")
            print(f"URL: {url}")
            print(f"Params: {params}")
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
            
            time.sleep(3)  # 避免請求過於頻繁
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

    def get_aggregated_data_from_db(self, period='D'):
        """
        Get aggregated data from database
        period: 'D' for daily, 'W' for weekly, 'M' for monthly
        """
        cursor = self.db_connection.cursor(dictionary=True)
        
        if period == 'D':
            # Original daily data query
            cursor.execute("""
                SELECT date, open_price, high_price, low_price, close_price, volume 
                FROM stock_prices 
                WHERE stock_no = %s
                ORDER BY date
            """, (self.stock_no,))
        else:
            # Aggregated query for weekly or monthly data
            if period == 'M':
                # Monthly aggregation
                cursor.execute("""
                    WITH monthly_data AS (
                        SELECT 
                            DATE_FORMAT(date, '%Y-%m-01') as month_start,
                            MIN(date) as first_date,
                            MAX(date) as last_date,
                            MAX(high_price) as high_price,
                            MIN(low_price) as low_price,
                            SUM(volume) as volume
                        FROM stock_prices 
                        WHERE stock_no = %s
                        GROUP BY DATE_FORMAT(date, '%Y-%m-01')
                    )
                    SELECT 
                        md.month_start as date,
                        sp1.open_price,
                        md.high_price,
                        md.low_price,
                        sp2.close_price,
                        md.volume
                    FROM monthly_data md
                    JOIN stock_prices sp1 ON sp1.date = md.first_date
                    JOIN stock_prices sp2 ON sp2.date = md.last_date
                    WHERE sp1.stock_no = %s AND sp2.stock_no = %s
                    ORDER BY md.month_start
                """, (self.stock_no, self.stock_no, self.stock_no))
            else:
                # Weekly aggregation
                cursor.execute("""
                    WITH weekly_data AS (
                        SELECT 
                            DATE(date - INTERVAL WEEKDAY(date) DAY) as week_start,
                            MIN(date) as first_date,
                            MAX(date) as last_date,
                            MAX(high_price) as high_price,
                            MIN(low_price) as low_price,
                            SUM(volume) as volume
                        FROM stock_prices 
                        WHERE stock_no = %s
                        GROUP BY YEARWEEK(date)
                    )
                    SELECT 
                        wd.week_start as date,
                        sp1.open_price,
                        wd.high_price,
                        wd.low_price,
                        sp2.close_price,
                        wd.volume
                    FROM weekly_data wd
                    JOIN stock_prices sp1 ON sp1.date = wd.first_date
                    JOIN stock_prices sp2 ON sp2.date = wd.last_date
                    WHERE sp1.stock_no = %s AND sp2.stock_no = %s
                    ORDER BY wd.week_start
                """, (self.stock_no, self.stock_no, self.stock_no))
        
        data = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(data)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        return df