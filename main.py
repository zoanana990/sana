import mysql.connector
import requests
from datetime import datetime, timedelta
import pandas as pd
import mplfinance as mpf
from decimal import Decimal

# twse_url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20220101&stockNo={stock_code}'
class StockDataFetcher:
    def __init__(self, db_config, stock_no, start_date, end_date):
        self.db_config = db_config
        self.stock_no = stock_no
        self.start_date = start_date
        self.end_date = end_date
        self.db_connection = None

    def connect_db(self):
        self.db_connection = mysql.connector.connect(**self.db_config)
        print("Successfully connected to the database!")

    def disconnect_db(self):
        if self.db_connection:
            self.db_connection.close()
            print("Database connection closed.")

    def insert_data(self, records):
        cursor = self.db_connection.cursor()
        for record in records:
            # Convert Taiwanese year to Western year
            date_parts = record[0].split("/")
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

            # Insert the data into the database table
            cursor.execute("""
                INSERT INTO stock_prices 
                (stock_no, date, volume, turnover, open_price, high_price, low_price, close_price, price_change, transaction_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (self.stock_no, date, volume, turnover, open_price, high_price, low_price, close_price, price_change,
                  transaction_count))
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


class StockDataPlotter:
    @staticmethod
    def plot_kline_with_volume(data, start_date=None, end_date=None, support=None, resistance=None):
        # Ensure the data format is correct
        data['date'] = pd.to_datetime(data['date'])
        data.set_index('date', inplace=True)

        # Filter data by date range
        if start_date:
            start_date = pd.to_datetime(start_date)
            data = data[data.index >= start_date]
        if end_date:
            end_date = pd.to_datetime(end_date)
            data = data[data.index <= end_date]

        # Convert data to the required format for plotting
        data['Open'] = pd.to_numeric(data['open_price'], errors='coerce')
        data['High'] = pd.to_numeric(data['high_price'], errors='coerce')
        data['Low'] = pd.to_numeric(data['low_price'], errors='coerce')
        data['Close'] = pd.to_numeric(data['close_price'], errors='coerce')
        data['Volume'] = pd.to_numeric(data['volume'], errors='coerce')
        data.fillna(method='ffill', inplace=True)

        # Set up additional plots (support/resistance lines)
        ap = []
        if support is not None:
            ap.append(mpf.make_addplot([float(support)] * len(data), color='green', linestyle='--', width=1))  # Convert to float
        if resistance is not None:
            ap.append(mpf.make_addplot([float(resistance)] * len(data), color='red', linestyle='--', width=1))  # Convert to float

        # Plot the K-line chart with volume
        mpf.plot(data, type='candle', volume=True, title='K-Line and Volume with Support/Resistance',
                 style='charles', ylabel='Price', ylabel_lower='Volume', addplot=ap)

class StockPattern:
    def __init__(self):
        pass

class StockPatternAnalyzer:
    def __init__(self, data, start_date, end_date):
        self.data = data
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.filtered_data = self.data[(self.data['date'] >= self.start_date) & (self.data['date'] <= self.end_date)]

        # TODO: Dynamically calculate support and resistance levels
        self.support = Decimal(self.filtered_data['low_price'].min())  # Convert to Decimal
        self.resistance = Decimal(self.filtered_data['high_price'].max())  # Convert to Decimal

        # TODO: There are several patterns in a interval, furthermore, there are nested mini pattern in a bit pattern

    def get_support_and_resistance(self):
        pass

    def is_consolidation(self):
        if self.filtered_data.empty:
            return False
        within_range = (self.filtered_data['close_price'] >= self.support) & (self.filtered_data['close_price'] <= self.resistance)
        return within_range.all()

    def count_touches(self):
        if self.filtered_data.empty:
            return {"resistance_touches": 0, "support_touches": 0}

        # Convert the float multipliers to Decimal
        resistance_touches = ((self.filtered_data['close_price'] >= self.resistance * Decimal('0.99')) &
                              (self.filtered_data['close_price'] <= self.resistance * Decimal('1.01'))).sum()
        support_touches = ((self.filtered_data['close_price'] >= self.support * Decimal('0.99')) &
                           (self.filtered_data['close_price'] <= self.support * Decimal('1.01'))).sum()

        return {"resistance_touches": resistance_touches, "support_touches": support_touches}

    def is_breakout(self):
        if self.filtered_data.empty:
            return False
        return (self.filtered_data['close_price'] > self.resistance).any()

    def is_breakdown(self):
        if self.filtered_data.empty:
            return False
        return (self.filtered_data['close_price'] < self.support).any()

    def analyze(self):
        return {
            "support": self.support,
            "resistance": self.resistance,
            "is_consolidation": self.is_consolidation(),
            "resistance_touches": self.count_touches()["resistance_touches"],
            "support_touches": self.count_touches()["support_touches"],
            "is_breakout": self.is_breakout(),
            "is_breakdown": self.is_breakdown()
        }


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