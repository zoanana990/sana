import mysql.connector
import pandas as pd

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