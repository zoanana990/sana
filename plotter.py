import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt

class StockDataPlotter:
    @staticmethod
    def plot_kline_with_volume(data, start_date=None, end_date=None, support=None, resistance=None, title=None):
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
        mpf.plot(data, type='candle', volume=True, 
                 title=title or 'K-Line and Volume with Support/Resistance',
                 style='charles', ylabel='Price', ylabel_lower='Volume', 
                 addplot=ap)

    def plot_income_chart(self, data, start_date=None, end_date=None, title=None):
        """
        Plot income and profit chart
        """
        plt.figure(figsize=(15, 8))
        
        # Plot revenue
        plt.subplot(2, 1, 1)
        plt.plot(data['date'], data['revenue'], label='Revenue', color='blue')
        plt.title(title or 'Monthly Revenue')
        plt.xlabel('Date')
        plt.ylabel('Revenue')
        plt.grid(True)
        plt.legend()

        # Plot profit
        plt.subplot(2, 1, 2)
        plt.plot(data['date'], data['profit'], label='Profit', color='green')
        plt.title('Monthly Profit')
        plt.xlabel('Date')
        plt.ylabel('Profit')
        plt.grid(True)
        plt.legend()

        plt.tight_layout()
        plt.show()
