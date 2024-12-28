import pandas as pd
from decimal import Decimal

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
