import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal

@dataclass
class Point:
    date: datetime
    price: float

class PatternType(Enum):
    DOUBLE_TOP = "double_top"
    DOUBLE_BOTTOM = "double_bottom"
    HEAD_SHOULDER = "head_shoulder"
    INVERSE_HEAD_SHOULDER = "inverse_head_shoulder"
    RISING_TRIANGLE = "rising_triangle"
    FALLING_TRIANGLE = "falling_triangle"
    SYMMETRICAL_TRIANGLE = "symmetrical_triangle"
    BULLISH_PENNANT = "bullish_pennant"
    BEARISH_PENNANT = "bearish_pennant"

class TrendLine(ABC):
    def __init__(self):
        self.slope: float = 0
        self.intercept: float = 0
        self.start_date: datetime = None
        self.end_date: datetime = None
        self.medium_date: datetime = None
        self.points: List[Point] = []

    def fit(self, points: List[Point]) -> None:
        """
        fit function
        """
        pass

    def get_price_at_date(self, date: datetime) -> float:
        """
        fetch the price at the date
        """
        pass

    @abstractmethod
    def is_false_break(self, price: float, date: datetime) -> bool:
        """
        determine whether the price is false breakout or false breakdown
        """
        pass

class SupportTrendLine(TrendLine, ABC):
    def is_breakout(self, price: float, date: datetime) -> bool:
        """
        false breakdown
        """
        pass

class ResistanceTrendLine(TrendLine, ABC):
    def is_breakout(self, price: float, date: datetime) -> bool:
        """
        false breakout
        """
        pass

class Pattern(ABC):
    def __init__(self, 
                 start_time: datetime,
                 end_time: datetime,
                 resistance_trend_line: ResistanceTrendLine,
                 support_trend_line: SupportTrendLine):
        self.start_time = start_time
        self.end_time = end_time
        self.resistance_trend_line = resistance_trend_line
        self.support_trend_line = support_trend_line
        self.pattern_type: PatternType = None
        self.confidence: float = 0.0
        self.target_price: float = 0.0
        self.stop_loss: float = 0.0
        self.satisfied: bool = False

    @abstractmethod
    def validate(self) -> bool:
        """
        check the interval is this pattern ?
        """
        pass

    @abstractmethod
    def calculate_target(self) -> float:
        """
        Calculate the target price
        """
        pass

    @abstractmethod
    def calculate_stop_loss(self) -> float:
        """
        Calculate the stop loss price
        """
        pass

class DoubleTop(Pattern):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pattern_type = PatternType.DOUBLE_TOP
        self.first_peak: Point = None
        self.second_peak: Point = None
        self.valley: Point = None

    def validate(self) -> bool:
        pass

    def calculate_target(self) -> float:
        pass

    def calculate_stop_loss(self) -> float:
        pass

#
# class StockPatternAnalyzer:
#     def __init__(self, data: pd.DataFrame):
#         self.data = data
#         self.patterns: List[Pattern] = []
#
#     def analyze(self) -> List[Pattern]:
#         self._find_double_tops()
#         self._find_double_bottoms()
#         self._find_head_shoulders()
#
#         return self.patterns
#
#     def _find_double_tops(self):
#         pass
#
#     def _find_double_bottoms(self):
#         pass
#
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