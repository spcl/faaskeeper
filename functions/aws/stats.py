from collections import defaultdict
from typing import Optional


class TimingStatistics:

    _instance: Optional["TimingStatistics"] = None

    def __init__(self):
        self.reset()

    def reset(self):
        self._repetitions = 0
        self._results = defaultdict(list)

    @staticmethod
    def instance() -> "TimingStatistics":
        if TimingStatistics._instance is None:
            TimingStatistics._instance = TimingStatistics()
        return TimingStatistics._instance

    @property
    def repetitions(self):
        return self._repetitions

    def add_repetition(self):
        self._repetitions += 1

    def add_result(self, key: str, val: float):
        self._results[key].append(val)

    def print(self):

        for key, value in self._results.items():
            print(f"Result: {key} Value: {value}")
