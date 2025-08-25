from abc import ABC, abstractmethod
import pandas as pd

class BaseExtractor(ABC):
    """
    所有数据提取器的抽象基类。
    要求子类实现 fetch() 方法。
    """

    @abstractmethod
    def fetch(self):
        """返回提取出的 DataFrame"""
        pass