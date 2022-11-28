from abc import abstractmethod
from typing import Dict, Any
from pandas import pd

class AbstractDatalayer:
    """
    Provides interface, that must be implemented in child classes
    """

    @abstractmethod
    def get_custom_from_conf(self, key) -> str:
        """
        Pass custom params, which defines in conf.yml
        :param key: name of custom
        :return:
        """

    @abstractmethod
    def fetch_dataframe(self, entity_name: str, params: Dict[str, Any] = {}) -> pd.DataFrame:
        """
        Reads rows from entity by config and converts it to dataframe
        :param entity_name: label, which described in config
        :param params: key-value object, pass specific structures for special realizations.
        :return:
        """
        pass