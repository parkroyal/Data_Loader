from typing import Any, List, Dict

class Config:
    """
    Contains parsed yaml config
    """
    def __init__(self, config_yaml: Any) -> None:
        # self.query: Dict[str, TableConfig] = {}
    
        self._parse_conf(config_yaml)

    def _parse_conf(self, conf_yaml: Any) -> None:
        """
        Parses yaml config and init python structures
        :param conf_yaml: config
        :return: None
        """
        for conf_name, conf_dict in conf_yaml.items():
            if conf_name == 'sources':
                self._parse_sources_conf(conf_dict)

    def _parse_sources_conf(self, conf_yaml: dict):
        """
        Parses "sources" config params
        :param conf_yaml: config
        :return: None
        """
        for conf_name, conf_dict in conf_yaml.items():
            if conf_name == 'relational_db':
                self.querys = _get_key_2_conf(conf_dict, QueryConfig)

class QueryConfig:
    """
    Parses table config. Example:
        user_table:
          db: 'datatp'
          schema: 'detail'
          connector_type: 'mysql_db'
          query: 'select * from [schema].[name]'
    """
    db: str
    schema: str
    connector_type: str
    query: str

    def __init__(self, conf: Dict):
        self.schema = conf.get('schema', '')
        self.name = conf.get('name', '')
        self.storage_key = conf.get('storage', '')
        self.storage_type = conf.get('connector_type', '')
        self.query_template = conf.get('query_template', '')
        self.expected_columns = conf.get('expected_columns', [])
        self.allow_empty = True if conf.get('allow_empty', 'no') == 'yes' else False    

class TableConfig:
    """
    Parses table config. Example:
        user_table:
          schema: 'trading_2018'
          name: 'All_Users_Table'
          storage: 'trading_db'
          connector_type: 'mock'
          expected_columns: [ 'LOGIN', 'NAME' ]
          query_template: 'select * from [schema].[name]'
    """
    schema: str
    name: str
    storage_key: str
    storage_type: str
    query_template: str
    expected_columns: List[str]

    def __init__(self, conf: Dict):
        self.schema = conf.get('schema', '')
        self.name = conf.get('name', '')
        self.storage_key = conf.get('storage', '')
        self.storage_type = conf.get('connector_type', '')
        self.query_template = conf.get('query_template', '')
        self.expected_columns = conf.get('expected_columns', [])
        self.allow_empty = True if conf.get('allow_empty', 'no') == 'yes' else False


def _get_key_2_conf(conf_dict: dict, class_name: Any) -> Dict[str, Any]:
    """
    Parses deep yaml structures into key-class_object structure
    :param conf_dict: structures config
    :param class_name: structure, that describes in config
    :return: key-class_object
    """
    key_2_conf_obj = {}
    for key, conf in conf_dict.items():
        key_2_conf_obj[key] = class_name(conf)
    return key_2_conf_obj