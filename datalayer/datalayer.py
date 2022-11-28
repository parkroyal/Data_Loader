from typing import Dict, List
import pandas as pd
import numpy as np
from datalayer.abstract_datalayer import AbstractDatalayer
from package.logging_helper import logger, log_exception

class Datalayer(AbstractDatalayer):
    """
    Provides common interface for all data sources, data destinations, senders.
    It simplifies reading and writing in various data sources, because it calls the corresponding method
    in the data manager, which already initialized from the config
    """

    def __init__(self, conf: Config):
        self.conf = conf
        self.source_2_table_with_managers: Dict[str, Dict[str, List[DataManager]]] = {}
        self.__init_table_2_managers()
        logger.info('Datalayer inited')

    @log_exception
    def __init_table_2_managers(self) -> None:
        """
        Initializes LocalFileDataManager, SqlDataManager for every entity with corresponding config
        :return: None
        """
        # 讀取SQL檔案資料
        self.__init_manager(self.conf.key_2_relations_db_source, SqlDataManager, 'source')
        
    @log_exception
    def __init_manager(self, key_2_conf, manager, type) -> None:
        """
        Initializes specific data manager for every entity in corresponding config.
        IMPORTANT: must be only 1 source for 1 entity name, but destinations can multiply
        :return: None
        """
        for table in list(key_2_conf.keys()):
            self.source_2_table_with_managers.setdefault(type, {}).setdefault(table, []).append(manager(self.conf))

            if type == 'source' and len(self.source_2_table_with_managers[type][table]) > 1:
                raise Exception("Must be only one source for {}. Fix config".format(table))

    @log_exception
    def fetch_dataframe(self, entity_name, params={}) -> pd.DataFrame:
        """Reads rows from source by config and converts it to dataframe
        :param entity_name: Name table, which described in source config
        :param params: key-value object, can contain query_param_2_value for query template, which will be replacing on values.
        :return: dataframe
        """
        logger.info('fetch_dataframe {}'.format(entity_name))
        df = self.source_2_table_with_managers['source'][entity_name][0].fetch_dataframe(entity_name, params)

        #transforming login columns to int
        if "login" in df.columns:
            df['login'] = df['login'].astype(int)
        #remapping mt4 server 
        if "server" in df.columns:
            conditions = [(df.server.eq('mt5_vfx_live')),
                        (df.server.eq('mt5_vfx_live2')),
                        (df.server.eq('mt5_pug_live'))]
            results = ['MT5_VFX_Live', 'MT5_VFX_Live2', 'MT5_PUG_Live']
            df['server'] = np.select(conditions, results, default=df.server)
        print(df.head(5))
        return df