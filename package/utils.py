# %%
from package.logging_helper import log_exception, logger
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from typing import List, Any
import pandas as pd 
import os
import numpy as np

from package.db_helper import get_data
# %%
# 今日日期
if date.today().weekday() == 0:
    nowDate = date.today() - timedelta(days = 3)
elif date.today().weekday() == 6:
    nowDate = date.today() - timedelta(days = 2)
else:
    nowDate = date.today() - timedelta(days = 1)

# 昨日日期
if nowDate.weekday() == 0:
    nowDate_lag = nowDate - timedelta(days = 3)
elif nowDate.weekday() == 6:
    nowDate_lag = date.today() - timedelta(days = 2)
else:
    nowDate_lag = nowDate - timedelta(days = 1)

symbol_category = pd.read_excel(r'\\192.168.1.20\Rmc\Data Analysis\Dropbox\股票產品\Symble_Table\Symbol Table.xlsx', sheet_name = 'Symbol')
symbol_category = symbol_category.rename(columns={"Symbol": "symbol", "fixed_symbol": "clean_symbol", "Type": "symbol_type"})
symbol_category = symbol_category[['symbol', 'clean_symbol', 'symbol_type']]
symbol_category = symbol_category.drop_duplicates()

# %%
def get_sql(list_x: List, type = 'int') -> str:

    list_x = list(set(list_x))
    # 排除nan值
    if type == 'str':
        string = ",".join([f"'{str(x)}'" for x in list_x if pd.isnull(x) == False])
    elif type == 'int':
        string = ",".join([str(x) for x in list_x if pd.isnull(x) == False])
    return string

# %% 
# 日期物件
class Date:
    def __init__(self, search_date:date = None) -> None:
        # 最新日報日期
        if search_date:
            self.nowDate = search_date
        else:
            if date.today().isoweekday() == 1:
                self.nowDate = date.today() - timedelta(days = 3)
            elif date.today().isoweekday() == 7:
                self.nowDate = date.today() - timedelta(days = 2)
            else:
                self.nowDate = date.today() - timedelta(days = 1)
    def get_week_date(self, type = "current_week"):
        if type == "current":
            # 找禮拜一
            start_date = self.nowDate
            while start_date.isoweekday() != 1:
                start_date -= timedelta(days=1)
            end_date = start_date + timedelta(days=6)
        else:
            start_date = self.nowDate - timedelta(days=7)
            while start_date.isoweekday() != 1:
                start_date -= timedelta(days=1)
            end_date = start_date + timedelta(days=6)

        return start_date, end_date
    def get_month_date(self, type = "current"):
        if type == "current":
            end_date = date.today().replace(month=date.today().month + 1) - timedelta(days = 1)
            start_date = end_date.replace(day = 1)
        else:
            end_date = date.today().replace(day = 1) - timedelta(days = 1)
            start_date = end_date.replace(day = 1)
        
        return start_date, end_date

    def get_year_date(self, type = "current"):
        if type == "current":
            start_date = self.nowDate.replace(month=self.nowDate.month + 1) - timedelta(days = 1)
            end_date = self.nowDate.replace(day = 1)
        else:
            end_date = self.nowDate.replace(day = 1) - timedelta(days = 1)
            start_date = end_date.replace(day = 1)
        
        return start_date, end_date
    
    def get_month_time(self, type = "current"):

        base = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_time = (base - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
        start_time = base - relativedelta(months=1)
        
        return start_time, end_time

    def get_date_time(self):

        start_time = datetime.combine(self.nowDate, datetime.min.time())
        end_time = start_time.replace(hour=23, minute=59, second=59, microsecond=999999)

        
        return start_time, end_time

# %% 
def write_to_excel(filename, df, output_folder, column=None, name="Report", date = date.today(), format=False):

    # output_folder = check_folder(output_folder)
    if len(df)>1000000:
        file =[]
        step = 700000
        start =0
        i=0
        while start<len(df):
            file.append(write_to_excel(filename+"_"+str(i),df[start:start+step]))
            i+=1
            start = start+step

        return file
    else:
        if date:
            file = os.path.join(output_folder, filename + "_" + date.strftime("%Y-%m-%d") + ".xlsx")
        else:
            file = os.path.join(output_folder, filename  + ".xlsx")
        writer = pd.ExcelWriter(file, engine='xlsxwriter')
        if format:
            if column is None:
                df.to_excel(writer, index=False, encoding='utf-8', sheet_name=name,float_format="%.2f")
            else:
                df.to_excel(writer, columns=column, index=False, encoding='utf-8', sheet_name=name,float_format="%.2f")
        else:
            if column is None:
                df.to_excel(writer, index=False, encoding='utf-8', sheet_name=name )
            else:
                df = df.filter(items=column)
                df.to_excel(writer, columns=column, index=False, encoding='utf-8', sheet_name=name )
        writer.save()
        writer.close()

        return file

# %% 
def write_multiple_df_to_excel(filename:str, data:dict, output_folder:str, date = date.today()):
    # output_folder = check_folder(output_folder)

    file = os.path.join(output_folder, filename + date.strftime("%Y-%m-%d") + ".xlsx")
    writer = pd.ExcelWriter(file, engine='xlsxwriter')

    for sheet, row in data.items():
        if "column" in row:
            _column = [x for x in row["column"] if x in row["data"].columns]
            row["data"].to_excel(writer, columns=_column, index=False, encoding='utf-8', sheet_name=sheet)
        else:
            row["data"].to_excel(writer,   index=False, encoding='utf-8', sheet_name=sheet)

    writer.save()
    writer.close()

    return file

# %%
def get_symbol_info():

    query = """
    select * from symbol_table
    """
    df = get_data(query, 'datatp', 'symbol')
    df = df[['server', 'symbol', 'clean_symbol', 'CCY', 'symbol_type', 'Digits', 'ContractSize']]
    conditions = [(df.server.eq('MT5 BHS')),
                (df.server.eq('MT5 PUG')),
                (df.server.eq('MT5')),
                (df.server.eq('VT1')),
                (df.server.eq('VT2'))]
    results = ['mt5_1', 'MT5_PUG_Live', 'MT5_VFX_Live', 'vtm1report', 'vtm2report']
    df['server'] = np.select(conditions, results, default=df.server)
    df = df.assign(symbol_type = lambda x :np.where(x.symbol.str.contains('[\\.]+r$') & x.symbol_type.eq('Index'), 'Index.r', 
                                           np.where(x.symbol.str.contains("Nikkei225"), "CFD's Asia", x.symbol_type)))
    df = df.drop_duplicates()
    return df


# %% 
def dict_db_server(schema:str, table:str = None):
    excel = "//192.168.1.20/Rmc/Data Analysis/Dropbox/Model_AUTO/package/db_switch.xlsx"
    df_mapping = pd.read_excel(excel)
    
    if schema == 'allothers' and table == 'allaccount':
        df_mapping = df_mapping.drop_duplicates(subset=['allaccount'])
        mapping = dict(zip(df_mapping.allaccount, df_mapping.mt45))

    if schema == 'allothers' and table == 'volume':
        df_mapping = df_mapping.drop_duplicates(subset=['volume_db'])
        mapping = dict(zip(df_mapping.volume_db, df_mapping.mt45))

    if schema == 'detail':
        df_mapping = df_mapping.drop_duplicates(subset=['detail'])
        mapping = dict(zip(df_mapping.detail, df_mapping.mt45))       
    
    if schema == 'symbol':
        df_mapping = df_mapping.drop_duplicates(subset=['symbol_db'])
        mapping = dict(zip(df_mapping.symbol_db, df_mapping.mt45))   
        
    return mapping


def get_symbol_categpory():

    query = """
    select distinct symbol, clean_symbol, symbol_type from symbol_table
    """
    df = get_data(query, 'datatp', 'symbol')

    return df


@log_exception
def if_else(expression_result: Any, true_value: Any,
            false_value: Any, missing_value: np.nan) -> Any:
    """
    Repeats logic from R function if_else
    :param expression_result: result of expressions, can be True, False, NA
    :param true_value: return its value if expression_result equals True
    :param false_value: return its value if expression_result equals False
    :param missing_value: return its value if expression_result equals NA
    :return: string value of true_value, false_value, missing_value
    """
    try:
        if pd.isna(expression_result):
            return missing_value
        elif expression_result:
            return true_value
        else:
            return false_value
    except Exception as e:
        logger.error('expression_result: {}'.format(expression_result))
        logger.error('true_value: {}'.format(true_value))
        logger.error('false_value: {}'.format(false_value))
        raise e