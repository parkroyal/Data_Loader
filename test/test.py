from package.db_helper import config, get_data
from package.logging_helper import logger
from typing import List

def generate_report(servers: List):

    query = """
    select database() as server, login, `group` 
    from mt4_users 
    where `group` not regexp 'test'
    """
    df = get_data(query, 'mt4', servers)
    logger.info(f'{df.login[0:5]}')
    return df
    

if __name__ == "__main__":
    generate_report(['enfaureport'])
