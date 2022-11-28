import sys
from package.logging_helper import logger

def main(report):
    if report == 'test1':
        from test.test import generate_report
        generate_report(['enfaureport'])

if __name__ == "__main__":
    args = sys.argv
    report_name = args[1]
    logger.info(f'args[0]:{args[0]} , args[1]:{args[1]}')
    main(report_name)