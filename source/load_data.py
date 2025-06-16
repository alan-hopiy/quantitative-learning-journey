# file: data_loader.py (建议给文件也取一个更通用的名字)

import pandas as pd
from sqlalchemy import create_engine
import logging
import config  # 假设你的数据库配置在config.py中

def load_data_from_db(table_name: str, columns: list = None):
    """
    【通用版】连接数据库，并从指定的表中加载数据。

    Args:
        table_name (str): 需要加载数据的数据库表名。
                          例如: 'stock_financial_indicators' 或 'stock_balance_sheets'。
        columns (list, optional): 一个包含列名的列表，用于指定需要加载哪些列。
                                  如果为 None (默认)，则加载所有列 ('SELECT *')。

    Returns:
        pd.DataFrame: 包含所请求数据的DataFrame。
        None: 如果加载失败。
    """
    logging.info(f"准备从数据库表 '{table_name}' 加载数据...")
    
    # 1. 决定要查询的列
    if columns and isinstance(columns, list):
        # 如果提供了列名列表，则构建 "SELECT col1, col2, ..."
        column_str = ", ".join(columns)
        logging.info(f"目标列: {column_str}")
    else:
        # 否则，加载所有列
        column_str = "*"
        logging.info("目标列: * (所有列)")

    try:
        # 2. 创建数据库连接
        db_uri = f"postgresql+psycopg2://{config.DB_USER}:{config.DB_PASS}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
        engine = create_engine(db_uri)
        
        # 3. 构建动态的SQL查询语句
        query = f"SELECT {column_str} FROM {table_name};"
        
        # 4. 从数据库读取数据
        df = pd.read_sql(query, engine)
        
        logging.info(f"成功从 '{table_name}' 加载 {len(df)} 行, {len(df.columns)} 列数据。")
        return df
        
    except Exception as e:
        logging.error(f"从表 '{table_name}' 加载数据失败: {e}")
        return None

# ==============================================================================
#  当独立运行此脚本时，执行以下测试代码
# ==============================================================================
if __name__ == '__main__':
    # 配置日志格式，方便调试
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("\n--- 测试 1: 加载'财务指标表' (stock_financial_indicators) 的所有数据 ---")
    df_indicators = load_data_from_db(table_name='stock_financial_indicators')
    if df_indicators is not None:
        print("数据形状 (Shape):", df_indicators.shape)
        print("前5行数据:")
        print(df_indicators.head())

    print("\n" + "="*80 + "\n")

    print("--- 测试 2: 加载'资产负债表' (stock_balance_sheets) 的所有数据 ---")
    df_balance_sheets = load_data_from_db(table_name='stock_balance_sheets')
    if df_balance_sheets is not None:
        print("数据形状 (Shape):", df_balance_sheets.shape)
        print("前5行数据:")
        print(df_balance_sheets.head())

    print("\n" + "="*80 + "\n")
    
    print("--- 测试 3: 从'资产负债表'中只加载特定几列数据 ---")
    # 演示如何使用可选的 columns 参数
    specific_cols = ['ts_code', 'end_date', 'total_assets', 'total_liab']
    df_bs_partial = load_data_from_db(table_name='stock_balance_sheets', columns=specific_cols)
    if df_bs_partial is not None:
        print("数据形状 (Shape):", df_bs_partial.shape)
        print("数据类型 (Dtypes):")
        # .info() 会打印更详细的信息
        df_bs_partial.info()