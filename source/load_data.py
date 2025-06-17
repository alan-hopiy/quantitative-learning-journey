# file: source/load_data.py

import pandas as pd
from sqlalchemy import create_engine
import logging
# 使用相对导入，正确找到 config.py
from . import config

def load_data_from_db(table_name: str, columns: list = None):
    """
    【通用版】连接到 PostgreSQL 数据库，并从指定的表中加载数据。
    
    Args:
        table_name (str): 要查询的数据库表名。
        columns (list, optional): 要选择的列名列表。如果为None，则选择所有列 ('*')。
        
    Returns:
        pd.DataFrame: 包含加载数据的DataFrame。
        None: 如果加载失败。
    """
    try:
        # --- [核心修正] ---
        # 从 config 模块读取 PostgreSQL 的连接参数
        user = config.DB_USER
        password = config.DB_PASS
        host = config.DB_HOST
        port = config.DB_PORT
        dbname = config.DB_NAME
        
        # 构建 SQLAlchemy 的 PostgreSQL 连接字符串
        # 格式为: 'postgresql://user:password@host:port/dbname'
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # 创建数据库引擎
        engine = create_engine(conn_string)
        
        # --- 后续逻辑不变 ---
        if columns:
            cols_str = ', '.join(f'"{col}"' for col in columns)
            query = f"SELECT {cols_str} FROM {table_name};"
        else:
            query = f"SELECT * FROM {table_name};"
            
        logging.info(f"正在执行查询: {query}")
        df = pd.read_sql(query, engine)
        logging.info(f"成功从表 '{table_name}' 加载了 {len(df)} 行数据。")
        return df

    except Exception as e:
        # 打印详细的错误信息，方便调试
        logging.error(f"从 PostgreSQL 的表 '{table_name}' 加载数据失败: {e}")
        return None

# 注意：我们不再需要那个写死表名的 load_financial_data_from_db() 函数了，
# 因为我们的 data_utils.py 已经更新为直接调用上面这个更通用的函数。
# 这让代码更简洁、更灵活。