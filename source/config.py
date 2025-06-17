# ==============================================================================
# config.py
# ------------------------------------------------------------------------------
# 项目通用配置文件
# 将所有可变参数、密钥和路径集中在此处，方便统一管理。
# ==============================================================================

import os

# --- Tushare API Token ---
TUSHARE_TOKEN = 'a872d82f46046d335ccf68ef591747ff66b9a9d598b40791b80f017a' # 您的Tushare Pro API密钥

# --- PostgreSQL 数据库配置 ---没有这个东西
DB_HOST = 'localhost'       # 数据库主机地址
DB_NAME = 'mydb'            # 数据库名称
DB_USER = 'alan-hopiy'      # 数据库用户名
DB_PASS = ''                # 数据库密码
DB_PORT = 5432              # 数据库端口 (使用数字更佳)

# --- 文件路径配置 ---
# 使用os模块可以自动获取用户主目录，这样代码在其他电脑上也能正确运行
_user_home_dir = os.path.expanduser('~') 
# 基础输出目录
BASE_OUTPUT_DIR = os.path.join(_user_home_dir, 'Documents', 'quantitative-learning-journey', 'outputs','初步数据')

