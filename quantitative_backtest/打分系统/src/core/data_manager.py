# -*- coding: utf-8 -*-
"""
数据管理器
负责数据获取、存储、备份和统一访问接口

项目：基本面量化回测系统
作者：量化团队
创建时间：2025-06-26
"""

import os
import time
import logging
import gzip
import shutil
import psycopg2
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy import create_engine, text
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

# 导入配置文件
try:
    from config.data_config import (
        DATABASE_CONFIG, TUSHARE_CONFIG, DATE_FIELDS, STOCK_CODE_FIELDS,
        PROFITABILITY_DATA_SOURCES, GROWTH_DATA_SOURCES, SAFETY_DATA_SOURCES,
        MARKET_CAP_DATA_SOURCES, ALL_TABLES, DATA_PATHS
    )
except ImportError:
    print("警告：无法导入数据配置，请确保在项目根目录运行并且config模块正确安装")

# ==============================================================================
# 1. 配置和日志设置
# ==============================================================================

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class UpdateConfig:
    """数据更新配置"""
    financial_update_months: List[int] = None  # 财务数据更新月份
    market_update_frequency: str = "daily"    # 市场数据更新频率
    api_call_interval: float = 0.2            # API调用间隔（秒）
    max_retries: int = 3                      # 最大重试次数
    backup_retention_days: int = 90           # 备份保留天数
    update_counter_reset: int = 4             # 更新计数器重置值

    def __post_init__(self):
        if self.financial_update_months is None:
            self.financial_update_months = [4, 8, 10]  # 4月、8月、10月末

# ==============================================================================
# 2. 数据库连接管理器
# ==============================================================================

class DatabaseManager:
    """数据库连接管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.engine = None
        self.connection_string = self._build_connection_string()
        
    def _build_connection_string(self) -> str:
        """构建数据库连接字符串"""
        return (f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}")
    
    def get_engine(self):
        """获取SQLAlchemy引擎"""
        if self.engine is None:
            self.engine = create_engine(
                self.connection_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True
            )
        return self.engine
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.fetchone()[0] == 1
        except Exception as e:
            logger.error(f"数据库连接测试失败: {e}")
            return False
    
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        try:
            engine = self.get_engine()
            return pd.read_sql(query, engine, params=params)
        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            raise
    
    def execute_update(self, query: str, params: Dict = None) -> int:
        """执行更新操作"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                conn.commit()
                return result.rowcount
        except Exception as e:
            logger.error(f"更新执行失败: {e}")
            raise

# ==============================================================================
# 3. Tushare API管理器
# ==============================================================================

class TushareManager:
    """Tushare API管理器"""
    
    def __init__(self, config: Dict[str, Any], api_interval: float = 0.2):
        self.config = config
        self.api_interval = api_interval
        self.pro = None
        self._init_api()
    
    def _init_api(self):
        """初始化Tushare API"""
        try:
            ts.set_token(self.config['token'])
            self.pro = ts.pro_api()
            logger.info("Tushare API初始化成功")
        except Exception as e:
            logger.error(f"Tushare API初始化失败: {e}")
            raise
    
    def _safe_api_call(self, api_func, **kwargs):
        """安全的API调用，包含重试机制"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(self.api_interval)  # 控制调用频率
                result = api_func(**kwargs)
                if result is not None and not result.empty:
                    return result
                else:
                    logger.warning(f"API返回空数据，尝试 {attempt + 1}/{max_retries}")
            except Exception as e:
                logger.warning(f"API调用失败，尝试 {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    logger.error(f"API调用最终失败: {e}")
                    raise
        return pd.DataFrame()
    
    def get_stock_basic(self, exchange: str = None) -> pd.DataFrame:
        """获取股票基本信息"""
        return self._safe_api_call(
            self.pro.stock_basic,
            exchange=exchange,
            list_status='L',
            fields='ts_code,symbol,name,area,industry,market,list_date'
        )
    
    def get_financial_indicator(self, ts_code: str = None, start_date: str = None, 
                              end_date: str = None, period: str = None) -> pd.DataFrame:
        """获取财务指标数据"""
        return self._safe_api_call(
            self.pro.fina_indicator,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
    
    def get_balance_sheet(self, ts_code: str = None, start_date: str = None,
                         end_date: str = None, period: str = None) -> pd.DataFrame:
        """获取资产负债表数据"""
        return self._safe_api_call(
            self.pro.balancesheet,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
    
    def get_income_statement(self, ts_code: str = None, start_date: str = None,
                           end_date: str = None, period: str = None) -> pd.DataFrame:
        """获取利润表数据"""
        return self._safe_api_call(
            self.pro.income,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
    
    def get_daily_basic(self, ts_code: str = None, trade_date: str = None,
                       start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取每日基本面数据"""
        return self._safe_api_call(
            self.pro.daily_basic,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date
        )
    
    def get_daily_price(self, ts_code: str = None, trade_date: str = None,
                       start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取日行情数据"""
        return self._safe_api_call(
            self.pro.daily,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date
        )

# ==============================================================================
# 4. 数据备份管理器
# ==============================================================================

class BackupManager:
    """数据备份管理器"""
    
    def __init__(self, backup_dir: str = "data/backup"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def create_backup(self, db_manager: DatabaseManager, backup_type: str = "incremental") -> str:
        """创建数据备份"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_subdir = self.backup_dir / f"{backup_type}_{timestamp}"
        backup_subdir.mkdir(exist_ok=True)
        
        try:
            # 备份所有重要表
            for table_name in ALL_TABLES:
                logger.info(f"备份表: {table_name}")
                
                # 导出为CSV
                query = f"SELECT * FROM {table_name}"
                df = db_manager.execute_query(query)
                
                csv_path = backup_subdir / f"{table_name}.csv"
                df.to_csv(csv_path, index=False)
                
                # 压缩文件
                with open(csv_path, 'rb') as f_in:
                    with gzip.open(f"{csv_path}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # 删除未压缩文件
                csv_path.unlink()
            
            logger.info(f"备份完成: {backup_subdir}")
            return str(backup_subdir)
            
        except Exception as e:
            logger.error(f"备份失败: {e}")
            # 清理失败的备份
            if backup_subdir.exists():
                shutil.rmtree(backup_subdir)
            raise
    
    def restore_backup(self, db_manager: DatabaseManager, backup_path: str):
        """恢复数据备份"""
        backup_dir = Path(backup_path)
        if not backup_dir.exists():
            raise FileNotFoundError(f"备份目录不存在: {backup_path}")
        
        try:
            for gz_file in backup_dir.glob("*.csv.gz"):
                table_name = gz_file.stem.replace('.csv', '')
                logger.info(f"恢复表: {table_name}")
                
                # 解压文件
                csv_path = backup_dir / f"{table_name}_temp.csv"
                with gzip.open(gz_file, 'rb') as f_in:
                    with open(csv_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # 读取数据并写入数据库
                df = pd.read_csv(csv_path)
                engine = db_manager.get_engine()
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                
                # 删除临时文件
                csv_path.unlink()
            
            logger.info(f"恢复完成: {backup_path}")
            
        except Exception as e:
            logger.error(f"恢复失败: {e}")
            raise
    
    def cleanup_old_backups(self, retention_days: int = 90):
        """清理旧备份"""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        for backup_subdir in self.backup_dir.iterdir():
            if backup_subdir.is_dir():
                # 从目录名提取日期
                try:
                    date_str = backup_subdir.name.split('_')[1] + '_' + backup_subdir.name.split('_')[2]
                    backup_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                    
                    if backup_date < cutoff_date:
                        logger.info(f"删除过期备份: {backup_subdir}")
                        shutil.rmtree(backup_subdir)
                        
                except (IndexError, ValueError):
                    logger.warning(f"无法解析备份目录日期: {backup_subdir}")

# ==============================================================================
# 5. 数据更新器
# ==============================================================================

class DataUpdater:
    """数据更新器"""
    
    def __init__(self, db_manager: DatabaseManager, tushare_manager: TushareManager,
                 backup_manager: BackupManager, config: UpdateConfig):
        self.db_manager = db_manager
        self.tushare_manager = tushare_manager
        self.backup_manager = backup_manager
        self.config = config
        self.update_counter = self._load_update_counter()
    
    def _load_update_counter(self) -> int:
        """加载更新计数器"""
        counter_file = Path("data/update_counter.txt")
        if counter_file.exists():
            try:
                return int(counter_file.read_text().strip())
            except:
                return 0
        return 0
    
    def _save_update_counter(self):
        """保存更新计数器"""
        counter_file = Path("data/update_counter.txt")
        counter_file.parent.mkdir(exist_ok=True)
        counter_file.write_text(str(self.update_counter))
    
    def update_financial_data(self, full_update: bool = None):
        """更新财务数据"""
        self.update_counter += 1
        
        # 决定是否全量更新
        if full_update is None:
            full_update = (self.update_counter % self.config.update_counter_reset == 0)
        
        update_type = "全量" if full_update else "增量"
        logger.info(f"开始{update_type}更新财务数据 (第{self.update_counter}次)")
        
        try:
            # 全量更新前先备份
            if full_update:
                logger.info("创建全量更新前备份...")
                self.backup_manager.create_backup(self.db_manager, "before_full_update")
            
            # 获取股票列表
            stock_list = self._get_stock_universe()
            logger.info(f"获取到{len(stock_list)}只股票")
            
            # 更新各类财务数据
            self._update_financial_indicators(stock_list, full_update)
            self._update_balance_sheets(stock_list, full_update)
            self._update_income_statements(stock_list, full_update)
            
            # 保存更新计数器
            self._save_update_counter()
            
            logger.info(f"财务数据{update_type}更新完成")
            
        except Exception as e:
            logger.error(f"财务数据更新失败: {e}")
            raise
    
    def update_market_data(self, trade_date: str = None):
        """更新市场数据"""
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y%m%d")
        
        logger.info(f"开始更新市场数据: {trade_date}")
        
        try:
            # 获取股票列表
            stock_list = self._get_stock_universe()
            
            # 更新日行情数据
            self._update_daily_prices(stock_list, trade_date)
            
            # 更新每日基本面数据
            self._update_daily_basic(stock_list, trade_date)
            
            logger.info(f"市场数据更新完成: {trade_date}")
            
        except Exception as e:
            logger.error(f"市场数据更新失败: {e}")
            raise
    
    def _get_stock_universe(self) -> List[str]:
        """获取股票池"""
        try:
            # 从数据库获取
            query = "SELECT DISTINCT ts_code FROM stock_basic_info WHERE ts_code IS NOT NULL"
            df = self.db_manager.execute_query(query)
            
            if df.empty:
                # 如果数据库为空，从API获取
                logger.info("从API获取股票基本信息...")
                basic_info = self.tushare_manager.get_stock_basic()
                if not basic_info.empty:
                    # 保存到数据库
                    engine = self.db_manager.get_engine()
                    basic_info.to_sql('stock_basic_info', engine, if_exists='replace', index=False)
                    return basic_info['ts_code'].tolist()
                else:
                    return []
            else:
                return df['ts_code'].tolist()
                
        except Exception as e:
            logger.error(f"获取股票池失败: {e}")
            return []
    
    def _update_financial_indicators(self, stock_list: List[str], full_update: bool):
        """更新财务指标数据"""
        logger.info("更新财务指标数据...")
        
        # 批量处理，避免单次调用过多
        batch_size = 50
        total_batches = len(stock_list) // batch_size + 1
        
        for i, batch_start in enumerate(range(0, len(stock_list), batch_size)):
            batch_end = min(batch_start + batch_size, len(stock_list))
            batch_stocks = stock_list[batch_start:batch_end]
            
            logger.info(f"处理财务指标批次 {i+1}/{total_batches}")
            
            for ts_code in batch_stocks:
                try:
                    # 获取最近8个季度的数据
                    df = self.tushare_manager.get_financial_indicator(
                        ts_code=ts_code,
                        start_date="20180101"  # 从2018年开始
                    )
                    
                    if not df.empty:
                        # 保存到数据库
                        engine = self.db_manager.get_engine()
                        df.to_sql(
                            'stock_financial_indicators',
                            engine,
                            if_exists='append' if not full_update else 'replace',
                            index=False,
                            method='multi'
                        )
                        
                except Exception as e:
                    logger.warning(f"更新{ts_code}财务指标失败: {e}")
                    continue
    
    def _update_balance_sheets(self, stock_list: List[str], full_update: bool):
        """更新资产负债表数据"""
        logger.info("更新资产负债表数据...")
        # 类似财务指标的处理逻辑
        pass
    
    def _update_income_statements(self, stock_list: List[str], full_update: bool):
        """更新利润表数据"""
        logger.info("更新利润表数据...")
        # 类似财务指标的处理逻辑
        pass
    
    def _update_daily_prices(self, stock_list: List[str], trade_date: str):
        """更新日行情数据"""
        logger.info(f"更新日行情数据: {trade_date}")
        # 实现日行情数据更新
        pass
    
    def _update_daily_basic(self, stock_list: List[str], trade_date: str):
        """更新每日基本面数据"""
        logger.info(f"更新每日基本面数据: {trade_date}")
        # 实现每日基本面数据更新
        pass

# ==============================================================================
# 6. 统一数据访问接口
# ==============================================================================

class DataAccessor:
    """统一数据访问接口"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def get_financial_data(self, stock_codes: Union[str, List[str]], 
                          start_date: str, end_date: str,
                          indicators: List[str] = None) -> pd.DataFrame:
        """
        获取财务数据
        
        Args:
            stock_codes: 股票代码或代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            indicators: 指标列表，如['q_roe', 'roa_yearly']
            
        Returns:
            DataFrame: 包含财务数据的DataFrame
        """
        if isinstance(stock_codes, str):
            stock_codes = [stock_codes]
        
        # 构建查询条件
        codes_str = "'" + "','".join(stock_codes) + "'"
        
        if indicators:
            fields = ['ts_code', 'end_date'] + indicators
            fields_str = ','.join(fields)
        else:
            fields_str = '*'
        
        query = f"""
        SELECT {fields_str}
        FROM stock_financial_indicators
        WHERE ts_code IN ({codes_str})
        AND end_date >= '{start_date.replace('-', '')}'
        AND end_date <= '{end_date.replace('-', '')}'
        ORDER BY ts_code, end_date
        """
        
        return self.db_manager.execute_query(query)
    
    def get_market_data(self, stock_codes: Union[str, List[str]],
                       start_date: str, end_date: str,
                       fields: List[str] = None) -> pd.DataFrame:
        """
        获取市场数据 - 自动选择正确的数据表
        
        Args:
            stock_codes: 股票代码或代码列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            fields: 字段列表，如['close', 'total_mv']
            
        Returns:
            DataFrame: 包含市场数据的DataFrame
        """
        if isinstance(stock_codes, str):
            stock_codes = [stock_codes]
        
        codes_str = "'" + "','".join(stock_codes) + "'"
        
        if fields:
            fields_list = ['ts_code', 'trade_date'] + fields
            fields_str = ','.join(fields_list)
        else:
            fields_str = '*'
        
        # 根据请求的字段选择正确的表
        basic_fields = {'total_mv', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 
                       'total_share', 'float_share', 'free_share', 'circ_mv', 'turnover_rate', 
                       'turnover_rate_f', 'volume_ratio'}
        
        if fields and any(field in basic_fields for field in fields):
            # 使用 stock_daily_basic 表
            table_name = "stock_daily_basic"
        else:
            # 使用 a_share_daily_data 表
            table_name = "a_share_daily_data"
        
        query = f"""
        SELECT {fields_str}
        FROM {table_name}
        WHERE ts_code IN ({codes_str})
        AND trade_date >= '{start_date.replace('-', '')}'
        AND trade_date <= '{end_date.replace('-', '')}'
        ORDER BY ts_code, trade_date
        """
        
        return self.db_manager.execute_query(query)
    def get_stock_basic_info(self, stock_codes: Union[str, List[str]] = None) -> pd.DataFrame:
        """
        获取股票基础信息
        
        Args:
            stock_codes: 股票代码或代码列表，None表示获取全部
            
        Returns:
            DataFrame: 包含股票基础信息的DataFrame
        """
        if stock_codes is None:
            query = "SELECT * FROM stock_basic_info ORDER BY ts_code"
        else:
            if isinstance(stock_codes, str):
                stock_codes = [stock_codes]
            codes_str = "'" + "','".join(stock_codes) + "'"
            query = f"SELECT * FROM stock_basic_info WHERE ts_code IN ({codes_str}) ORDER BY ts_code"
        
        return self.db_manager.execute_query(query)
    
    def get_benchmark_data(self, benchmark_code: str = "000300.SH",
                          start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取基准指数数据
        
        Args:
            benchmark_code: 基准代码，默认沪深300
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            DataFrame: 包含基准数据的DataFrame
        """
        query = f"""
        SELECT ts_code, trade_date, close, pct_chg
        FROM a_share_daily_data
        WHERE ts_code = '{benchmark_code}'
        """
        
        if start_date:
            query += f" AND trade_date >= '{start_date.replace('-', '')}'"
        if end_date:
            query += f" AND trade_date <= '{end_date.replace('-', '')}'"
        
        query += " ORDER BY trade_date"
        
        return self.db_manager.execute_query(query)

# ==============================================================================
# 7. 主数据管理器
# ==============================================================================

class DataManager:
    """主数据管理器 - 统一入口"""
    
    def __init__(self, config_override: Dict = None):
        # 加载配置
        self.config = UpdateConfig()
        if config_override:
            for key, value in config_override.items():
                setattr(self.config, key, value)
        
        # 初始化各管理器
        self.db_manager = DatabaseManager(DATABASE_CONFIG)
        self.tushare_manager = TushareManager(TUSHARE_CONFIG, self.config.api_call_interval)
        self.backup_manager = BackupManager()
        self.data_updater = DataUpdater(
            self.db_manager, self.tushare_manager, 
            self.backup_manager, self.config
        )
        self.data_accessor = DataAccessor(self.db_manager)
        
        # 调度器
        self.scheduler = None
        
        # 初始化检查
        self._initialize()
    
    def _initialize(self):
        """初始化检查"""
        # 测试数据库连接
        if not self.db_manager.test_connection():
            raise ConnectionError("数据库连接失败")
        
        # 创建必要目录
        for path_key, path_value in DATA_PATHS.items():
            Path(path_value).mkdir(parents=True, exist_ok=True)
        
        logger.info("数据管理器初始化完成")
    
    # 数据访问接口（委托给DataAccessor）
    def get_financial_data(self, *args, **kwargs) -> pd.DataFrame:
        """获取财务数据"""
        return self.data_accessor.get_financial_data(*args, **kwargs)
    
    def get_market_data(self, *args, **kwargs) -> pd.DataFrame:
        """获取市场数据"""
        return self.data_accessor.get_market_data(*args, **kwargs)
    
    def get_stock_basic_info(self, *args, **kwargs) -> pd.DataFrame:
        """获取股票基础信息"""
        return self.data_accessor.get_stock_basic_info(*args, **kwargs)
    
    def get_benchmark_data(self, *args, **kwargs) -> pd.DataFrame:
        """获取基准数据"""
        return self.data_accessor.get_benchmark_data(*args, **kwargs)
    
    # 数据更新接口（委托给DataUpdater）
    def update_financial_data(self, *args, **kwargs):
        """更新财务数据"""
        return self.data_updater.update_financial_data(*args, **kwargs)
    
    def update_market_data(self, *args, **kwargs):
        """更新市场数据"""
        return self.data_updater.update_market_data(*args, **kwargs)
    
    # 备份接口（委托给BackupManager）
    def create_backup(self, backup_type: str = "manual") -> str:
        """创建备份"""
        return self.backup_manager.create_backup(self.db_manager, backup_type)
    
    def restore_backup(self, backup_path: str):
        """恢复备份"""
        return self.backup_manager.restore_backup(self.db_manager, backup_path)
    
    # 调度器接口
    def start_scheduler(self, mode: str = "background"):
        """启动调度器"""
        if mode == "background":
            self.scheduler = BackgroundScheduler(
                executors={'default': ThreadPoolExecutor(2)}
            )
        else:
            self.scheduler = BlockingScheduler(
                executors={'default': ThreadPoolExecutor(2)}
            )
        
        # 添加财务数据更新任务
        self.scheduler.add_job(
            func=self.update_financial_data,
            trigger='cron',
            month=','.join(map(str, self.config.financial_update_months)),
            day='last',  # 月末最后一天
            hour=23,
            minute=0,
            id='financial_data_update'
        )
        
        # 添加市场数据更新任务
        self.scheduler.add_job(
            func=self.update_market_data,
            trigger='cron',
            day_of_week='mon-fri',  # 工作日
            hour=18,
            minute=0,
            id='market_data_update'
        )
        
        # 添加备份清理任务
        self.scheduler.add_job(
            func=self.backup_manager.cleanup_old_backups,
            trigger='cron',
            day=1,  # 每月1日
            hour=2,
            minute=0,
            kwargs={'retention_days': self.config.backup_retention_days},
            id='backup_cleanup'
        )
        
        logger.info("调度器配置完成")
        self.scheduler.start()
        logger.info(f"调度器启动完成 (模式: {mode})")
    
    def stop_scheduler(self):
        """停止调度器"""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("调度器已停止")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        if not self.scheduler:
            return {"status": "未初始化", "jobs": []}
        
        jobs_info = []
        for job in self.scheduler.get_jobs():
            jobs_info.append({
                "id": job.id,
                "name": job.name or job.func.__name__,
                "next_run": str(job.next_run_time) if job.next_run_time else "未安排",
                "trigger": str(job.trigger)
            })
        
        return {
            "status": "运行中" if self.scheduler.running else "已停止",
            "jobs": jobs_info
        }
    
    def manual_update(self, data_type: str = "all", **kwargs):
        """手动触发数据更新"""
        logger.info(f"手动触发数据更新: {data_type}")
        
        try:
            if data_type in ["all", "financial"]:
                self.update_financial_data(**kwargs)
            
            if data_type in ["all", "market"]:
                self.update_market_data(**kwargs)
            
            logger.info("手动更新完成")
            
        except Exception as e:
            logger.error(f"手动更新失败: {e}")
            raise

# ==============================================================================
# 8. 数据质量检查器
# ==============================================================================

class DataQualityChecker:
    """数据质量检查器"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def check_data_completeness(self, table_name: str, date_column: str,
                              start_date: str, end_date: str) -> Dict[str, Any]:
        """检查数据完整性"""
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT ts_code) as unique_stocks,
            MIN({date_column}) as earliest_date,
            MAX({date_column}) as latest_date,
            COUNT(*) - COUNT(ts_code) as missing_stock_codes
        FROM {table_name}
        WHERE {date_column} >= '{start_date.replace('-', '')}'
        AND {date_column} <= '{end_date.replace('-', '')}'
        """
        
        result = self.db_manager.execute_query(query)
        return result.iloc[0].to_dict() if not result.empty else {}
    
    def check_data_anomalies(self, table_name: str, numeric_columns: List[str]) -> Dict[str, Any]:
        """检查数据异常值"""
        anomalies = {}
        
        for column in numeric_columns:
            query = f"""
            SELECT 
                COUNT(*) as total,
                COUNT({column}) as non_null,
                AVG({column}) as mean_value,
                STDDEV({column}) as std_value,
                MIN({column}) as min_value,
                MAX({column}) as max_value
            FROM {table_name}
            WHERE {column} IS NOT NULL
            """
            
            result = self.db_manager.execute_query(query)
            if not result.empty:
                anomalies[column] = result.iloc[0].to_dict()
        
        return anomalies
    
    def generate_quality_report(self, tables_config: Dict[str, Dict]) -> Dict[str, Any]:
        """生成数据质量报告"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "tables": {}
        }
        
        for table_name, config in tables_config.items():
            logger.info(f"检查表: {table_name}")
            
            table_report = {}
            
            # 完整性检查
            if "date_column" in config:
                completeness = self.check_data_completeness(
                    table_name,
                    config["date_column"],
                    config.get("start_date", "20180101"),
                    config.get("end_date", datetime.now().strftime("%Y%m%d"))
                )
                table_report["completeness"] = completeness
            
            # 异常值检查
            if "numeric_columns" in config:
                anomalies = self.check_data_anomalies(
                    table_name,
                    config["numeric_columns"]
                )
                table_report["anomalies"] = anomalies
            
            report["tables"][table_name] = table_report
        
        return report

# ==============================================================================
# 9. 工具函数
# ==============================================================================

def create_data_manager(config_override: Dict = None) -> DataManager:
    """创建数据管理器实例的工厂函数"""
    return DataManager(config_override)

def validate_date_format(date_str: str) -> bool:
    """验证日期格式"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def convert_date_format(date_str: str, from_format: str = "%Y-%m-%d", 
                       to_format: str = "%Y%m%d") -> str:
    """转换日期格式"""
    dt = datetime.strptime(date_str, from_format)
    return dt.strftime(to_format)

# ==============================================================================
# 10. 使用示例和测试
# ==============================================================================

if __name__ == "__main__":
    # 使用示例
    logger.info("=== 数据管理器测试 ===")
    
    try:
        # 创建数据管理器
        dm = create_data_manager()
        
        # 测试基本功能
        logger.info("测试数据库连接...")
        if dm.db_manager.test_connection():
            logger.info("✅ 数据库连接正常")
        else:
            logger.error("❌ 数据库连接失败")
            exit(1)
        
        # 获取股票基础信息示例
        logger.info("获取股票基础信息...")
        basic_info = dm.get_stock_basic_info()
        logger.info(f"获取到 {len(basic_info)} 只股票的基础信息")
        
        # 获取财务数据示例
        if not basic_info.empty:
            sample_codes = basic_info['ts_code'].head(3).tolist()
            logger.info(f"获取样本股票财务数据: {sample_codes}")
            
            financial_data = dm.get_financial_data(
                stock_codes=sample_codes,
                start_date="2023-01-01",
                end_date="2023-12-31",
                indicators=['q_roe', 'roa_yearly', 'grossprofit_margin']
            )
            logger.info(f"获取到 {len(financial_data)} 条财务数据")
        
        # 测试调度器状态
        logger.info("调度器状态:")
        status = dm.get_scheduler_status()
        logger.info(f"状态: {status['status']}")
        logger.info(f"任务数量: {len(status['jobs'])}")
        
        # 数据质量检查示例
        logger.info("执行数据质量检查...")
        quality_checker = DataQualityChecker(dm.db_manager)
        
        # 检查财务指标表
        tables_to_check = {
            "stock_financial_indicators": {
                "date_column": "end_date",
                "numeric_columns": ["q_roe", "roa_yearly", "grossprofit_margin"],
                "start_date": "20230101",
                "end_date": "20231231"
            }
        }
        
        quality_report = quality_checker.generate_quality_report(tables_to_check)
        logger.info("数据质量检查完成")
        
        # 创建手动备份示例
        logger.info("创建手动备份...")
        backup_path = dm.create_backup("manual_test")
        logger.info(f"备份创建完成: {backup_path}")
        
        logger.info("=== 测试完成 ===")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise
    
    finally:
        # 清理资源
        if 'dm' in locals():
            dm.stop_scheduler()
            logger.info("资源清理完成")

# ==============================================================================
# 11. 导出接口
# ==============================================================================

__all__ = [
    'DataManager',
    'DatabaseManager', 
    'TushareManager',
    'BackupManager',
    'DataUpdater',
    'DataAccessor',
    'DataQualityChecker',
    'create_data_manager',
    'validate_date_format',
    'convert_date_format'
]