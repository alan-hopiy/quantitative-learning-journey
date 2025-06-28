# -*- coding: utf-8 -*-
"""
打分系统配置文件
专门用于股票评分系统的参数配置

项目：基本面量化回测系统 - 打分系统
作者：量化团队
创建时间：2025-06-28
"""

from typing import Dict, List, Any
from pathlib import Path

# ==============================================================================
# 1. 基本运行参数
# ==============================================================================

BASIC_CONFIG = {
    # 默认计算日期（可在主程序中修改）
    "default_calculation_date": "2022-05-01",
    
    # 输出控制
    "output_all_stocks": True,          # True=输出全部股票，False=只输出前N名
    "output_top_n": 100,                # 当output_all_stocks=False时，输出前N名
    
    # 处理参数
    "batch_size": 50,                   # 批处理大小（一次处理多少只股票）
    "max_workers": 4,                   # 多线程处理的最大工作线程数
    
    # 数据要求
    "min_data_quarters": 2,             # 最少需要几个季度的数据才参与计算
    "enable_progress_bar": True         # 是否显示进度条
}

# ==============================================================================
# 2. 股票筛选规则
# ==============================================================================

STOCK_FILTER_CONFIG = {
    # ST股票筛选
    "exclude_st_stocks": True,          # 排除ST股票
    "st_keywords": ["ST", "*ST", "PT"], # ST股票识别关键词
    
    # 市值筛选
    "enable_market_cap_filter": False,  # 是否启用市值筛选
    "min_market_cap": 10,               # 最小市值要求（亿元，当启用时）
    
    # 上市时间筛选
    "enable_listing_days_filter": False, # 是否启用上市时间筛选
    "min_listing_days": 252,            # 最少上市天数（当启用时）
    
    # 交易活跃度筛选
    "enable_trading_filter": False,     # 是否启用交易活跃度筛选
    "min_trading_days_ratio": 0.8       # 最近一年交易天数比例（当启用时）
}

# ==============================================================================
# 3. Excel输出配置
# ==============================================================================

OUTPUT_CONFIG = {
    # 输出路径
    "output_directory": "output/scores/",
    "log_directory": "output/logs/",
    
    # 文件命名
    "filename_template": "股票评分_{date}.xlsx",      # 文件名模板
    "backup_filename_template": "股票评分_{date}_{timestamp}.xlsx",  # 备份文件名
    
    # Excel格式设置
    "excel_settings": {
        "sheet_name": "股票评分结果",
        "freeze_panes": (1, 3),            # 冻结前3列
        "auto_filter": True,               # 启用自动筛选
        "column_width": {                  # 列宽设置
            "ts_code": 12,
            "stock_name": 15,
            "final_score": 12,
            "final_rank": 10,
            "default": 10
        }
    },
    
    # 数值格式化
    "number_format": {
        "factor_values": "0.0000",         # 因子值保留4位小数
        "dimension_scores": "0.0000",      # 维度得分保留4位小数  
        "final_score": "0.0000",           # 最终得分保留4位小数
        "percentage": "0.00%"              # 百分比格式
    }
}

# ==============================================================================
# 4. 输出列配置
# ==============================================================================

OUTPUT_COLUMNS_CONFIG = {
    # 基础信息列
    "basic_info": [
        {"column": "ts_code", "name": "股票代码", "width": 12},
        {"column": "stock_name", "name": "股票名称", "width": 15},
        {"column": "industry", "name": "行业", "width": 12},
        {"column": "market_cap_group", "name": "市值分组", "width": 10}
    ],
    
    # 盈利能力因子列
    "profitability_factors": [
        {"column": "ROE", "name": "ROE", "width": 10},
        {"column": "ROA", "name": "ROA", "width": 10},
        {"column": "毛利率", "name": "毛利率", "width": 10},
        {"column": "自由现金流资产比", "name": "自由现金流资产比", "width": 15},
        {"column": "毛利润资产比", "name": "毛利润资产比", "width": 15},
        {"column": "现金流质量", "name": "现金流质量", "width": 12}
    ],
    
    # 成长能力因子列
    "growth_factors": [
        {"column": "ROE成长性", "name": "ROE成长性", "width": 12},
        {"column": "ROA成长性", "name": "ROA成长性", "width": 12},
        {"column": "毛利率成长性", "name": "毛利率成长性", "width": 15},
        {"column": "自由现金流资产比成长性", "name": "自由现金流资产比成长性", "width": 20},
        {"column": "毛利润资产比成长性", "name": "毛利润资产比成长性", "width": 18}
    ],
    
    # 安全性因子列
    "safety_factors": [
        {"column": "低Beta", "name": "低Beta", "width": 10},
        {"column": "低个股风险", "name": "低个股风险", "width": 12},
        {"column": "低负债率", "name": "低负债率", "width": 10},
        {"column": "奥特曼Z值", "name": "奥特曼Z值", "width": 12}
    ],
    
    # 维度得分列
    "dimension_scores": [
        {"column": "profitability_score", "name": "盈利能力得分", "width": 15},
        {"column": "growth_score", "name": "成长能力得分", "width": 15},
        {"column": "safety_score", "name": "安全性得分", "width": 12}
    ],
    
    # 最终结果列
    "final_results": [
        {"column": "final_score", "name": "最终得分", "width": 12},
        {"column": "final_rank", "name": "最终排名", "width": 10}
    ]
}

# ==============================================================================
# 5. 日志和错误处理配置
# ==============================================================================

LOGGING_CONFIG = {
    # 日志级别
    "log_level": "INFO",                # DEBUG, INFO, WARNING, ERROR
    "console_log_level": "INFO",        # 控制台输出级别
    "file_log_level": "DEBUG",          # 文件输出级别
    
    # 日志文件设置
    "log_file_template": "scoring_{date}.log",
    "max_log_file_size": 10,            # MB
    "log_file_backup_count": 5,
    
    # 错误处理
    "error_handling": {
        "continue_on_stock_error": True,    # 单只股票计算失败时是否继续
        "save_error_details": True,         # 是否保存错误详情
        "error_log_file": "scoring_errors_{date}.log"
    },
    
    # 统计信息
    "statistics": {
        "show_progress": True,              # 显示处理进度
        "show_summary": True,               # 显示计算总结
        "save_statistics": True             # 保存统计信息到文件
    }
}

# ==============================================================================
# 6. 性能和资源配置
# ==============================================================================

PERFORMANCE_CONFIG = {
    # 内存管理
    "memory_management": {
        "chunk_size": 1000,                 # 数据分块大小
        "clear_cache_interval": 100,        # 清理缓存间隔（处理股票数）
        "max_memory_usage_mb": 1024         # 最大内存使用（MB）
    },
    
    # 数据库连接
    "database": {
        "connection_pool_size": 5,          # 连接池大小
        "query_timeout": 30,                # 查询超时时间（秒）
        "retry_count": 3                    # 重试次数
    },
    
    # 计算优化
    "calculation": {
        "enable_parallel": True,            # 启用并行计算
        "parallel_threshold": 20,           # 并行计算阈值（股票数量）
        "cache_intermediate_results": True   # 缓存中间结果
    }
}

# ==============================================================================
# 7. 调试和验证配置
# ==============================================================================

DEBUG_CONFIG = {
    # 调试模式
    "debug_mode": False,                    # 调试模式开关
    "debug_stock_limit": 10,                # 调试模式下处理的股票数量限制
    "debug_save_intermediate": True,        # 保存中间计算结果
    
    # 数据验证
    "validation": {
        "check_data_quality": True,         # 检查数据质量
        "validate_calculation_results": True, # 验证计算结果
        "alert_on_anomaly": True            # 异常数据时提醒
    },
    
    # 测试配置
    "test_mode": {
        "enable_test_mode": False,          # 测试模式
        "test_stock_codes": [               # 测试用股票代码
            "000001.SZ", "000002.SZ", "600000.SH", "600036.SH", "000858.SZ"
        ]
    }
}

# ==============================================================================
# 8. 配置管理器
# ==============================================================================

class ScoringConfigManager:
    """打分系统配置管理器"""
    
    def __init__(self):
        self.basic_config = BASIC_CONFIG
        self.filter_config = STOCK_FILTER_CONFIG
        self.output_config = OUTPUT_CONFIG
        self.columns_config = OUTPUT_COLUMNS_CONFIG
        self.logging_config = LOGGING_CONFIG
        self.performance_config = PERFORMANCE_CONFIG
        self.debug_config = DEBUG_CONFIG
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """获取指定类型的配置"""
        config_map = {
            "basic": self.basic_config,
            "filter": self.filter_config,
            "output": self.output_config,
            "columns": self.columns_config,
            "logging": self.logging_config,
            "performance": self.performance_config,
            "debug": self.debug_config
        }
        
        if config_type not in config_map:
            raise ValueError(f"未知的配置类型: {config_type}")
        
        return config_map[config_type]
    
    def get_all_output_columns(self) -> List[Dict[str, Any]]:
        """获取所有输出列的配置"""
        all_columns = []
        for column_group in self.columns_config.values():
            all_columns.extend(column_group)
        return all_columns
    
    def create_output_directory(self):
        """创建输出目录"""
        output_dir = Path(self.output_config["output_directory"])
        log_dir = Path(self.output_config["log_directory"])
        
        output_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_config(self) -> bool:
        """验证配置的有效性"""
        try:
            # 验证输出目录路径
            output_dir = self.output_config["output_directory"]
            if not output_dir:
                raise ValueError("输出目录不能为空")
            
            # 验证批处理大小
            batch_size = self.basic_config["batch_size"]
            if batch_size <= 0:
                raise ValueError("批处理大小必须大于0")
            
            # 验证其他关键配置...
            
            return True
            
        except Exception as e:
            print(f"配置验证失败: {e}")
            return False

# ==============================================================================
# 9. 全局配置实例和接口函数
# ==============================================================================

# 创建全局配置管理器实例
scoring_config_manager = ScoringConfigManager()

# 常用接口函数
def get_basic_config() -> Dict[str, Any]:
    """获取基本配置"""
    return scoring_config_manager.get_config("basic")

def get_filter_config() -> Dict[str, Any]:
    """获取筛选配置"""
    return scoring_config_manager.get_config("filter")

def get_output_config() -> Dict[str, Any]:
    """获取输出配置"""
    return scoring_config_manager.get_config("output")

def get_columns_config() -> Dict[str, Any]:
    """获取列配置"""
    return scoring_config_manager.get_config("columns")

def get_logging_config() -> Dict[str, Any]:
    """获取日志配置"""
    return scoring_config_manager.get_config("logging")

def get_debug_config() -> Dict[str, Any]:
    """获取调试配置"""
    return scoring_config_manager.get_config("debug")

def initialize_scoring_system():
    """初始化打分系统（创建目录、验证配置等）"""
    # 验证配置
    if not scoring_config_manager.validate_config():
        raise RuntimeError("配置验证失败")
    
    # 创建输出目录
    scoring_config_manager.create_output_directory()
    
    print("✅ 打分系统配置初始化完成")

# ==============================================================================
# 10. 使用示例和测试
# ==============================================================================

if __name__ == "__main__":
    print("=== 打分系统配置测试 ===")
    
    try:
        # 初始化系统
        initialize_scoring_system()
        
        # 测试配置获取
        basic = get_basic_config()
        print(f"默认计算日期: {basic['default_calculation_date']}")
        print(f"批处理大小: {basic['batch_size']}")
        
        filter_conf = get_filter_config()
        print(f"排除ST股票: {filter_conf['exclude_st_stocks']}")
        
        output = get_output_config()
        print(f"输出目录: {output['output_directory']}")
        
        # 测试列配置
        all_columns = scoring_config_manager.get_all_output_columns()
        print(f"总输出列数: {len(all_columns)}")
        
        print("✅ 配置测试通过")
        
    except Exception as e:
        print(f"❌ 配置测试失败: {e}")
        raise