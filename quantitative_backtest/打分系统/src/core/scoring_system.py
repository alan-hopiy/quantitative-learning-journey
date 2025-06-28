# -*- coding: utf-8 -*-
"""
打分系统核心模块
整合因子计算、数据管理和结果输出的核心协调器

项目：基本面量化回测系统 - 打分系统
作者：量化团队
创建时间：2025-06-28
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 导入配置和模块
try:
    from config.scoring_config import (
        get_basic_config, get_filter_config, get_output_config,
        get_logging_config, initialize_scoring_system
    )
    from src.core.data_manager import DataManager, create_data_manager
    from src.core.factor_calculator import FactorCalculator, create_factor_calculator
except ImportError as e:
    print(f"警告：无法导入模块: {e}")

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================================
# 1. 股票筛选器
# ==============================================================================

class StockFilter:
    """股票筛选器"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.filter_config = get_filter_config()
    
    def get_stock_universe(self, calculation_date: datetime) -> pd.DataFrame:
        """
        获取股票池
        
        Args:
            calculation_date: 计算日期
            
        Returns:
            DataFrame: 包含股票基础信息的DataFrame
        """
        logger.info("获取股票基础信息...")
        
        # 获取所有股票基础信息
        stock_info = self.data_manager.get_stock_basic_info()
        
        if stock_info.empty:
            logger.error("未获取到股票基础信息")
            return pd.DataFrame()
        
        logger.info(f"获取到 {len(stock_info)} 只股票的基础信息")
        
        # 应用筛选规则
        filtered_stocks = self._apply_filters(stock_info, calculation_date)
        
        logger.info(f"筛选后剩余 {len(filtered_stocks)} 只股票")
        return filtered_stocks
    
    def _apply_filters(self, stock_info: pd.DataFrame, calculation_date: datetime) -> pd.DataFrame:
        """应用各种筛选规则"""
        filtered_data = stock_info.copy()
        
        # 1. 排除ST股票
        if self.filter_config['exclude_st_stocks']:
            filtered_data = self._filter_st_stocks(filtered_data)
        
        # 2. 市值筛选（如果启用）
        if self.filter_config['enable_market_cap_filter']:
            filtered_data = self._filter_by_market_cap(filtered_data, calculation_date)
        
        # 3. 上市时间筛选（如果启用）
        if self.filter_config['enable_listing_days_filter']:
            filtered_data = self._filter_by_listing_date(filtered_data, calculation_date)
        
        return filtered_data
    
    def _filter_st_stocks(self, stock_info: pd.DataFrame) -> pd.DataFrame:
        """排除ST股票"""
        st_keywords = self.filter_config['st_keywords']
        
        # 检查股票名称是否包含ST关键词
        if 'name' in stock_info.columns:
            for keyword in st_keywords:
                mask = ~stock_info['name'].str.contains(keyword, case=False, na=False)
                stock_info = stock_info[mask]
        
        return stock_info
    
    def _filter_by_market_cap(self, stock_info: pd.DataFrame, calculation_date: datetime) -> pd.DataFrame:
        """按市值筛选"""
        min_market_cap = self.filter_config['min_market_cap'] * 10000  # 转换为万元
        
        # 获取最近的市值数据 - 使用正确的数据源
        start_date = calculation_date - timedelta(days=30)
        try:
            # 直接查询stock_daily_basic表获取市值数据
            query = f"""
            SELECT ts_code, trade_date, total_mv
            FROM stock_daily_basic
            WHERE ts_code IN ('{"','".join(stock_info['ts_code'].tolist())}')
            AND trade_date >= '{start_date.strftime("%Y%m%d")}'
            AND trade_date <= '{calculation_date.strftime("%Y%m%d")}'
            ORDER BY ts_code, trade_date
            """
            market_data = self.data_manager.db_manager.execute_query(query)
        except Exception as e:
            logger.warning(f"获取市值数据失败: {e}，跳过市值筛选")
            return stock_info
        
        if not market_data.empty:
            # 取最近的市值数据
            latest_market_cap = market_data.groupby('ts_code')['total_mv'].last()
            
            # 筛选市值满足条件的股票
            valid_stocks = latest_market_cap[latest_market_cap >= min_market_cap].index
            stock_info = stock_info[stock_info['ts_code'].isin(valid_stocks)]
        
        return stock_info
    
    def _filter_by_listing_date(self, stock_info: pd.DataFrame, calculation_date: datetime) -> pd.DataFrame:
        """按上市时间筛选"""
        if 'list_date' not in stock_info.columns:
            return stock_info
        
        min_listing_days = self.filter_config['min_listing_days']
        cutoff_date = calculation_date - timedelta(days=min_listing_days)
        
        # 转换上市日期格式
        stock_info['list_date_dt'] = pd.to_datetime(stock_info['list_date'], format='%Y%m%d', errors='coerce')
        
        # 筛选上市时间满足条件的股票
        mask = stock_info['list_date_dt'] <= cutoff_date
        filtered_stocks = stock_info[mask].drop(columns=['list_date_dt'])
        
        return filtered_stocks

# ==============================================================================
# 2. 结果整合器
# ==============================================================================

class ResultIntegrator:
    """结果整合器"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
    
    def integrate_results(self, factor_results: Dict[str, Any], 
                         stock_info: pd.DataFrame) -> pd.DataFrame:
        """
        整合因子计算结果和股票基础信息
        
        Args:
            factor_results: 因子计算结果
            stock_info: 股票基础信息
            
        Returns:
            DataFrame: 整合后的完整结果
        """
        logger.info("整合计算结果...")
        
        factor_scores = factor_results.get('factor_scores', pd.DataFrame())
        
        if factor_scores.empty:
            logger.warning("因子计算结果为空")
            return pd.DataFrame()
        
        # 合并股票基础信息
        merged_data = factor_scores.merge(
            stock_info[['ts_code', 'name', 'industry']],
            on='ts_code',
            how='left'
        )
        
        # 重命名列
        if 'name' in merged_data.columns:
            merged_data = merged_data.rename(columns={'name': 'stock_name'})
        
        # 确保必要的列存在
        if 'stock_name' not in merged_data.columns:
            merged_data['stock_name'] = merged_data['ts_code']
        
        if 'industry' not in merged_data.columns:
            merged_data['industry'] = '未知'
        
        # 排序（按最终排名）
        if 'final_rank' in merged_data.columns:
            merged_data = merged_data.sort_values('final_rank')
        elif 'final_score' in merged_data.columns:
            merged_data = merged_data.sort_values('final_score', ascending=False)
            # 重新计算排名
            merged_data['final_rank'] = range(1, len(merged_data) + 1)
        
        logger.info(f"结果整合完成，包含 {len(merged_data)} 只股票")
        return merged_data
    
    def add_statistics_info(self, results: pd.DataFrame) -> Dict[str, Any]:
        """添加统计信息"""
        if results.empty:
            return {"status": "无有效数据"}
        
        stats = {
            "total_stocks": len(results),
            "calculation_date": results['calculation_date'].iloc[0] if 'calculation_date' in results.columns else None,
            "大盘股数量": len(results[results.get('market_cap_group', '') == 'large_cap']),
            "小盘股数量": len(results[results.get('market_cap_group', '') == 'small_cap']),
        }
        
        # 因子完整性统计
        factor_columns = [col for col in results.columns if col not in 
                         ['ts_code', 'stock_name', 'industry', 'calculation_date', 'market_cap_group', 
                          'profitability_score', 'growth_score', 'safety_score', 'final_score', 'final_rank']]
        
        factor_completeness = {}
        for factor in factor_columns:
            if factor in results.columns:
                valid_count = results[factor].notna().sum()
                factor_completeness[factor] = f"{valid_count}/{len(results)} ({valid_count/len(results)*100:.1f}%)"
        
        stats["因子完整性"] = factor_completeness
        
        # 得分分布统计
        if 'final_score' in results.columns:
            final_scores = results['final_score'].dropna()
            if len(final_scores) > 0:
                stats["得分统计"] = {
                    "平均分": final_scores.mean(),
                    "标准差": final_scores.std(),
                    "最高分": final_scores.max(),
                    "最低分": final_scores.min(),
                    "中位数": final_scores.median()
                }
        
        return stats

# ==============================================================================
# 3. 主打分系统类
# ==============================================================================

class StockScoringSystem:
    """股票打分系统主类"""
    
    def __init__(self, data_manager: Optional[DataManager] = None):
        """
        初始化打分系统
        
        Args:
            data_manager: 数据管理器实例，如果为None则自动创建
        """
        # 初始化配置
        initialize_scoring_system()
        
        # 初始化数据管理器
        if data_manager is None:
            self.data_manager = create_data_manager()
        else:
            self.data_manager = data_manager
        
        # 初始化各组件
        self.factor_calculator = create_factor_calculator(self.data_manager)
        self.stock_filter = StockFilter(self.data_manager)
        self.result_integrator = ResultIntegrator(self.data_manager)
        
        # 加载配置
        self.basic_config = get_basic_config()
        self.output_config = get_output_config()
        self.logging_config = get_logging_config()
        
        logger.info("股票打分系统初始化完成")
    
    def calculate_scores(self, calculation_date: Union[str, datetime], 
                        stock_codes: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算股票评分
        
        Args:
            calculation_date: 计算日期
            stock_codes: 指定股票代码列表，如果为None则计算全部股票
            
        Returns:
            Dict: 包含计算结果和统计信息
        """
        # 日期格式处理
        if isinstance(calculation_date, str):
            calculation_date = datetime.strptime(calculation_date, "%Y-%m-%d")
        
        logger.info(f"开始计算股票评分，计算日期: {calculation_date.strftime('%Y-%m-%d')}")
        
        try:
            # 1. 获取股票池
            if stock_codes is None:
                stock_info = self.stock_filter.get_stock_universe(calculation_date)
                if stock_info.empty:
                    return {"error": "未获取到有效股票"}
                stock_codes = stock_info['ts_code'].tolist()
            else:
                stock_info = self.data_manager.get_stock_basic_info(stock_codes)
                if stock_info.empty:
                    return {"error": "指定股票代码无效"}
            
            logger.info(f"待计算股票数量: {len(stock_codes)}")
            
            # 2. 计算因子
            factor_results = self.factor_calculator.calculate_all_factors(
                stock_codes=stock_codes,
                calculation_date=calculation_date
            )
            
            # 3. 整合结果
            final_results = self.result_integrator.integrate_results(
                factor_results, stock_info
            )
            
            if final_results.empty:
                return {"error": "因子计算未产生有效结果"}
            
            # 4. 生成统计信息
            statistics = self.result_integrator.add_statistics_info(final_results)
            
            # 5. 应用输出限制
            if not self.basic_config['output_all_stocks']:
                top_n = self.basic_config['output_top_n']
                final_results = final_results.head(top_n)
                logger.info(f"限制输出前 {top_n} 名股票")
            
            result = {
                "success": True,
                "calculation_date": calculation_date.strftime("%Y-%m-%d"),
                "results": final_results,
                "statistics": statistics,
                "total_calculated": len(final_results)
            }
            
            logger.info(f"✅ 股票评分计算完成，有效结果: {len(final_results)} 只")
            return result
            
        except Exception as e:
            logger.error(f"❌ 股票评分计算失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "calculation_date": calculation_date.strftime("%Y-%m-%d")
            }
    
    def batch_calculate(self, date_list: List[Union[str, datetime]], 
                       stock_codes: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        批量计算多个日期的评分
        
        Args:
            date_list: 日期列表
            stock_codes: 指定股票代码列表
            
        Returns:
            Dict: 批量计算结果
        """
        logger.info(f"开始批量计算，日期数量: {len(date_list)}")
        
        batch_results = {}
        success_count = 0
        
        for date in date_list:
            try:
                result = self.calculate_scores(date, stock_codes)
                batch_results[str(date)] = result
                
                if result.get("success", False):
                    success_count += 1
                    logger.info(f"✅ {date} 计算成功")
                else:
                    logger.warning(f"⚠️ {date} 计算失败: {result.get('error', '未知错误')}")
                    
            except Exception as e:
                logger.error(f"❌ {date} 计算异常: {e}")
                batch_results[str(date)] = {"success": False, "error": str(e)}
        
        summary = {
            "total_dates": len(date_list),
            "success_count": success_count,
            "failure_count": len(date_list) - success_count,
            "success_rate": success_count / len(date_list) if date_list else 0,
            "results": batch_results
        }
        
        logger.info(f"批量计算完成，成功率: {summary['success_rate']:.1%}")
        return summary
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            # 测试数据库连接
            db_status = self.data_manager.db_manager.test_connection()
            
            # 获取股票数量
            stock_info = self.data_manager.get_stock_basic_info()
            stock_count = len(stock_info) if not stock_info.empty else 0
            
            status = {
                "系统状态": "正常" if db_status else "数据库连接异常",
                "数据库连接": "正常" if db_status else "异常",
                "股票池大小": stock_count,
                "配置状态": "已加载",
                "组件状态": {
                    "数据管理器": "正常",
                    "因子计算器": "正常",
                    "股票筛选器": "正常",
                    "结果整合器": "正常"
                }
            }
            
            return status
            
        except Exception as e:
            return {
                "系统状态": "异常",
                "错误信息": str(e)
            }

# ==============================================================================
# 4. 工具函数
# ==============================================================================

def create_scoring_system(data_manager: Optional[DataManager] = None) -> StockScoringSystem:
    """创建打分系统实例的工厂函数"""
    return StockScoringSystem(data_manager)

def quick_score(calculation_date: Union[str, datetime], 
               stock_codes: Optional[List[str]] = None) -> pd.DataFrame:
    """
    快速评分函数
    
    Args:
        calculation_date: 计算日期
        stock_codes: 股票代码列表
        
    Returns:
        DataFrame: 评分结果
    """
    scoring_system = create_scoring_system()
    result = scoring_system.calculate_scores(calculation_date, stock_codes)
    
    if result.get("success", False):
        return result["results"]
    else:
        logger.error(f"快速评分失败: {result.get('error', '未知错误')}")
        return pd.DataFrame()

# ==============================================================================
# 5. 使用示例和测试
# ==============================================================================

if __name__ == "__main__":
    logger.info("=== 股票打分系统测试 ===")
    
    try:
        # 创建打分系统
        scoring_system = create_scoring_system()
        
        # 检查系统状态
        logger.info("检查系统状态...")
        status = scoring_system.get_system_status()
        print("系统状态:", status)
        
        if status.get("数据库连接") != "正常":
            logger.error("数据库连接异常，无法继续测试")
            exit(1)
        
        # 测试单日评分
        test_date = "2022-05-01"
        logger.info(f"测试日期: {test_date}")
        
        # 先用少量股票测试
        stock_info = scoring_system.data_manager.get_stock_basic_info()
        if not stock_info.empty:
            test_stocks = stock_info['ts_code'].head(10).tolist()
            logger.info(f"使用测试股票: {len(test_stocks)} 只")
            
            result = scoring_system.calculate_scores(test_date, test_stocks)
            
            if result.get("success", False):
                print(f"\n✅ 评分计算成功!")
                print(f"计算日期: {result['calculation_date']}")
                print(f"有效结果: {result['total_calculated']} 只股票")
                
                # 显示统计信息
                stats = result.get("statistics", {})
                print(f"大盘股: {stats.get('大盘股数量', 0)} 只")
                print(f"小盘股: {stats.get('小盘股数量', 0)} 只")
                
                # 显示前5名结果
                results_df = result["results"]
                if not results_df.empty:
                    print(f"\n前5名股票:")
                    top5_columns = ['ts_code', 'stock_name', 'final_score', 'final_rank']
                    available_columns = [col for col in top5_columns if col in results_df.columns]
                    print(results_df[available_columns].head())
                
            else:
                print(f"❌ 评分计算失败: {result.get('error', '未知错误')}")
        else:
            logger.error("未获取到股票基础信息")
        
        print("\n=== 测试完成 ===")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise
    
    finally:
        # 清理资源
        if 'scoring_system' in locals():
            scoring_system.data_manager.stop_scheduler()
            logger.info("资源清理完成")

# ==============================================================================
# 6. 导出接口
# ==============================================================================

__all__ = [
    'StockScoringSystem',
    'StockFilter',
    'ResultIntegrator', 
    'create_scoring_system',
    'quick_score'
]