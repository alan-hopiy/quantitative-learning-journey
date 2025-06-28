# -*- coding: utf-8 -*-
"""
因子计算器
实现15个基本面因子的计算逻辑和多层级Z值标准化

项目：基本面量化回测系统
作者：量化团队
创建时间：2025-06-27
"""

import numpy as np
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from pathlib import Path
import warnings
from scipy import stats
warnings.filterwarnings('ignore')

# 导入配置文件
try:
    from config.factor_config import (
        factor_config_manager, FactorConfig, DimensionConfig,
        get_profitability_factors, get_growth_factors, get_safety_factors,
        get_standardization_config, get_stock_filter_config,
        get_dynamic_profitability_window, get_dynamic_growth_periods,
        get_beta_config, get_missing_thresholds
    )
    from config.data_config import (
        PROFITABILITY_DATA_SOURCES, GROWTH_DATA_SOURCES, SAFETY_DATA_SOURCES,
        MARKET_CAP_DATA_SOURCES
    )
    from src.core.data_manager import DataManager
except ImportError as e:
    print(f"警告：无法导入配置模块: {e}")

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================================
# 1. 数据预处理器
# ==============================================================================

class DataPreprocessor:
    """数据预处理器"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        
    def get_quarterly_data(self, stock_codes: List[str], start_date: str, 
                          end_date: str, indicators: List[str]) -> pd.DataFrame:
        """获取季度财务数据并预处理 - 支持多表查询"""
        logger.info(f"获取 {len(stock_codes)} 只股票的季度数据，指标: {indicators}")
        
        # 分类字段：哪些在财务指标表，哪些在资产负债表
        financial_fields = []
        balance_fields = []
        
        # 已知的字段分布
        known_financial_fields = {
            'q_roe', 'roa_yearly', 'grossprofit_margin', 'fcff', 'gross_margin', 
            'daa', 'working_capital', 'debt_to_assets', 'retained_earnings', 
            'ebit', 'op_income'
        }
        known_balance_fields = {'total_assets'}
        
        for indicator in indicators:
            if indicator in known_financial_fields:
                financial_fields.append(indicator)
            elif indicator in known_balance_fields:
                balance_fields.append(indicator)
            else:
                # 默认假设在财务指标表中
                financial_fields.append(indicator)
                logger.debug(f"未知字段 {indicator}，假设在财务指标表中")
        
        # 获取财务指标数据
        financial_data = pd.DataFrame()
        if financial_fields:
            try:
                financial_data = self.data_manager.get_financial_data(
                    stock_codes=stock_codes,
                    start_date=start_date,
                    end_date=end_date,
                    indicators=financial_fields
                )
                if not financial_data.empty:
                    logger.info(f"从财务指标表获取到 {len(financial_data)} 行数据")
            except Exception as e:
                logger.warning(f"获取财务指标数据失败: {e}")
        
        # 获取资产负债表数据
        balance_data = pd.DataFrame()
        if balance_fields:
            try:
                balance_data = self._get_balance_sheet_data(
                    stock_codes=stock_codes,
                    start_date=start_date,
                    end_date=end_date,
                    fields=balance_fields
                )
                if not balance_data.empty:
                    logger.info(f"从资产负债表获取到 {len(balance_data)} 行数据")
            except Exception as e:
                logger.warning(f"获取资产负债表数据失败: {e}")
        
        # 合并数据
        if financial_data.empty and balance_data.empty:
            logger.warning("未获取到任何财务数据")
            return pd.DataFrame()
        elif financial_data.empty:
            merged_data = balance_data
        elif balance_data.empty:
            merged_data = financial_data
        else:
            # 按股票代码和日期合并
            merged_data = financial_data.merge(
                balance_data, 
                on=['ts_code', 'end_date'], 
                how='outer'
            )
        
        if merged_data.empty:
            logger.warning("合并后数据为空")
            return pd.DataFrame()
        
        # 数据清洗和预处理
        cleaned_data = self._clean_financial_data(merged_data, indicators)
        cleaned_data['end_date'] = pd.to_datetime(cleaned_data['end_date'], format='%Y%m%d', errors='coerce')
        cleaned_data = cleaned_data.sort_values(['ts_code', 'end_date'])
        
        logger.info(f"预处理完成，最终数据量: {len(cleaned_data)} 行")
        return cleaned_data
    
    def _get_balance_sheet_data(self, stock_codes: List[str], start_date: str, 
                               end_date: str, fields: List[str]) -> pd.DataFrame:
        """获取资产负债表数据"""
        if not fields:
            return pd.DataFrame()
        
        # 构建查询
        codes_str = "'" + "','".join(stock_codes) + "'"
        fields_with_base = ['ts_code', 'end_date'] + fields
        fields_str = ','.join(fields_with_base)
        
        query = f"""
        SELECT {fields_str}
        FROM stock_balance_sheets
        WHERE ts_code IN ({codes_str})
        AND end_date >= '{start_date.replace('-', '')}'
        AND end_date <= '{end_date.replace('-', '')}'
        ORDER BY ts_code, end_date
        """
        
        try:
            result = self.data_manager.db_manager.execute_query(query)
            logger.debug(f"资产负债表查询成功，获取 {len(result)} 行数据")
            return result
        except Exception as e:
            logger.error(f"资产负债表查询失败: {e}")
            return pd.DataFrame()
    
    def get_daily_data(self, stock_codes: List[str], start_date: str, 
                      end_date: str, fields: List[str]) -> pd.DataFrame:
        """获取日度市场数据并预处理"""
        logger.info(f"获取 {len(stock_codes)} 只股票的日度数据，字段: {fields}")
        
        raw_data = self.data_manager.get_market_data(
            stock_codes=stock_codes,
            start_date=start_date,
            end_date=end_date,
            fields=fields
        )
        
        if raw_data.empty:
            logger.warning("未获取到市场数据")
            return pd.DataFrame()
        
        raw_data['trade_date'] = pd.to_datetime(raw_data['trade_date'], format='%Y%m%d')
        raw_data = raw_data.sort_values(['ts_code', 'trade_date'])
        
        logger.info(f"日度数据处理完成，数据量: {len(raw_data)} 行")
        return raw_data
    
    def _clean_financial_data(self, data: pd.DataFrame, indicators: List[str]) -> pd.DataFrame:
        """清洗财务数据"""
        data = data.drop_duplicates(subset=['ts_code', 'end_date'], keep='last')
        
        for indicator in indicators:
            if indicator in data.columns:
                data[indicator] = self._winsorize_series(data[indicator])
        
        return data
    
    def _winsorize_series(self, series: pd.Series, lower: float = 0.01, 
                         upper: float = 0.99) -> pd.Series:
        """Winsorize处理异常值"""
        if series.isnull().all():
            return series
        
        lower_bound = series.quantile(lower)
        upper_bound = series.quantile(upper)
        
        return series.clip(lower=lower_bound, upper=upper_bound)

# ==============================================================================
# 2. 盈利能力因子计算器
# ==============================================================================

class ProfitabilityCalculator:
    """盈利能力因子计算器"""
    
    def __init__(self, data_preprocessor: DataPreprocessor):
        self.data_preprocessor = data_preprocessor
        self.factor_configs = get_profitability_factors()
        
    def calculate_factors(self, stock_codes: List[str], calculation_date: datetime) -> pd.DataFrame:
        """计算所有盈利能力因子"""
        logger.info(f"开始计算盈利能力因子，计算日期: {calculation_date}")
        
        year = calculation_date.year
        quarters = get_dynamic_profitability_window(year)
        start_date = calculation_date - timedelta(days=quarters * 90)
        
        indicators = ['q_roe', 'roa_yearly', 'grossprofit_margin', 'fcff', 
                     'gross_margin', 'daa', 'working_capital', 'total_assets']
        
        financial_data = self.data_preprocessor.get_quarterly_data(
            stock_codes=stock_codes,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=calculation_date.strftime("%Y-%m-%d"),
            indicators=indicators
        )
        
        if financial_data.empty:
            logger.warning("未获取到财务数据，返回空结果")
            return pd.DataFrame()
        
        result_data = []
        
        for stock_code in stock_codes:
            stock_data = financial_data[financial_data['ts_code'] == stock_code].copy()
            
            if len(stock_data) < 2:
                continue
                
            stock_data = stock_data.tail(quarters)
            
            factor_values = {}
            factor_values['ts_code'] = stock_code
            factor_values['calculation_date'] = calculation_date
            
            # 因子1: ROE
            if 'q_roe' in stock_data.columns and stock_data['q_roe'].notna().sum() >= 2:
                factor_values['ROE'] = stock_data['q_roe'].mean()
            
            # 因子2: ROA
            if 'roa_yearly' in stock_data.columns and stock_data['roa_yearly'].notna().sum() >= 2:
                factor_values['ROA'] = stock_data['roa_yearly'].mean()
            
            # 因子3: 毛利率
            if 'grossprofit_margin' in stock_data.columns and stock_data['grossprofit_margin'].notna().sum() >= 2:
                factor_values['毛利率'] = stock_data['grossprofit_margin'].mean()
            
            # 因子4: 自由现金流/资产
            if all(col in stock_data.columns for col in ['fcff', 'total_assets']):
                fcff_asset_ratio = stock_data['fcff'] / stock_data['total_assets']
                if fcff_asset_ratio.notna().sum() >= 2:
                    factor_values['自由现金流资产比'] = fcff_asset_ratio.mean()
            
            # 因子5: 毛利润/资产
            if all(col in stock_data.columns for col in ['gross_margin', 'total_assets']):
                gross_asset_ratio = stock_data['gross_margin'] / stock_data['total_assets']
                if gross_asset_ratio.notna().sum() >= 2:
                    factor_values['毛利润资产比'] = gross_asset_ratio.mean()
            
            # 因子6: 现金流质量
            if all(col in stock_data.columns for col in ['daa', 'working_capital', 'total_assets']):
                stock_data_sorted = stock_data.sort_values('end_date')
                wc_change = stock_data_sorted['working_capital'].diff()
                cash_quality = (stock_data_sorted['daa'] - wc_change) / stock_data_sorted['total_assets']
                if cash_quality.notna().sum() >= 2:
                    factor_values['现金流质量'] = cash_quality.mean()
            
            result_data.append(factor_values)
        
        result_df = pd.DataFrame(result_data)
        logger.info(f"盈利能力因子计算完成，有效股票数: {len(result_df)}")
        
        return result_df

# ==============================================================================
# 3. 成长能力因子计算器
# ==============================================================================

class GrowthCalculator:
    """成长能力因子计算器"""
    
    def __init__(self, data_preprocessor: DataPreprocessor):
        self.data_preprocessor = data_preprocessor
        self.factor_configs = get_growth_factors()
        
    def calculate_factors(self, stock_codes: List[str], calculation_date: datetime) -> pd.DataFrame:
        """计算所有成长能力因子"""
        logger.info(f"开始计算成长能力因子，计算日期: {calculation_date}")
        
        year = calculation_date.year
        growth_periods = get_dynamic_growth_periods(year)
        max_period = max(growth_periods)
        start_date = calculation_date - timedelta(days=(max_period + 1) * 365)
        
        indicators = ['q_roe', 'roa_yearly', 'grossprofit_margin', 'fcff', 
                     'gross_margin', 'total_assets']
        
        financial_data = self.data_preprocessor.get_quarterly_data(
            stock_codes=stock_codes,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=calculation_date.strftime("%Y-%m-%d"),
            indicators=indicators
        )
        
        if financial_data.empty:
            logger.warning("未获取到财务数据，返回空结果")
            return pd.DataFrame()
        
        result_data = []
        
        for stock_code in stock_codes:
            stock_data = financial_data[financial_data['ts_code'] == stock_code].copy()
            
            if len(stock_data) < 8:
                continue
            
            stock_data = stock_data.sort_values('end_date')
            
            factor_values = {}
            factor_values['ts_code'] = stock_code
            factor_values['calculation_date'] = calculation_date
            
            growth_rates = {}
            
            for period in growth_periods:
                period_growth = self._calculate_period_growth(stock_data, period, calculation_date)
                for factor_name, growth_rate in period_growth.items():
                    if factor_name not in growth_rates:
                        growth_rates[factor_name] = []
                    growth_rates[factor_name].append(growth_rate)
            
            for factor_name, rates in growth_rates.items():
                if len(rates) > 0 and not all(pd.isna(rates)):
                    rates_array = np.array(rates)
                    valid_rates = rates_array[~np.isnan(rates_array)]
                    
                    if len(valid_rates) > 0:
                        if len(valid_rates) == 1:
                            factor_values[f"{factor_name}成长性"] = valid_rates[0]
                        else:
                            mean_rate = np.mean(valid_rates)
                            std_rate = np.std(valid_rates, ddof=1) if len(valid_rates) > 1 else 1
                            if std_rate > 0:
                                factor_values[f"{factor_name}成长性"] = (mean_rate - np.mean(valid_rates)) / std_rate
                            else:
                                factor_values[f"{factor_name}成长性"] = mean_rate
            
            result_data.append(factor_values)
        
        result_df = pd.DataFrame(result_data)
        logger.info(f"成长能力因子计算完成，有效股票数: {len(result_df)}")
        
        return result_df
    
    def _calculate_period_growth(self, stock_data: pd.DataFrame, years_back: int, 
                               calculation_date: datetime) -> Dict[str, float]:
        """计算指定期间的增长率"""
        growth_rates = {}
        
        base_date = calculation_date - timedelta(days=years_back * 365)
        recent_data = stock_data[stock_data['end_date'] >= calculation_date - timedelta(days=365)]
        base_data = stock_data[
            (stock_data['end_date'] >= base_date - timedelta(days=365)) &
            (stock_data['end_date'] <= base_date)
        ]
        
        if len(recent_data) < 2 or len(base_data) < 2:
            return growth_rates
        
        factors_to_calculate = {
            'ROE': 'q_roe',
            'ROA': 'roa_yearly',
            '毛利率': 'grossprofit_margin',
            '自由现金流资产比': ['fcff', 'total_assets'],
            '毛利润资产比': ['gross_margin', 'total_assets']
        }
        
        for factor_name, columns in factors_to_calculate.items():
            try:
                if isinstance(columns, str):
                    recent_value = recent_data[columns].mean()
                    base_value = base_data[columns].mean()
                else:
                    recent_ratio = (recent_data[columns[0]] / recent_data[columns[1]]).mean()
                    base_ratio = (base_data[columns[0]] / base_data[columns[1]]).mean()
                    recent_value = recent_ratio
                    base_value = base_ratio
                
                if pd.notna(recent_value) and pd.notna(base_value) and base_value != 0:
                    growth_rate = (recent_value - base_value) / abs(base_value)
                    growth_rates[factor_name] = growth_rate
                    
            except Exception as e:
                logger.debug(f"计算 {factor_name} 增长率失败: {e}")
                continue
        
        return growth_rates

# ==============================================================================
# 4. 安全性因子计算器
# ==============================================================================

class SafetyCalculator:
    """安全性因子计算器"""
    
    def __init__(self, data_preprocessor: DataPreprocessor):
        self.data_preprocessor = data_preprocessor
        self.factor_configs = get_safety_factors()
        self.beta_config = get_beta_config()
        
    def calculate_factors(self, stock_codes: List[str], calculation_date: datetime) -> pd.DataFrame:
        """计算所有安全性因子"""
        logger.info(f"开始计算安全性因子，计算日期: {calculation_date}")
        
        market_data = self._get_market_data_for_risk(stock_codes, calculation_date)
        financial_data = self._get_financial_data_for_safety(stock_codes, calculation_date)
        market_value_data = self._get_market_value_data(stock_codes, calculation_date)
        
        result_data = []
        
        for stock_code in stock_codes:
            factor_values = {}
            factor_values['ts_code'] = stock_code
            factor_values['calculation_date'] = calculation_date
            
            risk_factors = self._calculate_market_risk_factors(stock_code, market_data)
            factor_values.update(risk_factors)
            
            debt_ratio = self._calculate_debt_ratio(stock_code, financial_data)
            if debt_ratio is not None:
                factor_values['低负债率'] = debt_ratio
            
            altman_z = self._calculate_altman_z(stock_code, financial_data, market_value_data)
            if altman_z is not None:
                factor_values['奥特曼Z值'] = altman_z
            
            if len(factor_values) > 2:
                result_data.append(factor_values)
        
        result_df = pd.DataFrame(result_data)
        logger.info(f"安全性因子计算完成，有效股票数: {len(result_df)}")
        
        return result_df
    
    def _get_market_data_for_risk(self, stock_codes: List[str], calculation_date: datetime) -> pd.DataFrame:
        """获取计算市场风险因子所需的日度数据"""
        start_date = calculation_date - timedelta(days=self.beta_config['time_window_days'])
        all_codes = stock_codes + [self.beta_config['benchmark_code']]
        
        market_data = self.data_preprocessor.get_daily_data(
            stock_codes=all_codes,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=calculation_date.strftime("%Y-%m-%d"),
            fields=['close', 'pct_chg']
        )
        
        return market_data
    
    def _get_financial_data_for_safety(self, stock_codes: List[str], calculation_date: datetime) -> pd.DataFrame:
        """获取计算安全性因子所需的财务数据"""
        start_date = calculation_date - timedelta(days=365)
        
        indicators = ['debt_to_assets', 'working_capital', 'retained_earnings', 
                     'ebit', 'total_revenue', 'total_assets']
        
        financial_data = self.data_preprocessor.get_quarterly_data(
            stock_codes=stock_codes,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=calculation_date.strftime("%Y-%m-%d"),
            indicators=indicators
        )
        
        return financial_data
    
    def _get_market_value_data(self, stock_codes: List[str], calculation_date: datetime) -> pd.DataFrame:
        """获取市值数据"""
        start_date = calculation_date - timedelta(days=30)
        
        market_value_data = self.data_preprocessor.get_daily_data(
            stock_codes=stock_codes,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=calculation_date.strftime("%Y-%m-%d"),
            fields=['total_mv']
        )
        
        return market_value_data
    
    def _calculate_market_risk_factors(self, stock_code: str, market_data: pd.DataFrame) -> Dict[str, float]:
        """计算市场风险因子（Beta和个股风险）"""
        risk_factors = {}
        
        stock_returns = market_data[market_data['ts_code'] == stock_code]['pct_chg'].values
        benchmark_returns = market_data[
            market_data['ts_code'] == self.beta_config['benchmark_code']
        ]['pct_chg'].values
        
        if len(stock_returns) < 50 or len(benchmark_returns) < 50:
            return risk_factors
        
        min_length = min(len(stock_returns), len(benchmark_returns))
        stock_returns = stock_returns[-min_length:]
        benchmark_returns = benchmark_returns[-min_length:]
        
        valid_idx = ~(np.isnan(stock_returns) | np.isnan(benchmark_returns))
        stock_returns = stock_returns[valid_idx]
        benchmark_returns = benchmark_returns[valid_idx]
        
        if len(stock_returns) < 30:
            return risk_factors
        
        try:
            covariance = np.cov(stock_returns, benchmark_returns)[0, 1]
            market_variance = np.var(benchmark_returns, ddof=1)
            
            if market_variance > 0:
                beta = covariance / market_variance
                risk_factors['低Beta'] = -beta
            
            alpha = np.mean(stock_returns) - beta * np.mean(benchmark_returns)
            predicted_returns = alpha + beta * benchmark_returns
            residuals = stock_returns - predicted_returns
            idiosyncratic_risk = np.std(residuals, ddof=1) * np.sqrt(252)
            
            risk_factors['低个股风险'] = -idiosyncratic_risk
            
        except Exception as e:
            logger.debug(f"计算 {stock_code} 市场风险因子失败: {e}")
        
        return risk_factors
    
    def _calculate_debt_ratio(self, stock_code: str, financial_data: pd.DataFrame) -> Optional[float]:
        """计算资产负债率"""
        stock_data = financial_data[financial_data['ts_code'] == stock_code]
        
        if stock_data.empty or 'debt_to_assets' not in stock_data.columns:
            return None
        
        recent_data = stock_data.tail(4)
        debt_ratio = recent_data['debt_to_assets'].mean()
        
        return debt_ratio if pd.notna(debt_ratio) else None
    
    def _calculate_altman_z(self, stock_code: str, financial_data: pd.DataFrame, 
                          market_value_data: pd.DataFrame) -> Optional[float]:
        """计算奥特曼Z值"""
        stock_financial = financial_data[financial_data['ts_code'] == stock_code]
        stock_market = market_value_data[market_value_data['ts_code'] == stock_code]
        
        if stock_financial.empty:
            return None
        
        try:
            latest_financial = stock_financial.tail(1).iloc[0]
            
            working_capital = latest_financial.get('working_capital', np.nan)
            retained_earnings = latest_financial.get('retained_earnings', np.nan)
            ebit = latest_financial.get('ebit', np.nan)
            total_revenue = latest_financial.get('total_revenue', np.nan)
            total_assets = latest_financial.get('total_assets', np.nan)
            
            if not stock_market.empty:
                market_value = stock_market.tail(1).iloc[0].get('total_mv', np.nan)
            else:
                market_value = np.nan
            
            total_debt = total_assets * 0.4
            
            if any(pd.isna(x) or x == 0 for x in [total_assets, total_debt]):
                return None
            
            z1 = 1.2 * (working_capital / total_assets) if pd.notna(working_capital) else 0
            z2 = 1.4 * (retained_earnings / total_assets) if pd.notna(retained_earnings) else 0
            z3 = 3.3 * (ebit / total_assets) if pd.notna(ebit) else 0
            z4 = 0.6 * (market_value / total_debt) if pd.notna(market_value) and total_debt > 0 else 0
            z5 = 1.0 * (total_revenue / total_assets) if pd.notna(total_revenue) else 0
            
            altman_z = z1 + z2 + z3 + z4 + z5
            
            return altman_z
            
        except Exception as e:
            logger.debug(f"计算 {stock_code} 奥特曼Z值失败: {e}")
            return None

# ==============================================================================
# 5. 标准化处理器
# ==============================================================================

class FactorStandardizer:
    """因子标准化处理器"""
    
    def __init__(self):
        self.standardization_config = get_standardization_config()
        
    def standardize_factors(self, factor_data: pd.DataFrame, 
                          market_cap_data: pd.DataFrame) -> pd.DataFrame:
        """对因子进行多层级Z值标准化"""
        logger.info("开始因子标准化处理")
        
        if factor_data.empty:
            return pd.DataFrame()
        
        merged_data = factor_data.merge(
            market_cap_data[['ts_code', 'market_cap_group']], 
            on='ts_code', 
            how='left'
        )
        
        standardized_data = self._standardize_by_market_cap_group(merged_data)
        dimension_scores = self._calculate_dimension_scores(standardized_data)
        final_scores = self._calculate_final_scores(dimension_scores)
        
        logger.info("因子标准化处理完成")
        return final_scores
    
    def _standardize_by_market_cap_group(self, data: pd.DataFrame) -> pd.DataFrame:
        """按市值组进行因子标准化"""
        standardized_data = data.copy()
        
        exclude_cols = ['ts_code', 'calculation_date', 'market_cap_group']
        factor_cols = [col for col in data.columns if col not in exclude_cols]
        
        for market_cap_group in ['large_cap', 'small_cap']:
            group_mask = data['market_cap_group'] == market_cap_group
            group_data = data[group_mask]
            
            if len(group_data) < 5:
                continue
            
            for factor_col in factor_cols:
                if factor_col in group_data.columns:
                    factor_values = group_data[factor_col].dropna()
                    
                    if len(factor_values) >= 5:
                        mean_val = factor_values.mean()
                        std_val = factor_values.std(ddof=1)
                        
                        if std_val > 0:
                            standardized_values = (factor_values - mean_val) / std_val
                            standardized_data.loc[group_mask, factor_col] = standardized_values
        
        return standardized_data
    
    def _calculate_dimension_scores(self, standardized_data: pd.DataFrame) -> pd.DataFrame:
        """计算维度得分"""
        dimension_scores = standardized_data[['ts_code', 'calculation_date', 'market_cap_group']].copy()
        
        missing_thresholds = get_missing_thresholds()
        
        profitability_factors = ['ROE', 'ROA', '毛利率', '自由现金流资产比', '毛利润资产比', '现金流质量']
        profitability_score = self._calculate_single_dimension_score(
            standardized_data, profitability_factors, 
            missing_thresholds['profitability_min_factors']
        )
        dimension_scores['profitability_score'] = profitability_score
        
        growth_factors = ['ROE成长性', 'ROA成长性', '毛利率成长性', '自由现金流资产比成长性', '毛利润资产比成长性']
        growth_score = self._calculate_single_dimension_score(
            standardized_data, growth_factors,
            missing_thresholds['growth_min_factors']
        )
        dimension_scores['growth_score'] = growth_score
        
        safety_factors = ['低Beta', '低个股风险', '低负债率', '奥特曼Z值']
        safety_score = self._calculate_single_dimension_score(
            standardized_data, safety_factors,
            missing_thresholds['safety_min_factors']
        )
        dimension_scores['safety_score'] = safety_score
        
        return dimension_scores
    
    def _calculate_single_dimension_score(self, data: pd.DataFrame, factor_list: List[str], 
                                        min_factors: int) -> pd.Series:
        """计算单个维度的得分"""
        scores = pd.Series(index=data.index, dtype=float)
        
        for idx, row in data.iterrows():
            factor_values = []
            for factor in factor_list:
                if factor in data.columns and pd.notna(row[factor]):
                    factor_values.append(row[factor])
            
            if len(factor_values) >= min_factors:
                dimension_score = np.mean(factor_values)
                scores.iloc[idx] = dimension_score
            else:
                scores.iloc[idx] = np.nan
        
        return scores
    
    def _calculate_final_scores(self, dimension_scores: pd.DataFrame) -> pd.DataFrame:
        """计算最终综合得分"""
        final_data = dimension_scores.copy()
        
        missing_thresholds = get_missing_thresholds()
        max_missing_dimensions = missing_thresholds['max_missing_dimensions']
        
        dimension_cols = ['profitability_score', 'growth_score', 'safety_score']
        
        for market_cap_group in ['large_cap', 'small_cap']:
            group_mask = dimension_scores['market_cap_group'] == market_cap_group
            group_data = dimension_scores[group_mask]
            
            if len(group_data) < 5:
                continue
            
            for dim_col in dimension_cols:
                dim_values = group_data[dim_col].dropna()
                
                if len(dim_values) >= 5:
                    mean_val = dim_values.mean()
                    std_val = dim_values.std(ddof=1)
                    
                    if std_val > 0:
                        standardized_values = (dim_values - mean_val) / std_val
                        final_data.loc[group_mask, dim_col] = standardized_values
        
        final_scores = []
        final_ranks = []
        
        for market_cap_group in ['large_cap', 'small_cap']:
            group_mask = final_data['market_cap_group'] == market_cap_group
            group_data = final_data[group_mask]
            
            group_final_scores = []
            valid_indices = []
            
            for idx, row in group_data.iterrows():
                valid_dimensions = []
                for dim_col in dimension_cols:
                    if pd.notna(row[dim_col]):
                        valid_dimensions.append(row[dim_col])
                
                missing_dimensions = len(dimension_cols) - len(valid_dimensions)
                
                if missing_dimensions <= max_missing_dimensions and len(valid_dimensions) > 0:
                    final_score = np.mean(valid_dimensions)
                    group_final_scores.append(final_score)
                    valid_indices.append(idx)
            
            if len(group_final_scores) >= 5:
                mean_final = np.mean(group_final_scores)
                std_final = np.std(group_final_scores, ddof=1)
                
                if std_final > 0:
                    standardized_final_scores = [(score - mean_final) / std_final for score in group_final_scores]
                else:
                    standardized_final_scores = group_final_scores
                
                ranks = stats.rankdata([-score for score in standardized_final_scores], method='ordinal')
                
                for i, idx in enumerate(valid_indices):
                    final_scores.append(standardized_final_scores[i])
                    final_ranks.append(ranks[i])
            else:
                for i, idx in enumerate(valid_indices):
                    final_scores.append(group_final_scores[i] if group_final_scores else np.nan)
                    final_ranks.append(i + 1 if group_final_scores else np.nan)
        
        # 补齐缺失的股票
        for idx in final_data.index:
            if len(final_scores) <= idx:
                final_scores.append(np.nan)
                final_ranks.append(np.nan)
        
        final_data['final_score'] = final_scores[:len(final_data)]
        final_data['final_rank'] = final_ranks[:len(final_data)]
        
        return final_data

# ==============================================================================
# 6. 主因子计算器
# ==============================================================================

class FactorCalculator:
    """主因子计算器"""
    
    def __init__(self, data_manager: DataManager):
        self.data_manager = data_manager
        self.data_preprocessor = DataPreprocessor(data_manager)
        self.profitability_calculator = ProfitabilityCalculator(self.data_preprocessor)
        self.growth_calculator = GrowthCalculator(self.data_preprocessor)
        self.safety_calculator = SafetyCalculator(self.data_preprocessor)
        self.standardizer = FactorStandardizer()
        
    def calculate_all_factors(self, stock_codes: List[str], 
                            calculation_date: datetime) -> Dict[str, Any]:
        """计算所有因子并进行标准化"""
        logger.info(f"开始计算所有因子，计算日期: {calculation_date}, 股票数量: {len(stock_codes)}")
        
        try:
            market_cap_data = self._get_market_cap_classification(stock_codes, calculation_date)
            
            profitability_factors = self.profitability_calculator.calculate_factors(
                stock_codes, calculation_date
            )
            
            growth_factors = self.growth_calculator.calculate_factors(
                stock_codes, calculation_date
            )
            
            safety_factors = self.safety_calculator.calculate_factors(
                stock_codes, calculation_date
            )
            
            all_factors = self._merge_factor_data(
                profitability_factors, growth_factors, safety_factors
            )
            
            if all_factors.empty:
                logger.warning("未计算出任何有效因子")
                return {
                    "calculation_date": calculation_date.strftime("%Y-%m-%d"),
                    "factor_scores": pd.DataFrame()
                }
            
            standardized_factors = self.standardizer.standardize_factors(
                all_factors, market_cap_data
            )
            
            result = {
                "calculation_date": calculation_date.strftime("%Y-%m-%d"),
                "factor_scores": standardized_factors
            }
            
            logger.info(f"因子计算完成，有效股票数: {len(standardized_factors)}")
            return result
            
        except Exception as e:
            logger.error(f"因子计算失败: {e}")
            raise
    
    def _get_market_cap_classification(self, stock_codes: List[str], 
                                     calculation_date: datetime) -> pd.DataFrame:
        """获取股票市值分类"""
        start_date = calculation_date - timedelta(days=30)
        
        market_data = self.data_preprocessor.get_daily_data(
            stock_codes=stock_codes,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=calculation_date.strftime("%Y-%m-%d"),
            fields=['total_mv']
        )
        
        if market_data.empty:
            logger.warning("未获取到市值数据，使用默认分类")
            return pd.DataFrame({
                'ts_code': stock_codes,
                'market_cap_group': ['large_cap'] * len(stock_codes)
            })
        
        latest_market_cap = market_data.groupby('ts_code')['total_mv'].last().reset_index()
        
        market_cap_config = factor_config_manager.get_global_config('market_cap')
        cutoff = market_cap_config['large_small_cutoff']
        
        latest_market_cap['market_cap_group'] = latest_market_cap['total_mv'].apply(
            lambda x: 'large_cap' if x >= cutoff * 10000 else 'small_cap'
        )
        
        return latest_market_cap[['ts_code', 'market_cap_group']]
    
    def _merge_factor_data(self, profitability_data: pd.DataFrame, 
                          growth_data: pd.DataFrame, 
                          safety_data: pd.DataFrame) -> pd.DataFrame:
        """合并各维度的因子数据"""
        if profitability_data.empty:
            base_data = pd.DataFrame()
        else:
            base_data = profitability_data.copy()
        
        if not growth_data.empty:
            if base_data.empty:
                base_data = growth_data.copy()
            else:
                base_data = base_data.merge(
                    growth_data, 
                    on=['ts_code', 'calculation_date'], 
                    how='outer'
                )
        
        if not safety_data.empty:
            if base_data.empty:
                base_data = safety_data.copy()
            else:
                base_data = base_data.merge(
                    safety_data, 
                    on=['ts_code', 'calculation_date'], 
                    how='outer'
                )
        
        return base_data

# ==============================================================================
# 7. 工具函数和接口
# ==============================================================================

def create_factor_calculator(data_manager: DataManager) -> FactorCalculator:
    """创建因子计算器实例的工厂函数"""
    return FactorCalculator(data_manager)

def validate_calculation_date(calculation_date: datetime) -> bool:
    """验证计算日期是否合理"""
    start_date = datetime.strptime(factor_config_manager.get_global_config('start_date'), '%Y-%m-%d')
    
    if calculation_date < start_date:
        logger.error(f"计算日期 {calculation_date} 早于配置的起始日期 {start_date}")
        return False
    
    if calculation_date > datetime.now():
        logger.error(f"计算日期 {calculation_date} 不能超过当前日期")
        return False
    
    return True

def get_calculation_months() -> List[int]:
    """获取调仓月份（用于批量计算）"""
    calculation_config = factor_config_manager.get_global_config('calculation_frequency')
    return calculation_config['rebalance_months']

# ==============================================================================
# 8. 使用示例
# ==============================================================================

if __name__ == "__main__":
    logger.info("=== 因子计算器测试 ===")
    
    try:
        from src.core.data_manager import create_data_manager
        
        dm = create_data_manager()
        calculator = create_factor_calculator(dm)
        
        logger.info("获取股票基础信息...")
        basic_info = dm.get_stock_basic_info()
        
        if basic_info.empty:
            logger.error("未获取到股票基础信息")
            exit(1)
        
        test_stocks = basic_info['ts_code'].head(20).tolist()
        logger.info(f"选择测试股票: {len(test_stocks)} 只")
        
        calculation_date = datetime(2022, 5, 1)
        
        if not validate_calculation_date(calculation_date):
            logger.error("计算日期验证失败")
            exit(1)
        
        logger.info(f"开始计算因子，日期: {calculation_date}")
        results = calculator.calculate_all_factors(test_stocks, calculation_date)
        
        factor_scores = results['factor_scores']
        if not factor_scores.empty:
            logger.info(f"✅ 因子计算成功")
            logger.info(f"- 有效股票数: {len(factor_scores)}")
            logger.info(f"- 数据列数: {len(factor_scores.columns)}")
            
            print("\n📊 计算结果统计:")
            print(f"大盘股数量: {len(factor_scores[factor_scores['market_cap_group'] == 'large_cap'])}")
            print(f"小盘股数量: {len(factor_scores[factor_scores['market_cap_group'] == 'small_cap'])}")
            
            if 'final_score' in factor_scores.columns:
                final_scores = factor_scores['final_score'].dropna()
                print(f"最终得分范围: {final_scores.min():.3f} ~ {final_scores.max():.3f}")
                print(f"最终得分均值: {final_scores.mean():.3f}")
        else:
            logger.warning("⚠️ 未计算出有效因子")
        
        print("\n=== 测试完成 ===")
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise
    
    finally:
        if 'dm' in locals():
            dm.stop_scheduler()
            logger.info("资源清理完成")

# ==============================================================================
# 9. 导出接口
# ==============================================================================

__all__ = [
    'FactorCalculator',
    'DataPreprocessor',
    'ProfitabilityCalculator',
    'GrowthCalculator', 
    'SafetyCalculator',
    'FactorStandardizer',
    'create_factor_calculator',
    'validate_calculation_date',
    'get_calculation_months'
]