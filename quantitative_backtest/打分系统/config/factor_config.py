# -*- coding: utf-8 -*-
"""
因子计算配置文件
定义基本面因子的计算逻辑、参数和权重配置

项目：基本面量化回测系统
作者：量化团队
创建时间：2025-06-26
"""

from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum

# ==============================================================================
# 1. 基础枚举和数据类定义
# ==============================================================================

class FactorDirection(Enum):
    """因子方向定义"""
    POSITIVE = 1    # 因子值越大越好
    NEGATIVE = -1   # 因子值越小越好

@dataclass
class FactorConfig:
    """单个因子配置"""
    name: str                           # 因子名称
    description: str                    # 因子描述
    calculation_method: str             # 计算方法描述
    outlier_rule: Dict[str, Any]       # 异常值处理规则
    direction: FactorDirection          # 因子方向

@dataclass 
class DimensionConfig:
    """维度配置"""
    name: str                           # 维度名称
    description: str                    # 维度描述
    factors: List[FactorConfig]         # 包含的因子列表
    weight: float                       # 维度权重
    factor_weights: List[float]         # 维度内各因子权重

# ==============================================================================
# 2. 盈利能力因子配置（6个因子）
# ==============================================================================

PROFITABILITY_FACTORS = [
    FactorConfig(
        name="ROE",
        description="净资产收益率",
        calculation_method="取各季度ROE的平均值",
        outlier_rule={"method": "winsorize", "lower": 0.05, "upper": 0.95},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="ROA", 
        description="总资产收益率",
        calculation_method="取各季度ROA的平均值",
        outlier_rule={"method": "winsorize", "lower": 0.05, "upper": 0.95},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="毛利率",
        description="毛利率",
        calculation_method="取各季度毛利率的平均值", 
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="自由现金流资产比",
        description="自由现金流/总资产",
        calculation_method="每季度先计算fcff/total_assets比率，再取比率的平均值",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="毛利润资产比", 
        description="毛利润/总资产",
        calculation_method="每季度先计算gross_margin/total_assets比率，再取比率的平均值",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="现金流质量",
        description="(折旧-运营资本变化)/总资产", 
        calculation_method="每季度先计算(daa-working_capital变化)/total_assets比率，再取比率的平均值",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    )
]

# ==============================================================================
# 3. 成长能力因子配置（5个因子，复用盈利能力前5个）
# ==============================================================================

GROWTH_FACTORS = [
    FactorConfig(
        name="ROE成长性",
        description="ROE增长率的多期Z值",
        calculation_method="计算最新4季度vs(3年前、4年前、5年前)4季度的增长率，分别标准化后再次标准化",
        outlier_rule={"method": "winsorize", "lower": 0.05, "upper": 0.95},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="ROA成长性", 
        description="ROA增长率的多期Z值",
        calculation_method="计算最新4季度vs(3年前、4年前、5年前)4季度的增长率，分别标准化后再次标准化",
        outlier_rule={"method": "winsorize", "lower": 0.05, "upper": 0.95},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="毛利率成长性",
        description="毛利率增长率的多期Z值", 
        calculation_method="计算最新4季度vs(3年前、4年前、5年前)4季度的增长率，分别标准化后再次标准化",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="自由现金流资产比成长性",
        description="自由现金流/总资产增长率的多期Z值",
        calculation_method="计算最新4季度vs(3年前、4年前、5年前)4季度的增长率，分别标准化后再次标准化", 
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    ),
    
    FactorConfig(
        name="毛利润资产比成长性",
        description="毛利润/总资产增长率的多期Z值",
        calculation_method="计算最新4季度vs(3年前、4年前、5年前)4季度的增长率，分别标准化后再次标准化",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    )
]

# ==============================================================================
# 4. 安全性因子配置（4个因子）  
# ==============================================================================

SAFETY_FACTORS = [
    FactorConfig(
        name="低Beta",
        description="低市场风险（负Beta值）",
        calculation_method="通过股票收益率vs市场收益率回归计算Beta，取负值",
        outlier_rule={"method": "winsorize", "lower": 0.05, "upper": 0.95},
        direction=FactorDirection.POSITIVE  # 因为取了负值，所以越大越好
    ),
    
    FactorConfig(
        name="低个股风险",
        description="低特异性风险（负个股风险）", 
        calculation_method="通过回归残差计算年化特异性风险，取负值",
        outlier_rule={"method": "winsorize", "lower": 0.05, "upper": 0.95},
        direction=FactorDirection.POSITIVE  # 因为取了负值，所以越大越好
    ),
    
    FactorConfig(
        name="低负债率",
        description="资产负债率",
        calculation_method="取各季度资产负债率的平均值",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.NEGATIVE  # 负债率越低越好
    ),
    
    FactorConfig(
        name="奥特曼Z值",
        description="Altman Z-Score财务健康指标",
        calculation_method="每季度计算Z=1.2*(运营资本/总资产)+1.4*(留存收益/总资产)+3.3*(EBIT/总资产)+0.6*(市值/总负债)+1.0*(销售收入/总资产)，再取平均值",
        outlier_rule={"method": "winsorize", "lower": 0.01, "upper": 0.99},
        direction=FactorDirection.POSITIVE
    )
]

# ==============================================================================
# 5. 三维度配置
# ==============================================================================

PROFITABILITY_DIMENSION = DimensionConfig(
    name="盈利能力",
    description="衡量公司盈利效率和质量的综合指标",
    factors=PROFITABILITY_FACTORS,
    weight=None,  # 不使用权重，而是Z值标准化
    factor_weights=[1/6] * 6  # 6个因子等权重
)

GROWTH_DIMENSION = DimensionConfig(
    name="成长能力", 
    description="衡量公司盈利能力持续增长的综合指标",
    factors=GROWTH_FACTORS,
    weight=None,  # 不使用权重，而是Z值标准化
    factor_weights=[1/5] * 5  # 5个因子等权重
)

SAFETY_DIMENSION = DimensionConfig(
    name="安全性",
    description="衡量公司财务稳健性和风险水平的综合指标", 
    factors=SAFETY_FACTORS,
    weight=None,  # 不使用权重，而是Z值标准化
    factor_weights=[1/4] * 4  # 4个因子等权重
)

# ==============================================================================
# 6. 全局配置参数
# ==============================================================================

GLOBAL_CONFIG = {
    # 数据起始时间
    "start_date": "2018-06-30",  # 从2018年Q2开始
    
    # 标准化参数
    "standardization": {
        "method": "zscore",           # Z值标准化
        "scope": "market_wide",       # 全市场范围
        "time_window": "current_year" # 当年数据
    },
    
    # 股票筛选规则
    "stock_filter": {
        "exclude_st": True,           # 剔除ST股票
        "min_market_cap": None,       # 无最小市值限制
        "min_trading_days": 200       # 最少交易天数
    },
    
    # 评分逻辑配置
    "scoring": {
        "method": "multi_level_zscore",   # 多层级Z值标准化
        "level1": "factor_to_dimension",  # 第一级：因子 → 维度Z值  
        "level2": "dimension_to_final",   # 第二级：维度Z值 → 最终Z值
        "dimension_combination": "equal_weight_then_zscore"  # 维度内等权重，然后再次Z值标准化
    },
    
    # 市值分组配置
    "market_cap": {
        "large_small_cutoff": 100,    # 100亿分界线（单位：亿元）
        "currency": "CNY"
    }
}

# ==============================================================================
# 7. 配置管理器
# ==============================================================================

class FactorConfigManager:
    """因子配置管理器"""
    
    def __init__(self):
        self.dimensions = {
            "profitability": PROFITABILITY_DIMENSION,
            "growth": GROWTH_DIMENSION, 
            "safety": SAFETY_DIMENSION
        }
        self.global_config = GLOBAL_CONFIG
    
    def get_all_factors(self) -> List[FactorConfig]:
        """获取所有因子配置"""
        all_factors = []
        for dimension in self.dimensions.values():
            all_factors.extend(dimension.factors)
        return all_factors
    
    def get_factor_by_name(self, name: str) -> FactorConfig:
        """根据名称获取因子配置"""
        for factor in self.get_all_factors():
            if factor.name == name:
                return factor
        raise ValueError(f"未找到名称为 '{name}' 的因子")
    
    def get_dimension_config(self, dimension_name: str) -> DimensionConfig:
        """获取维度配置"""
        if dimension_name not in self.dimensions:
            raise ValueError(f"未找到维度 '{dimension_name}'")
        return self.dimensions[dimension_name]
    
    def get_dimension_weights(self) -> Dict[str, str]:
        """获取维度评分方法（不再使用数值权重）"""
        return {name: "equal_weight_then_zscore" for name in self.dimensions.keys()}
    
    def get_global_config(self, key: str = None):
        """获取全局配置"""
        if key is None:
            return self.global_config
        return self.global_config.get(key)
    
    def validate_config(self) -> bool:
        """验证配置完整性"""
        # 检查每个维度内因子权重是否接近1
        for name, dim in self.dimensions.items():
            factor_weight_sum = sum(dim.factor_weights)
            if abs(factor_weight_sum - 1.0) > 0.001:
                raise ValueError(f"维度 '{name}' 的因子权重总和不等于1: {factor_weight_sum}")
        
        # 检查因子数量与权重数量是否匹配
        for name, dim in self.dimensions.items():
            if len(dim.factors) != len(dim.factor_weights):
                raise ValueError(f"维度 '{name}' 的因子数量与权重数量不匹配")
        
        return True

# ==============================================================================
# 8. 模块接口
# ==============================================================================

# 创建全局配置管理器实例
factor_config_manager = FactorConfigManager()

# 常用接口函数
def get_profitability_factors() -> List[FactorConfig]:
    """获取盈利能力因子配置"""
    return PROFITABILITY_DIMENSION.factors

def get_growth_factors() -> List[FactorConfig]:
    """获取成长能力因子配置"""
    return GROWTH_DIMENSION.factors

def get_safety_factors() -> List[FactorConfig]:
    """获取安全性因子配置"""
    return SAFETY_DIMENSION.factors

def get_all_dimensions() -> Dict[str, DimensionConfig]:
    """获取所有维度配置"""
    return factor_config_manager.dimensions

def get_standardization_config() -> Dict[str, Any]:
    """获取标准化配置"""
    return factor_config_manager.get_global_config("standardization")

def get_stock_filter_config() -> Dict[str, Any]:
    """获取股票筛选配置"""
    return factor_config_manager.get_global_config("stock_filter")
def get_dynamic_profitability_window(year: int) -> int:
    """
    获取盈利能力因子的动态时间窗口
    
    Args:
        year: 计算年份
        
    Returns:
        int: 需要的季度数量
    """
    # 固定使用20个季度（5年）的数据来计算盈利能力因子
    return 20

def get_dynamic_growth_periods(year: int) -> List[int]:
    """
    获取成长能力因子的动态对比期数
    
    Args:
        year: 计算年份
        
    Returns:
        List[int]: 需要对比的年数列表
    """
    if year <= 2021:
        # 2021年及之前：只对比3年前
        return [3]
    elif year == 2022:
        # 2022年：对比3年前和4年前
        return [3, 4]
    else:
        # 2023年及之后：对比3年前、4年前、5年前
        return [3, 4, 5]

def get_beta_config() -> Dict[str, Any]:
    """
    获取Beta计算配置参数
    
    Returns:
        Dict[str, Any]: Beta计算相关配置
    """
    return {
        "time_window_days": 252,        # 使用1年（252个交易日）的数据
        "benchmark_code": "000300.SH",  # 基准指数：沪深300
        "min_observations": 50,         # 最少需要50个有效观测值
        "frequency": "daily"            # 日频数据
    }

def get_missing_thresholds() -> Dict[str, Any]:
    """
    获取数据缺失容错阈值配置
    
    Returns:
        Dict[str, Any]: 各维度的最低因子数量要求
    """
    return {
        # 各维度最低因子数量要求
        "profitability_min_factors": 4,    # 盈利能力：6个因子中至少要4个
        "growth_min_factors": 3,           # 成长能力：5个因子中至少要3个  
        "safety_min_factors": 3,           # 安全性：4个因子中至少要3个
        
        # 最终评分的维度要求
        "max_missing_dimensions": 1,       # 最多允许1个维度缺失
        "min_valid_dimensions": 2,         # 最少需要2个有效维度
        
        # 单只股票的最低数据要求
        "min_quarters_for_calculation": 2, # 最少需要2个季度的数据
        "min_trading_days_for_beta": 50    # Beta计算最少需要50个交易日
    }
# ==============================================================================
# 9. 测试和验证
# ==============================================================================

if __name__ == "__main__":
    # 验证配置完整性
    try:
        factor_config_manager.validate_config()
        print("✅ 配置验证通过！")
        
        # 打印配置摘要
        print(f"\n📊 配置摘要:")
        print(f"- 盈利能力因子: {len(get_profitability_factors())} 个")
        print(f"- 成长能力因子: {len(get_growth_factors())} 个") 
        print(f"- 安全性因子: {len(get_safety_factors())} 个")
        print(f"- 总计: {len(factor_config_manager.get_all_factors())} 个因子")
        
        print(f"\n⚖️ 评分方法:")
        scoring_config = factor_config_manager.get_global_config('scoring')
        print(f"- 评分方式: {scoring_config['method']}")
        print(f"- 第一级: {scoring_config['level1']}")  
        print(f"- 第二级: {scoring_config['level2']}")
        print(f"- 维度组合: {scoring_config['dimension_combination']}")
            
        print(f"\n🕐 数据起始时间: {factor_config_manager.get_global_config('start_date')}")
        
    except Exception as e:
        print(f"❌ 配置验证失败: {e}")