# -*- coding: utf-8 -*-
"""
数据配置文件 - 存储所有基础数据源和字段映射信息
修正版本：根据实际数据库字段调整
"""

# ==============================================================================
# 1. 数据库连接配置
# ==============================================================================

DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "mydb", 
    "user": "alan-hopiy",
    "password": ""
}

# ==============================================================================
# 2. API配置
# ==============================================================================

TUSHARE_CONFIG = {
    "token": "a872d82f46046d335ccf68ef591747ff66b9a9d598b40791b80f017a",
    "pro_api": True
}

# ==============================================================================
# 3. 日期字段配置（按数据类型分类）
# ==============================================================================

DATE_FIELDS = {
    "financial_data": "end_date",      # 财务数据日期字段（季频）
    "market_data": "trade_date"        # 市场数据日期字段（日频）
}

# ==============================================================================
# 4. 股票代码字段配置（按表名分类）
# ==============================================================================

STOCK_CODE_FIELDS = {
    "stock_basic_info": "ts_code",
    "stock_financial_indicators": "ts_code", 
    "stock_balance_sheets": "ts_code",
    "financial_income_statement": "ts_code",
    "stock_daily_basic": "ts_code",
    "a_share_daily_data": "ts_code",
    "stock_cap_classification": "ts_code"
}

# ==============================================================================
# 5. 基础信息字段配置
# ==============================================================================

BASIC_INFO_FIELDS = {
    "stock_code": {
        "table": "stock_basic_info",
        "field": "ts_code"
    },
    "industry": {
        "table": "stock_basic_info", 
        "field": "industry"
    },
    "list_date": {
        "table": "stock_basic_info",
        "field": "list_date"
    }
}

# ==============================================================================
# 6. 盈利质量因子数据源配置（6个因子）- 修正版
# ==============================================================================

PROFITABILITY_DATA_SOURCES = {
    # 因子1: ROE
    "q_roe": {
        "table": "stock_financial_indicators",
        "field": "q_roe",
        "data_type": "financial_data"
    },
    
    # 因子2: ROA
    "roa_yearly": {
        "table": "stock_financial_indicators", 
        "field": "roa_yearly",
        "data_type": "financial_data"
    },
    
    # 因子3: 毛利率
    "grossprofit_margin": {
        "table": "stock_financial_indicators",
        "field": "grossprofit_margin", 
        "data_type": "financial_data"
    },
    
    # 因子4: 自由现金流/资产 (需要两个字段)
    "fcff": {
        "table": "stock_financial_indicators",
        "field": "fcff",
        "data_type": "financial_data"
    },
    "total_assets_for_fcff": {
        "table": "stock_balance_sheets",
        "field": "total_assets", 
        "data_type": "financial_data"
    },
    
    # 因子5: 毛利润/资产 (需要两个字段)
    "gross_margin": {
        "table": "stock_financial_indicators",
        "field": "gross_margin",
        "data_type": "financial_data"
    },
    "total_assets_for_margin": {
        "table": "stock_balance_sheets", 
        "field": "total_assets",
        "data_type": "financial_data"
    },
    
    # 因子6: 折旧-运营资本的变化/总资产 (需要三个字段)
    "daa": {
        "table": "stock_financial_indicators",
        "field": "daa",
        "data_type": "financial_data"
    },
    "working_capital": {
        "table": "stock_financial_indicators",
        "field": "working_capital", 
        "data_type": "financial_data"
    },
    "total_assets_for_daa": {
        "table": "stock_balance_sheets",
        "field": "total_assets",
        "data_type": "financial_data"
    }
}

# ==============================================================================
# 7. 成长能力因子数据源配置（5个因子，复用盈利质量前5个）
# ==============================================================================

GROWTH_DATA_SOURCES = {
    # 复用盈利质量因子1-5的数据源
    "q_roe": PROFITABILITY_DATA_SOURCES["q_roe"],
    "roa_yearly": PROFITABILITY_DATA_SOURCES["roa_yearly"], 
    "grossprofit_margin": PROFITABILITY_DATA_SOURCES["grossprofit_margin"],
    "fcff": PROFITABILITY_DATA_SOURCES["fcff"],
    "total_assets_for_fcff": PROFITABILITY_DATA_SOURCES["total_assets_for_fcff"],
    "gross_margin": PROFITABILITY_DATA_SOURCES["gross_margin"],
    "total_assets_for_margin": PROFITABILITY_DATA_SOURCES["total_assets_for_margin"]
}

# ==============================================================================
# 8. 安全性因子数据源配置（4个因子）- 修正版
# ==============================================================================

SAFETY_DATA_SOURCES = {
    # 因子1&2: 低Beta和低个股风险 (通过日行情数据计算)
    "daily_close": {
        "table": "a_share_daily_data",
        "field": "close",
        "data_type": "market_data"
    },
    "benchmark_code": "000300.SH",  # 沪深300作为基准
    
    # 因子3: 资产负债率
    "debt_to_assets": {
        "table": "stock_financial_indicators",
        "field": "debt_to_assets", 
        "data_type": "financial_data"
    },
    
    # 因子4: 奥特曼Z值 (需要6个字段) - 修正表名
    "altman_working_capital": {
        "table": "stock_financial_indicators",
        "field": "working_capital",
        "data_type": "financial_data"
    },
    "altman_retained_earnings": {
        "table": "stock_financial_indicators", 
        "field": "retained_earnings",
        "data_type": "financial_data"
    },
    "altman_ebit": {
        "table": "stock_financial_indicators",
        "field": "ebit",
        "data_type": "financial_data"
    },
    "altman_market_value": {
        "table": "stock_daily_basic",  # 修正：从stock_daily_basic获取市值
        "field": "total_mv",
        "data_type": "market_data"
    },
    "altman_sales": {
        "table": "stock_financial_indicators",  # 修正：从财务指标表获取收入
        "field": "op_income",  # 使用营业收入
        "data_type": "financial_data"
    },
    "altman_total_assets": {
        "table": "stock_balance_sheets",
        "field": "total_assets",
        "data_type": "financial_data"
    }
}

# ==============================================================================
# 9. 市值分组数据源配置 - 修正版
# ==============================================================================

MARKET_CAP_DATA_SOURCES = {
    "total_market_value": {
        "table": "stock_daily_basic",  # 修正：从stock_daily_basic获取市值
        "field": "total_mv",
        "data_type": "market_data"
    }
}

# ==============================================================================
# 10. 所有数据表清单
# ==============================================================================

ALL_TABLES = [
    "stock_basic_info",
    "stock_financial_indicators", 
    "stock_balance_sheets",
    "financial_income_statement",
    "stock_daily_basic",
    "a_share_daily_data",
    "stock_cap_classification"
]

# ==============================================================================
# 11. 数据路径配置
# ==============================================================================

DATA_PATHS = {
    "cache_dir": "data/cache/",
    "temp_dir": "data/temp/",
    "results_dir": "results/",
    "reports_dir": "results/reports/",
    "charts_dir": "results/charts/", 
    "logs_dir": "results/logs/"
}

# ==============================================================================
# 12. 实际可用字段映射（新增） - 根据数据库检查结果
# ==============================================================================

# 财务指标表可用字段
FINANCIAL_INDICATORS_FIELDS = [
    'ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps', 'total_revenue_ps', 'revenue_ps', 
    'capital_rese_ps', 'surplus_rese_ps', 'undist_profit_ps', 'extra_item', 'profit_dedt', 
    'gross_margin', 'current_ratio', 'quick_ratio', 'cash_ratio', 'ar_turn', 'ca_turn', 
    'fa_turn', 'assets_turn', 'op_income', 'ebit', 'ebitda', 'fcff', 'fcfe', 'current_exint', 
    'noncurrent_exint', 'interestdebt', 'netdebt', 'tangible_asset', 'working_capital', 
    'networking_capital', 'invest_capital', 'retained_earnings', 'diluted2_eps', 'bps', 
    'ocfps', 'retainedps', 'cfps', 'ebit_ps', 'fcff_ps', 'fcfe_ps', 'netprofit_margin', 
    'grossprofit_margin', 'cogs_of_sales', 'expense_of_sales', 'profit_to_gr', 'saleexp_to_gr', 
    'adminexp_of_gr', 'finaexp_of_gr', 'impai_ttm', 'gc_of_gr', 'op_of_gr', 'ebit_of_gr', 
    'roe', 'roe_waa', 'roe_dt', 'roa', 'npta', 'roic', 'roe_yearly', 'roa2_yearly', 
    'debt_to_assets', 'assets_to_eqt', 'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets', 
    'tbassets_to_totalassets', 'int_to_talcap', 'eqt_to_talcapital', 'currentdebt_to_debt', 
    'longdeb_to_debt', 'ocf_to_shortdebt', 'debt_to_eqt', 'eqt_to_debt', 'eqt_to_interestdebt', 
    'tangibleasset_to_debt', 'tangasset_to_intdebt', 'tangibleasset_to_netdebt', 'ocf_to_debt', 
    'turn_days', 'roa_yearly', 'roa_dp', 'fixed_assets', 'profit_to_op', 'q_saleexp_to_gr', 
    'q_gc_to_gr', 'q_roe', 'q_dt_roe', 'q_npta', 'q_ocf_to_sales', 'basic_eps_yoy', 'dt_eps_yoy', 
    'cfps_yoy', 'op_yoy', 'ebt_yoy', 'netprofit_yoy', 'dt_netprofit_yoy', 'ocf_yoy', 'roe_yoy', 
    'bps_yoy', 'assets_yoy', 'eqt_yoy', 'tr_yoy', 'or_yoy', 'q_sales_yoy', 'q_op_qoq', 
    'equity_yoy', 'daa'
]

# 日度基本面数据表可用字段
DAILY_BASIC_FIELDS = [
    'ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 'volume_ratio', 
    'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_share', 'float_share', 
    'free_share', 'total_mv', 'circ_mv'
]

# 日度行情数据表可用字段
DAILY_PRICE_FIELDS = [
    'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 
    'vol', 'amount'
]

# 资产负债表可用字段
BALANCE_SHEET_FIELDS = [
    'ts_code', 'ann_date', 'end_date', 'report_type', 'comp_type', 'total_assets', 'total_liab', 
    'total_hldr_eqy_inc_min_int', 'cap_rese', 'undistr_porfit', 'accounts_receiv'
]