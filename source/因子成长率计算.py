
# file: source/因子成长率计算.py (最终日志增强版)

import pandas as pd
import numpy as np
import os

def calculate_stable_growth(row, target_years, 
                            base_window_half_size=2, 
                            min_periods_in_window=3):
    """
    【可配置】成长率计算函数，基于窗口平均值，以平滑异常值。
    """
    series = row.dropna()
    
    if len(series) < 3:
        return np.nan
    latest_window = series.iloc[-3:]
    latest_avg = latest_window.mean()

    base_avg = np.nan
    actual_years = np.nan
    
    for shift in range(6): 
        base_period_center_idx = -(target_years * 4) - 1 - shift
        window_start_idx = base_period_center_idx - base_window_half_size
        window_end_idx = base_period_center_idx + base_window_half_size + 1
        
        if window_start_idx >= -len(series) and window_end_idx <= 0:
            base_window = series.iloc[window_start_idx:window_end_idx]
            if base_window.count() >= min_periods_in_window:
                base_avg = base_window.mean()
                actual_years = (series.index.get_loc(latest_window.index[-1]) - series.index.get_loc(base_window.index[-1])) / 4.0
                break

    if pd.isna(base_avg):
        return np.nan

    if base_avg > 0 and latest_avg > 0:
        if actual_years < 1: actual_years = 1.0
        return (latest_avg / base_avg) ** (1 / actual_years) - 1
    else:
        return -10

def build_composite_growth_factor(panel_data: pd.DataFrame, 
                                  factor_prefix: str,
                                  years_list: list = [3, 4, 5]):
    """
    【工作流函数】根据输入的宽表面板数据，构建一个复合成长因子，并输出详细日志。
    """
    print(f"\n--- 开始构建复合成长因子: {factor_prefix}_growth ---")
    results_df = pd.DataFrame(index=panel_data.index)

    # 步骤 2: 计算多周期成长率
    print("\n--- 步骤 2: 计算带质量惩罚的多周期成长率 ---")
    for years in years_list:
        print(f"正在计算 {years}年期 CAGR...")
        col_name = f'{factor_prefix}_cagr_{years}yr_stable'
        results_df[col_name] = panel_data.apply(
            calculate_stable_growth, 
            axis=1, 
            target_years=years,
            base_window_half_size=2
        )

    # 步骤 3: 独立进行Z-score打分
    print("\n--- 步骤 3: 独立进行Z-score打分 ---")
    zscore_cols = []
    for years in years_list:
        cagr_col = f'{factor_prefix}_cagr_{years}yr_stable'
        zscore_col = f'{factor_prefix}_zscore_{years}yr_stable'
        zscore_cols.append(zscore_col)
        
        mean = results_df[cagr_col].mean()
        std = results_df[cagr_col].std()
        results_df[zscore_col] = (results_df[cagr_col] - mean) / std
        print(f"完成 {zscore_col} 的计算。")
    
    # 步骤 4: 基于有效得分数量，筛选高质量公司
    print("\n--- 步骤 4: 基于有效得分数量，筛选高质量公司 ---")
    print(f"筛选前公司数量: {len(results_df)}")
    MIN_VALID_SCORES = 2
    valid_score_counts = results_df[zscore_cols].count(axis=1)
    results_df_filtered = results_df[valid_score_counts >= MIN_VALID_SCORES].copy()
    print(f"筛选后公司数量: {len(results_df_filtered)}")

    # 步骤 5: 对缺失的Z-score进行惩罚性填充
    print("\n--- 步骤 5: 对缺失的Z-score填充0值 (作为中性惩罚) ---")
    results_df_filtered[zscore_cols] = results_df_filtered[zscore_cols].fillna(0)
    print("缺失Z-score填充完成。")

    # 步骤 6: 合成得分，并对最终合成得分再次进行标准化
    print("\n--- 步骤 6: 合成得分并进行最终标准化 ---")
    final_score_col = f'composite_{factor_prefix}_growth_score'
    composite_avg = results_df_filtered[zscore_cols].mean(axis=1)
    final_mean = composite_avg.mean()
    final_std = composite_avg.std()
    results_df_filtered[final_score_col] = (composite_avg - final_mean) / final_std
    print("最终得分合成与标准化完成。")
    
    # 步骤 7: 排序并预览最终结果
    results_df_filtered.sort_values(by=final_score_col, ascending=False, inplace=True)
    print(f"\n--- 最终{factor_prefix}成长性打分排名预览 (前10名) ---")
    # 动态选择要展示的列（所有z-score列和最终的合成得分列）
    display_cols = zscore_cols + [final_score_col]
    print(results_df_filtered[display_cols].head(10))
    
    return results_df_filtered