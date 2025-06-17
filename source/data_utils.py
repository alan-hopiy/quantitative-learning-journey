# file: source/data_utils.py

import pandas as pd
import os
from . import config
from .load_data import load_data_from_db 

def export_data_for_inspection(table_name: str, column_to_inspect: str, output_filename: str = None):
    """从数据库指定表中加载数据，提取指定列，并将其导出到 Excel 文件以便人工检查。"""
    print(f"--- 准备从表[{table_name}]导出用于检查的 '{column_to_inspect}' 原始数据 ---")

    df_raw = load_data_from_db(table_name=table_name)
    if df_raw is None or df_raw.empty:
        print(f"[错误] 从表[{table_name}]加载数据失败或数据为空，无法继续。")
        return False
    print("数据加载成功！")

    required_cols = ['ts_code', 'end_date', column_to_inspect]
    if not all(col in df_raw.columns for col in required_cols):
        print(f"[错误] 您的数据中缺少必要的列。需要 {required_cols}，但只找到了 {df_raw.columns.tolist()}")
        return False

    df_for_inspection = df_raw[required_cols].copy()
    
    if 'end_date' in df_for_inspection.columns:
        df_for_inspection['end_date'] = pd.to_datetime(df_for_inspection['end_date'], errors='coerce')
        df_for_inspection.sort_values(by=['ts_code', 'end_date'], inplace=True)
    else:
        df_for_inspection.sort_values(by=['ts_code'], inplace=True)
    print("数据已准备和排序完毕。")
    
    # [细节修改] 统一输出到 '初步数据' 文件夹
    output_dir = os.path.join(config.BASE_OUTPUT_DIR, '初步数据')
    os.makedirs(output_dir, exist_ok=True)
    
    if output_filename is None:
        final_filename = f"{table_name}_{column_to_inspect}_inspection.xlsx"
    else:
        final_filename = output_filename
    
    output_path = os.path.join(output_dir, final_filename)

    try:
        df_for_inspection.to_excel(output_path, index=False, engine='openpyxl')
        print(f"\n[成功] 原始数据已成功导出到: {output_path}")
        return True
    except Exception as e:
        print(f"\n[失败] 文件导出失败: {e}")
        return False

def export_wide_panel(table_name: str, value_column: str, output_filename: str = None):
    """[完整实现] 从数据库指定表中加载长表数据，将其中的一个指标列转换为宽表格式，并导出。"""
    print(f"--- 准备将表[{table_name}]的 '{value_column}' 数据转换为宽表 ---")

    # 1. 加载数据
    df_raw = load_data_from_db(table_name=table_name)
    if df_raw is None or df_raw.empty:
        print(f"[错误] 从表[{table_name}]加载数据失败或数据为空，无法继续。")
        return False
    print("长表数据加载成功！")

    # 2. 数据准备
    required_cols = ['ts_code', 'end_date', value_column]
    if not all(col in df_raw.columns for col in required_cols):
        print(f"[错误] 您的数据中缺少必要的列。需要 {required_cols}，但只找到了 {df_raw.columns.tolist()}")
        return False
    
    df = df_raw[required_cols].copy()
    df['end_date'] = pd.to_datetime(df['end_date'])
    df[value_column] = pd.to_numeric(df[value_column], errors='coerce')

    # --- [细节修改] 打印详细的去重和去空值信息 ---
    print(f"\n数据清洗前总行数: {len(df)}")
    
    # 处理空值
    rows_before_na = len(df)
    df.dropna(subset=['ts_code', 'end_date', value_column], inplace=True)
    rows_after_na = len(df)
    print(f"因包含空值被删除的行数: {rows_before_na - rows_after_na}")
    
    # 处理重复值
    rows_before_duplicates = len(df)
    df.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)
    rows_after_duplicates = len(df)
    print(f"因重复被删除的行数: {rows_before_duplicates - rows_after_duplicates}")
    print(f"数据清洗后剩余行数: {len(df)}")
    # --- [修改结束] ---

    # 3. 核心：长表转宽表 (Pivot)
    print("\n正在执行 pivot_table 操作...")
    try:
        panel_df = df.pivot_table(
            index='ts_code',
            columns='end_date',
            values=value_column
        )
        print("宽表创建成功！")
    except Exception as e:
        print(f"\n[失败] pivot_table 操作失败: {e}")
        return False

    # 4. 导出到Excel
    # --- [细节修改] 统一输出到 '初步数据' 文件夹 ---
    output_dir = os.path.join(config.BASE_OUTPUT_DIR, '初步数据')
    os.makedirs(output_dir, exist_ok=True)
    
    if output_filename is None:
        final_filename = f"{table_name}_{value_column}_wide_panel.xlsx"
    else:
        final_filename = output_filename
        
    output_path = os.path.join(output_dir, final_filename)
    
    try:
        panel_df.to_excel(output_path, index=True, engine='openpyxl')
        print(f"\n[成功] 宽表数据已成功导出到: {output_path}")
        return True
    except Exception as e:
        print(f"\n[失败] 宽表文件导出失败: {e}")
        return False