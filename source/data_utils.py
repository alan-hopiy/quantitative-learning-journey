# file: data_utils.py

import pandas as pd
import os
import config  # 假设您的 config 文件在同一路径下
from load_data import load_financial_data_from_db # 引入您的数据加载函数

def export_data_for_inspection(column_to_inspect: str, output_filename: str = None):
    """
    从数据库加载原始财务数据，提取指定列，并将其导出到 Excel 文件以便人工检查。

    这个函数会自动处理数据类型转换、排序和文件路径管理。

    Args:
        column_to_inspect (str): 您想要检查的列的名称，例如 'fcfe', 'roe', 'net_profit'。
        output_filename (str, optional): 导出的 Excel 文件名。
                                         如果留空 (None)，会自动生成一个基于列名的文件名，
                                         例如 'fcfe_for_inspection.xlsx'。

    Returns:
        bool: 如果导出成功，返回 True；否则返回 False。
    """
    print(f"--- 准备导出用于检查的 '{column_to_inspect}' 原始数据 ---")

    # 1. 加载数据
    df_raw = load_financial_data_from_db()
    if df_raw is None or df_raw.empty:
        print("[错误] 数据加载失败或数据为空，无法继续。")
        return False

    print("数据加载成功！")

    # 2. 数据准备与清洗
    required_cols = ['ts_code', 'end_date', column_to_inspect]
    if not all(col in df_raw.columns for col in required_cols):
        print(f"[错误] 您的数据中缺少必要的列。需要 {required_cols}，但只找到了 {df_raw.columns.tolist()}")
        return False

    df_for_inspection = df_raw[required_cols].copy()

    # --- 标准化数据格式 ---
    df_for_inspection['end_date'] = pd.to_datetime(df_for_inspection['end_date'], errors='coerce')
    df_for_inspection[column_to_inspect] = pd.to_numeric(df_for_inspection[column_to_inspect], errors='coerce')

    # 删除关键信息为空的行
    df_for_inspection.dropna(subset=['ts_code', 'end_date'], inplace=True)

    # 按公司和时间顺序排列
    df_for_inspection.sort_values(by=['ts_code', 'end_date'], inplace=True)
    print("数据已准备和排序完毕。")

    # 3. 导出到Excel
    print("\n--- 正在导出数据到Excel文件 ---")
    output_dir = config.BASE_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    # 如果用户没有指定文件名，我们就自动创建一个
    if output_filename is None:
        final_filename = f"{column_to_inspect}_source_data_for_inspection.xlsx"
    else:
        final_filename = output_filename

    output_path = os.path.join(output_dir, final_filename)

    try:
        df_for_inspection.to_excel(output_path, index=False, engine='openpyxl')
        print(f"\n[成功] 数据已成功导出到: {output_path}")
        print("请打开此文件进行检查。")
        return True
    except Exception as e:
        print(f"\n[失败] 文件导出失败: {e}")
        return False


# (这是要添加到 data_utils.py 文件中的新函数)

def export_wide_panel(value_column: str, output_filename: str = None):
    """
    从数据库加载长表格式的财务数据，将其中的一个指标列转换为宽表（面板数据）格式，
    并导出到 Excel 文件。

    宽表格式：索引为股票代码(ts_code)，列为报告期(end_date)，值为指定的财务指标。

    Args:
        value_column (str): 您想要转换为面板数据的指标列名，例如 'q_roe', 'q_net_profit'。
        output_filename (str, optional): 导出的 Excel 文件名。
                                         如果留空 (None)，会自动生成一个基于指标名的文件名，
                                         例如 'q_roe_wide_format.xlsx'。

    Returns:
        bool: 如果导出成功，返回 True；否则返回 False。
    """
    print(f"--- 准备将 '{value_column}' 数据转换为宽表 ---")

    # 1. 加载数据 (与上一个函数相同)
    df_raw = load_financial_data_from_db()
    if df_raw is None or df_raw.empty:
        print("[错误] 数据加载失败或数据为空，无法继续。")
        return False
    print("数据加载成功！")

    # 2. 数据准备与清洗
    required_cols = ['ts_code', 'end_date', value_column]
    if not all(col in df_raw.columns for col in required_cols):
        print(f"[错误] 您的数据中缺少必要的列。需要 {required_cols}，但只找到了 {df_raw.columns.tolist()}")
        return False

    df = df_raw[required_cols].copy()

    # --- 标准化数据格式 ---
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
    
    # 对于pivot，必须确保要转换的值不含NaN
    df.dropna(subset=['ts_code', 'end_date', value_column], inplace=True)

    # --- 删除重复行，确保每个公司在每个报告期只有一个值 ---
    print(f"\n去重前的数据行数: {len(df)}")
    df.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)
    print(f"去重后的数据行数: {len(df)}")

    # 排序
    df.sort_values(by=['ts_code', 'end_date'], inplace=True)
    print("\n数据清洗与去重完成。")

    # 3. 核心步骤：长表转宽表 (Pivot)
    print("\n正在执行 pivot_table 操作...")
    try:
        panel_df = df.pivot_table(
            index='ts_code',
            columns='end_date',
            values=value_column
        )
        print("宽表创建成功！")

        # 4. 导出到Excel
        print("\n--- 正在导出宽表到Excel文件 ---")
        output_dir = config.BASE_OUTPUT_DIR
        os.makedirs(output_dir, exist_ok=True)

        # 自动生成文件名
        if output_filename is None:
            final_filename = f"{value_column}_wide_format.xlsx"
        else:
            final_filename = output_filename

        output_path = os.path.join(output_dir, final_filename)

        # index=True 表示需要把行索引(ts_code)也写入文件
        panel_df.to_excel(output_path, index=True, engine='openpyxl')

        print(f"\n[成功] 已将宽表格式的数据导出到: {output_path}")
        return True

    except Exception as e:
        print(f"\n[失败] pivot_table或导出操作失败: {e}")
        return False