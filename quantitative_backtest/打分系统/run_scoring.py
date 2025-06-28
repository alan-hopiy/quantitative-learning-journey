# -*- coding: utf-8 -*-
"""
股票评分系统主程序
一键运行股票基本面评分和排名

使用方法：
1. 修改下面的 CALCULATION_DATE 参数
2. 运行此程序
3. 在 output/scores/ 目录查看Excel结果

项目：基本面量化回测系统 - 打分系统
作者：量化团队
创建时间：2025-06-28
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 添加项目路径
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

# 导入系统模块
try:
    from src.core.scoring_system import create_scoring_system
    from src.utils.excel_utils import export_to_excel
    from config.scoring_config import get_basic_config, initialize_scoring_system
except ImportError as e:
    print(f"❌ 模块导入失败: {e}")
    print("请确保所有文件都已正确保存到对应位置")
    sys.exit(1)

# ==============================================================================
# 🎯 主要参数设置（用户修改这里）
# ==============================================================================

# 📅 计算日期 - 修改这个日期来计算不同时间点的股票评分
CALCULATION_DATE = "2022-05-01"

# 📊 其他设置（一般不需要修改）
ENABLE_DEBUG_MODE = False          # 调试模式（仅处理少量股票）
DEBUG_STOCK_LIMIT = 20             # 调试模式下的股票数量限制
OUTPUT_CUSTOM_PATH = None          # 自定义输出路径（None=自动生成）
SPECIFIC_STOCKS = None             # 指定股票代码列表（None=全部股票）

# ==============================================================================
# 📝 日志设置
# ==============================================================================

def setup_logging():
    """设置日志格式"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # 控制台输出
        ]
    )
    
    # 减少第三方库的日志输出
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

# ==============================================================================
# 🚀 主执行函数
# ==============================================================================

def run_stock_scoring():
    """执行股票评分主流程"""
    
    print("🎯 股票基本面评分系统")
    print("=" * 50)
    print(f"📅 计算日期: {CALCULATION_DATE}")
    print(f"🔧 调试模式: {'开启' if ENABLE_DEBUG_MODE else '关闭'}")
    
    if SPECIFIC_STOCKS:
        print(f"📊 指定股票: {len(SPECIFIC_STOCKS)} 只")
    else:
        print(f"📊 计算范围: 全部股票")
    
    print("=" * 50)
    
    try:
        # 1. 初始化系统
        print("🔄 正在初始化评分系统...")
        
        # 初始化配置
        initialize_scoring_system()
        
        # 创建评分系统
        scoring_system = create_scoring_system()
        
        # 检查系统状态
        system_status = scoring_system.get_system_status()
        print(f"📊 系统状态: {system_status.get('系统状态', '未知')}")
        print(f"📈 股票池大小: {system_status.get('股票池大小', 0)} 只")
        
        if system_status.get('系统状态') != '正常':
            print("❌ 系统状态异常，请检查数据库连接和配置")
            return False
        
        # 2. 准备股票列表
        stock_codes = SPECIFIC_STOCKS
        
        if ENABLE_DEBUG_MODE and stock_codes is None:
            # 调试模式：获取少量股票进行测试
            print(f"🔧 调试模式：限制处理 {DEBUG_STOCK_LIMIT} 只股票")
            stock_info = scoring_system.data_manager.get_stock_basic_info()
            if not stock_info.empty:
                stock_codes = stock_info['ts_code'].head(DEBUG_STOCK_LIMIT).tolist()
        
        # 3. 执行评分计算
        print("🔄 开始计算股票评分...")
        print("   ⏳ 这可能需要几分钟时间，请耐心等待...")
        
        calculation_result = scoring_system.calculate_scores(
            calculation_date=CALCULATION_DATE,
            stock_codes=stock_codes
        )
        
        # 4. 检查计算结果
        if not calculation_result.get("success", False):
            error_msg = calculation_result.get("error", "未知错误")
            print(f"❌ 评分计算失败: {error_msg}")
            return False
        
        results_df = calculation_result["results"]
        statistics = calculation_result["statistics"]
        
        print(f"✅ 评分计算完成!")
        print(f"📊 成功计算 {len(results_df)} 只股票")
        
        # 5. 显示计算统计
        print("\n📈 计算统计:")
        print("-" * 30)
        print(f"大盘股数量: {statistics.get('大盘股数量', 0)} 只")
        print(f"小盘股数量: {statistics.get('小盘股数量', 0)} 只")
        
        if "得分统计" in statistics:
            score_stats = statistics["得分统计"]
            print(f"平均得分: {score_stats.get('平均分', 0):.4f}")
            print(f"得分标准差: {score_stats.get('标准差', 0):.4f}")
            print(f"最高得分: {score_stats.get('最高分', 0):.4f}")
            print(f"最低得分: {score_stats.get('最低分', 0):.4f}")
        
        # 6. 显示前10名股票
        if not results_df.empty:
            print("\n🏆 前10名股票:")
            print("-" * 50)
            
            display_columns = []
            if 'ts_code' in results_df.columns:
                display_columns.append('ts_code')
            if 'stock_name' in results_df.columns:
                display_columns.append('stock_name')
            if 'final_score' in results_df.columns:
                display_columns.append('final_score')
            if 'final_rank' in results_df.columns:
                display_columns.append('final_rank')
            
            if display_columns:
                top10 = results_df[display_columns].head(10)
                for i, (_, row) in enumerate(top10.iterrows(), 1):
                    code = row.get('ts_code', 'N/A')
                    name = row.get('stock_name', 'N/A')
                    score = row.get('final_score', 0)
                    print(f"{i:2d}. {code} {name:8s} 得分: {score:8.4f}")
        
        # 7. 导出Excel文件
        print("\n💾 正在导出Excel文件...")
        
        export_result = export_to_excel(
            results=results_df,
            calculation_date=CALCULATION_DATE,
            statistics=statistics,
            output_path=OUTPUT_CUSTOM_PATH
        )
        
        if export_result["success"]:
            output_file = export_result["output_file"]
            file_size = export_result["file_size"]
            
            print(f"✅ Excel文件导出成功!")
            print(f"📁 文件路径: {output_file}")
            print(f"📦 文件大小: {file_size}")
            print(f"📊 包含数据: {export_result['total_stocks']} 只股票，{export_result['columns_count']} 个数据列")
        else:
            print(f"❌ Excel导出失败: {export_result['error']}")
            return False
        
        # 8. 完成总结
        print("\n" + "=" * 50)
        print("🎉 股票评分任务完成!")
        print(f"📅 计算日期: {CALCULATION_DATE}")
        print(f"📊 评分股票: {len(results_df)} 只")
        print(f"📁 结果文件: {Path(output_file).name}")
        print(f"⏰ 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 9. 使用提示
        print("\n💡 使用提示:")
        print("1. 查看详细结果请打开Excel文件")
        print("2. 修改日期重新运行可计算其他时间点的评分")
        print("3. Excel文件包含所有因子的详细数值和统计信息")
        
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断了程序执行")
        return False
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        logging.exception("详细错误信息:")
        return False
    finally:
        # 清理资源
        if 'scoring_system' in locals():
            try:
                scoring_system.data_manager.stop_scheduler()
            except:
                pass

# ==============================================================================
# 🔧 批量计算函数（高级功能）
# ==============================================================================

def run_batch_scoring(date_list):
    """
    批量计算多个日期的评分
    
    Args:
        date_list: 日期列表，如 ["2022-05-01", "2022-08-01", "2022-11-01"]
    """
    print("🎯 批量股票评分任务")
    print("=" * 50)
    print(f"📅 计算日期: {len(date_list)} 个")
    for date in date_list:
        print(f"   - {date}")
    print("=" * 50)
    
    try:
        # 初始化系统
        initialize_scoring_system()
        scoring_system = create_scoring_system()
        
        # 批量计算
        batch_result = scoring_system.batch_calculate(
            date_list=date_list,
            stock_codes=SPECIFIC_STOCKS
        )
        
        # 显示结果
        print(f"\n📊 批量计算完成:")
        print(f"总任务数: {batch_result['total_dates']}")
        print(f"成功数量: {batch_result['success_count']}")
        print(f"失败数量: {batch_result['failure_count']}")
        print(f"成功率: {batch_result['success_rate']:.1%}")
        
        # 为每个成功的日期生成Excel
        for date, result in batch_result['results'].items():
            if result.get('success', False):
                export_result = export_to_excel(
                    results=result['results'],
                    calculation_date=date,
                    statistics=result['statistics']
                )
                if export_result['success']:
                    print(f"✅ {date}: {export_result['output_file']}")
                else:
                    print(f"❌ {date}: Excel导出失败")
            else:
                print(f"❌ {date}: {result.get('error', '计算失败')}")
        
        return True
        
    except Exception as e:
        print(f"❌ 批量计算失败: {e}")
        return False

# ==============================================================================
# 🎮 交互式菜单（可选功能）
# ==============================================================================

def interactive_menu():
    """交互式菜单"""
    while True:
        print("\n🎯 股票评分系统菜单")
        print("=" * 30)
        print("1. 单日评分计算")
        print("2. 批量评分计算") 
        print("3. 系统状态检查")
        print("4. 退出程序")
        print("=" * 30)
        
        choice = input("请选择功能 (1-4): ").strip()
        
        if choice == "1":
            global CALCULATION_DATE
            date_input = input(f"请输入计算日期 (默认: {CALCULATION_DATE}): ").strip()
            if date_input:
                CALCULATION_DATE = date_input
            run_stock_scoring()
            
        elif choice == "2":
            print("请输入日期列表，用逗号分隔 (如: 2022-05-01,2022-08-01)")
            dates_input = input("日期列表: ").strip()
            if dates_input:
                date_list = [d.strip() for d in dates_input.split(',')]
                run_batch_scoring(date_list)
            
        elif choice == "3":
            try:
                initialize_scoring_system()
                scoring_system = create_scoring_system()
                status = scoring_system.get_system_status()
                print("\n📊 系统状态:")
                for key, value in status.items():
                    print(f"  {key}: {value}")
            except Exception as e:
                print(f"❌ 状态检查失败: {e}")
                
        elif choice == "4":
            print("👋 再见!")
            break
        else:
            print("❌ 无效选择，请重新输入")

# ==============================================================================
# 🏁 程序入口
# ==============================================================================

if __name__ == "__main__":
    # 设置日志
    setup_logging()
    
    # 检查Python版本
    if sys.version_info < (3, 7):
        print("❌ 需要Python 3.7或更高版本")
        sys.exit(1)
    
    print("🚀 启动股票评分系统...")
    
    try:
        # 运行主程序
        success = run_stock_scoring()
        
        if success:
            print("\n🎊 程序执行成功!")
            
            # 询问是否继续
            continue_choice = input("\n是否继续使用其他功能? (y/N): ").strip().lower()
            if continue_choice in ['y', 'yes']:
                interactive_menu()
        else:
            print("\n💥 程序执行失败，请检查错误信息")
            
    except KeyboardInterrupt:
        print("\n⚠️ 程序被用户中断")
    except Exception as e:
        print(f"\n💥 程序异常退出: {e}")
        logging.exception("程序异常详情:")
    finally:
        print("\n👋 程序结束")

# ==============================================================================
# 📚 使用说明和示例
# ==============================================================================

"""
🎯 使用说明:

1. 基础使用:
   - 修改顶部的 CALCULATION_DATE = "2022-05-01"
   - 运行此程序: python run_scoring.py
   - 在 output/scores/ 目录查看Excel结果

2. 调试模式:
   - 设置 ENABLE_DEBUG_MODE = True
   - 程序将只处理前20只股票，用于测试

3. 指定股票:
   - 设置 SPECIFIC_STOCKS = ["000001.SZ", "000002.SZ"]
   - 程序将只计算指定的股票

4. 批量计算:
   - 在程序中调用 run_batch_scoring(["2022-05-01", "2022-08-01"])
   
5. 交互式使用:
   - 程序运行完成后选择继续，可进入交互式菜单

📊 输出文件:
   - Excel文件: output/scores/股票评分_YYYYMMDD.xlsx
   - 包含详细的因子数值、维度得分和最终排名
   - 附带统计信息工作表

🔧 自定义配置:
   - 修改 config/scoring_config.py 中的参数
   - 调整输出格式、筛选条件等

⚠️ 注意事项:
   - 确保数据库连接正常
   - 首次运行可能需要较长时间
   - 建议在调试模式下先测试
"""