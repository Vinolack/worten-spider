#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Worten爬虫GUI界面
提供图形化界面控制爬虫程序，包含进度跟踪和速率监控
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import subprocess
import threading
import time
import queue
import os
import sys
import multiprocessing
from datetime import datetime
import logging
from typing import Optional

# 导入爬虫模块
try:
    import price_check
except ImportError:
    messagebox.showerror("导入错误", "无法导入price_check模块，请确保文件在同一目录下")
    sys.exit(1)


class WortenScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Worten爬虫价格检查控制台 v1.0")
        self.root.geometry("600x500")
        self.root.resizable(True, True)
        
        # 设置窗口图标（如果有的话）
        try:
            self.root.iconbitmap("icon.ico")
        except:
            pass
        
        # 状态变量
        self.is_running = False
        self.scraper_thread = None
        self.process = None
        self.progress_queue = queue.Queue()
        self.start_time = None
        self.processed_count = 0
        self.total_count = 0
        
        # 创建界面
        self.create_widgets()
        
        # 启动进度更新定时器
        self.update_progress()
        
        # 配置日志
        self.setup_logging()
        
    def setup_logging(self):
        """配置GUI日志显示"""
        # 不创建默认日志文件，只在需要时创建
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[],
            force=True
        )
        
    def setup_error_logging(self):
        """设置错误日志路径"""
        # 获取输出目录（与xlsx文件相同目录）
        output_dir = os.path.dirname(os.path.abspath(self.output_file_var.get())) if self.output_file_var.get() else os.getcwd()
        
        # 创建带时间戳的错误日志文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        error_log_path = os.path.join(output_dir, f"error_{timestamp}.log")
        
        # 清除现有处理器并添加新的文件处理器
        logging.getLogger().handlers.clear()
        file_handler = logging.FileHandler(error_log_path, encoding='utf-8')
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().setLevel(logging.ERROR)
        
    def create_widgets(self):
        """创建GUI组件"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # 文件选择区域
        file_frame = ttk.LabelFrame(main_frame, text="文件设置", padding="10")
        file_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        file_frame.columnconfigure(1, weight=1)
        
        ttk.Label(file_frame, text="输入文件:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.input_file_var = tk.StringVar(value="price_check_links.xlsx")
        self.input_entry = ttk.Entry(file_frame, textvariable=self.input_file_var, width=50)
        self.input_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 5))
        ttk.Button(file_frame, text="浏览", command=self.browse_input_file).grid(row=0, column=2)
        
        ttk.Label(file_frame, text="输出文件:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5), pady=(5, 0))
        self.output_file_var = tk.StringVar()
        self.output_entry = ttk.Entry(file_frame, textvariable=self.output_file_var, width=50)
        self.output_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 5), pady=(5, 0))
        ttk.Button(file_frame, text="打开", command=self.open_output_file).grid(row=1, column=2, pady=(5, 0))
        
        # 控制按钮区域
        control_frame = ttk.LabelFrame(main_frame, text="控制面板", padding="10")
        control_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        self.start_button = ttk.Button(control_frame, text="开始爬取", command=self.start_scraping, style="Accent.TButton")
        self.start_button.grid(row=0, column=0, padx=(0, 10))
        
        self.stop_button = ttk.Button(control_frame, text="停止爬取", command=self.stop_scraping, state=tk.DISABLED)
        self.stop_button.grid(row=0, column=1, padx=(0, 10))
        
        # 状态信息区域
        status_frame = ttk.LabelFrame(main_frame, text="运行状态", padding="10")
        status_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        status_frame.columnconfigure(1, weight=1)
        
        # 进度条
        ttk.Label(status_frame, text="进度:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.progress_var = tk.StringVar(value="准备就绪")
        self.progress_label = ttk.Label(status_frame, textvariable=self.progress_var)
        self.progress_label.grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        
        self.progress_bar = ttk.Progressbar(status_frame, mode='determinate', length=400)
        self.progress_bar.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(5, 0))
        
        # 统计信息
        stats_frame = ttk.Frame(status_frame)
        stats_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 0))
        stats_frame.columnconfigure(1, weight=1)
        
        ttk.Label(stats_frame, text="已处理:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        self.processed_var = tk.StringVar(value="0")
        ttk.Label(stats_frame, textvariable=self.processed_var, font=('Arial', 10, 'bold')).grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        
        ttk.Label(stats_frame, text="总任务:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        self.total_var = tk.StringVar(value="0")
        ttk.Label(stats_frame, textvariable=self.total_var, font=('Arial', 10, 'bold')).grid(row=0, column=3, sticky=tk.W, padx=(0, 20))
        
        ttk.Label(stats_frame, text="处理速率:").grid(row=0, column=4, sticky=tk.W, padx=(0, 5))
        self.rate_var = tk.StringVar(value="0.0/分钟")
        ttk.Label(stats_frame, textvariable=self.rate_var, font=('Arial', 10, 'bold')).grid(row=0, column=5, sticky=tk.W)
        
        # 状态栏
        self.status_bar = ttk.Label(main_frame, text="就绪", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E))
        
    def browse_input_file(self):
        """浏览选择输入文件"""
        filename = filedialog.askopenfilename(
            title="选择输入Excel文件",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if filename:
            self.input_file_var.set(filename)
            
    def browse_output_file(self):
        """浏览选择输出文件"""
        filename = filedialog.asksaveasfilename(
            title="选择输出Excel文件",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if filename:
            self.output_file_var.set(filename)
            
    def open_output_file(self):
        """打开输出文件"""
        filename = self.output_file_var.get().strip()
        if not filename:
            messagebox.showwarning("警告", "请先选择输出文件")
            return
            
        if not os.path.exists(filename):
            messagebox.showwarning("警告", f"文件不存在: {filename}")
            return
            
        try:
            # 根据操作系统选择打开方式
            if os.name == 'nt':  # Windows
                os.startfile(filename)
            elif os.name == 'posix':  # macOS and Linux
                os.system(f'open "{filename}"' if sys.platform == 'darwin' else f'xdg-open "{filename}"')
            else:
                messagebox.showinfo("信息", f"文件路径: {filename}")
        except Exception as e:
            messagebox.showerror("错误", f"无法打开文件: {str(e)}")
            
    def start_scraping(self):
        """开始爬取"""
        if self.is_running:
            return
            
        # 检查输入文件
        input_file = self.input_file_var.get().strip()
        if not input_file or not os.path.exists(input_file):
            messagebox.showerror("错误", "请选择有效的输入文件")
            return
            
        # 设置输出文件
        if not self.output_file_var.get().strip():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            default_output = f"worten_price_data_{timestamp}.xlsx"
            self.output_file_var.set(default_output)
            
        # 设置错误日志路径
        self.setup_error_logging()
            
        # 更新界面状态
        self.is_running = True
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.status_bar.config(text="爬取进行中...")
        
        # 重置计数器
        self.processed_count = 0
        self.total_count = 0
        self.start_time = time.time()
        
        # 开始爬取任务（记录到错误日志文件）
        logging.info("开始爬取任务...")
        
        # 在新线程中启动爬虫
        self.scraper_thread = threading.Thread(target=self.run_scraper, daemon=True)
        self.scraper_thread.start()
        
    def stop_scraping(self):
        """停止爬取"""
        if not self.is_running:
            return
            
        self.is_running = False
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.status_bar.config(text="正在停止...")
        
        # 终止正在运行的爬虫子进程
        if self.process and self.process.poll() is None:
            logging.warning("检测到正在运行的爬虫进程，正在终止...")
            try:
                # 先尝试优雅关闭
                self.process.terminate()
                # 等待最多5秒
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 如果优雅关闭失败，强制杀死进程
                    logging.warning("爬虫进程未能在5秒内关闭，强制终止...")
                    self.process.kill()
                    self.process.wait()
            except Exception as e:
                logging.error(f"终止爬虫进程时出错: {e}")
        
        # 等待爬虫线程完成（最多等待2秒）
        if self.scraper_thread and self.scraper_thread.is_alive():
            logging.info("等待爬虫线程完成...")
            self.scraper_thread.join(timeout=2)
        
        # 正在停止爬取任务（记录到错误日志）
        logging.info("正在停止爬取任务...")
        
    def run_scraper(self):
        """在独立线程中运行爬虫"""
        try:
            # 设置环境变量
            os.environ['WORTEN_INPUT_FILE'] = self.input_file_var.get()
            os.environ['WORTEN_OUTPUT_FILE'] = self.output_file_var.get()
            
            # 定义进度回调函数
            def progress_callback(progress_data):
                # 即使在停止状态下也允许更新最终状态
                if not self.is_running and progress_data.get('message', '').find('完成') == -1:
                    return
                try:
                    self.progress_queue.put(progress_data)
                    logging.debug(f"进度更新: {progress_data}")
                except Exception as e:
                    logging.error(f"进度回调错误: {e}")
            
            # 定义停止检查回调函数
            def stop_check_callback():
                return not self.is_running
            
            # 启动爬虫，传递进度回调和停止检查回调
            price_check.main(progress_callback=progress_callback, stop_check_callback=stop_check_callback)
            
        except Exception as e:
            # 记录错误到日志文件
            logging.error(f"爬取过程中发生错误: {str(e)}")
            messagebox.showerror("错误", f"爬取失败: {str(e)}")
        finally:
            self.is_running = False
            self.root.after(0, self.scraping_finished)
            
    def simulate_progress(self):
        """模拟进度更新（临时用于测试GUI）"""
        import random
        
        for i in range(100):
            if not self.is_running:
                break
                
            # 模拟处理
            time.sleep(0.1)
            
            # 更新进度
            self.processed_count += 1
            self.total_count = 100  # 假设总共100个任务
            
            # 计算处理速率
            elapsed_time = time.time() - self.start_time
            rate = self.processed_count / (elapsed_time / 60) if elapsed_time > 0 else 0
            
            # 发送进度更新
            self.progress_queue.put({
                'processed': self.processed_count,
                'total': self.total_count,
                'rate': rate,
                'message': f"正在处理第 {self.processed_count} 个任务..."
            })
            
    def scraping_finished(self):
        """爬取完成回调"""
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        self.status_bar.config(text=f"爬取完成，耗时 {elapsed_time:.1f} 秒")
        try:
            progress_data = self.progress_queue.get_nowait()
            processed = self.processed_count
            # 如果后端传了 rate 且不为 0，则更新；否则保持界面上最后的速率
            rate = progress_data.get('rate', 0)
            if rate > 0:
                self.rate_var.set(f"{rate:.1f}/分钟")
        except queue.Empty:
            processed = self.processed_count
        # 记录任务完成（只记录到错误日志文件）
        logging.info("爬取任务完成！")
        
        # 显示完成消息
        messagebox.showinfo("完成", f"爬取完成！\n已处理: {processed} 个任务\n耗时: {elapsed_time:.1f} 秒")
        
    def update_progress(self):
        """更新进度显示"""
        try:
            while True:
                try:
                    progress_data = self.progress_queue.get_nowait()
                    
                    # 更新进度条
                    self.processed_count = progress_data.get('processed', 0)
                    self.total_count = progress_data.get('total', 0)
                    rate = progress_data.get('rate', 0)
                    message = progress_data.get('message', '')
                    
                    # 更新GUI变量
                    self.processed_var.set(str(self.processed_count))
                    self.total_var.set(str(self.total_count))
                    self.rate_var.set(f"{rate:.1f}/分钟")
                    
                    # 更新进度条和状态文本
                    if self.total_count > 0:
                        percentage = min((self.processed_count / self.total_count) * 100, 100)  # 确保不超过100%
                        self.progress_bar['value'] = percentage
                        self.progress_var.set(f"{percentage:.1f}% - {message}")
                    else:
                        self.progress_var.set(message)
                    
                    # 更新状态栏
                    if self.processed_count > 0:
                        elapsed_time = time.time() - self.start_time if self.start_time else 0
                        if elapsed_time > 0:
                            self.status_bar.config(text=f"处理中... 已处理 {self.processed_count} 个任务，耗时 {elapsed_time:.1f} 秒")
                    
                except queue.Empty:
                    break
                    
        except Exception as e:
            logging.error(f"进度更新错误: {e}")
            
        # 继续定时更新
        self.root.after(100, self.update_progress)
        
    def on_closing(self):
        """窗口关闭事件"""
        if self.is_running:
            if messagebox.askokcancel("退出", "爬取正在进行中，确定要退出吗？"):
                self.stop_scraping()
                self.root.destroy()
        else:
            self.root.destroy()


def main():
    """主函数"""
    multiprocessing.freeze_support()
    root = tk.Tk()
    app = WortenScraperGUI(root)
    
    # 设置窗口关闭事件
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    # 启动GUI
    root.mainloop()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    
    # 如果你的爬虫使用了 spawn 模式（Windows默认）
    # 有时候显式设置启动模式能解决一些奇怪的路径问题
    try:
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        pass
    main()