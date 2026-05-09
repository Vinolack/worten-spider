#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unified Worten scraper GUI."""

import logging
import multiprocessing
import os
import queue
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

import configloader
import more_seller
import price_check
import product_info
from state_store import default_state_db

GUI_CONFIG = configloader.config()
LOG_LEVEL = configloader.get_log_level(GUI_CONFIG)


MODES = {
    "product_info": {
        "label": "商品信息抓取",
        "module": product_info,
        "output_prefix": "worten_data",
        "help": "功能说明：抓取商品基础信息，适合处理商品页、店铺页、类目页或列表页。\n输入说明：input_links.xlsx 必须包含 url 列；如 url 是店铺/类目/列表链接，可选填 pages_to_scrape 指定抓取页数。",
    },
    "price_check": {
        "label": "价格检查",
        "module": price_check,
        "output_prefix": "worten_price_data",
        "help": "功能说明：检查链接中的商品价格与销售状态，输出价格核对结果。\n输入说明：input_links.xlsx 必须包含 url 列；如 url 是店铺/类目/列表链接，可选填 pages_to_scrape 指定抓取页数。",
    },
    "more_seller": {
        "label": "跟卖信息抓取",
        "module": more_seller,
        "output_prefix": "worten_seller_data",
        "help": "功能说明：抓取商品页中的更多卖家/跟卖信息。\n输入说明：input_links.xlsx 必须包含 url 列；url 应填写商品链接，本功能不处理店铺/类目/列表页。",
    },
}


class WortenScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Worten爬虫控制台 v3")
        self.root.geometry("720x560")
        self.root.resizable(True, True)

        try:
            self.root.iconbitmap("icon.ico")
        except Exception:
            pass

        self.is_running = False
        self.scraper_thread = None
        self.progress_queue = queue.Queue()
        self.start_time = None
        self.last_result = None
        self.last_error = None

        self.mode_var = tk.StringVar(value="product_info")
        self.input_file_var = tk.StringVar(value="input_links.xlsx")
        self.output_file_var = tk.StringVar()
        self.progress_var = tk.StringVar(value="准备就绪")
        self.processed_var = tk.StringVar(value="0")
        self.total_var = tk.StringVar(value="0")
        self.rate_var = tk.StringVar(value="0.0/分钟")

        self.create_widgets()
        self.setup_logging()
        self.update_progress()

    def setup_logging(self):
        logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[], force=True)

    def setup_error_logging(self):
        output_dir = os.path.dirname(os.path.abspath(self.output_file_var.get())) if self.output_file_var.get() else os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        error_log_path = os.path.join(output_dir, f"error_{timestamp}.log")
        logging.getLogger().handlers.clear()
        file_handler = logging.FileHandler(error_log_path, encoding="utf-8")
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().setLevel(LOG_LEVEL)

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)

        mode_frame = ttk.LabelFrame(main_frame, text="功能选择", padding="10")
        mode_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        mode_frame.columnconfigure(1, weight=1)
        ttk.Label(mode_frame, text="功能:").grid(row=0, column=0, sticky=tk.W, padx=(0, 8))
        mode_combo = ttk.Combobox(mode_frame, state="readonly", textvariable=self.mode_var, values=list(MODES.keys()), width=22)
        mode_combo.grid(row=0, column=1, sticky=tk.W)
        mode_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_default_output())
        self.mode_label_var = tk.StringVar(value=MODES[self.mode_var.get()]["label"])
        ttk.Label(mode_frame, textvariable=self.mode_label_var).grid(row=0, column=2, sticky=tk.W, padx=(16, 0))
        self.mode_help_var = tk.StringVar(value=MODES[self.mode_var.get()]["help"])
        ttk.Label(mode_frame, textvariable=self.mode_help_var, justify=tk.LEFT, wraplength=660).grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(8, 0))

        file_frame = ttk.LabelFrame(main_frame, text="文件设置", padding="10")
        file_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        file_frame.columnconfigure(1, weight=1)

        ttk.Label(file_frame, text="输入文件:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        ttk.Entry(file_frame, textvariable=self.input_file_var, width=60).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 5))
        ttk.Button(file_frame, text="浏览", command=self.browse_input_file).grid(row=0, column=2)

        ttk.Label(file_frame, text="输出文件:").grid(row=1, column=0, sticky=tk.W, padx=(0, 5), pady=(5, 0))
        ttk.Entry(file_frame, textvariable=self.output_file_var, width=60).grid(row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 5), pady=(5, 0))
        output_buttons = ttk.Frame(file_frame)
        output_buttons.grid(row=1, column=2, pady=(5, 0))
        ttk.Button(output_buttons, text="选择", command=self.browse_output_file).grid(row=0, column=0, padx=(0, 4))
        ttk.Button(output_buttons, text="打开", command=self.open_output_file).grid(row=0, column=1)

        control_frame = ttk.LabelFrame(main_frame, text="控制面板", padding="10")
        control_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        self.start_button = ttk.Button(control_frame, text="开始/继续任务", command=self.start_scraping)
        self.start_button.grid(row=0, column=0, padx=(0, 10))
        self.stop_button = ttk.Button(control_frame, text="停止", command=self.stop_scraping, state=tk.DISABLED)
        self.stop_button.grid(row=0, column=1, padx=(0, 10))

        status_frame = ttk.LabelFrame(main_frame, text="运行状态", padding="10")
        status_frame.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        status_frame.columnconfigure(1, weight=1)
        ttk.Label(status_frame, text="进度:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        ttk.Label(status_frame, textvariable=self.progress_var).grid(row=0, column=1, sticky=tk.W, padx=(0, 10))
        self.progress_bar = ttk.Progressbar(status_frame, mode="determinate", length=520)
        self.progress_bar.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(5, 0))

        stats_frame = ttk.Frame(status_frame)
        stats_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(10, 0))
        ttk.Label(stats_frame, text="已处理:").grid(row=0, column=0, sticky=tk.W, padx=(0, 5))
        ttk.Label(stats_frame, textvariable=self.processed_var, font=("Arial", 10, "bold")).grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        ttk.Label(stats_frame, text="总任务:").grid(row=0, column=2, sticky=tk.W, padx=(0, 5))
        ttk.Label(stats_frame, textvariable=self.total_var, font=("Arial", 10, "bold")).grid(row=0, column=3, sticky=tk.W, padx=(0, 20))
        ttk.Label(stats_frame, text="处理速率:").grid(row=0, column=4, sticky=tk.W, padx=(0, 5))
        ttk.Label(stats_frame, textvariable=self.rate_var, font=("Arial", 10, "bold")).grid(row=0, column=5, sticky=tk.W)

        self.status_bar = ttk.Label(main_frame, text="就绪", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.grid(row=4, column=0, sticky=(tk.W, tk.E))
        self.refresh_default_output()

    def refresh_default_output(self):
        mode = self.mode_var.get()
        self.mode_label_var.set(MODES[mode]["label"])
        self.mode_help_var.set(MODES[mode]["help"])
        if not self.is_running:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            self.output_file_var.set(f"{MODES[mode]['output_prefix']}_{timestamp}.xlsx")

    def browse_input_file(self):
        filename = filedialog.askopenfilename(title="选择输入Excel文件", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
        if filename:
            self.input_file_var.set(filename)

    def browse_output_file(self):
        filename = filedialog.asksaveasfilename(title="选择输出Excel文件", defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
        if filename:
            self.output_file_var.set(filename)

    def open_output_file(self):
        filename = self.output_file_var.get().strip()
        if not filename or not os.path.exists(filename):
            messagebox.showwarning("警告", f"文件不存在: {filename or '未选择'}")
            return
        try:
            if os.name == "nt":
                os.startfile(filename)
            elif sys.platform == "darwin":
                os.system(f'open "{filename}"')
            else:
                os.system(f'xdg-open "{filename}"')
        except Exception as exc:
            messagebox.showerror("错误", f"无法打开文件: {exc}")

    def validate_inputs(self):
        input_file = self.input_file_var.get().strip()
        output_file = self.output_file_var.get().strip()
        if not input_file or not os.path.exists(input_file):
            messagebox.showerror("错误", "请选择有效的输入文件")
            return None
        if not output_file:
            self.refresh_default_output()
            output_file = self.output_file_var.get().strip()
        if not output_file.lower().endswith(".xlsx"):
            messagebox.showerror("错误", "输出文件必须是 .xlsx")
            return None
        output_dir = os.path.dirname(os.path.abspath(output_file)) or os.getcwd()
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as exc:
            messagebox.showerror("错误", f"输出目录不可用: {exc}")
            return None
        return input_file, output_file

    def start_scraping(self):
        if self.is_running:
            return
        paths = self.validate_inputs()
        if not paths:
            return
        self.setup_error_logging()
        self.is_running = True
        self.last_result = None
        self.last_error = None
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.status_bar.config(text="任务运行中...")
        self.progress_bar["value"] = 0
        self.processed_var.set("0")
        self.total_var.set("0")
        self.rate_var.set("0.0/分钟")
        self.start_time = time.time()
        self.scraper_thread = threading.Thread(target=self.run_scraper, args=paths, daemon=True)
        self.scraper_thread.start()

    def stop_scraping(self):
        if not self.is_running:
            return
        self.is_running = False
        self.stop_button.config(state=tk.DISABLED)
        self.status_bar.config(text="正在停止，当前任务状态已保存在数据库中...")

    def run_scraper(self, input_file, output_file):
        mode = self.mode_var.get()
        module = MODES[mode]["module"]
        state_db_path = default_state_db(os.path.dirname(os.path.abspath(output_file)) or os.getcwd())

        def progress_callback(progress_data):
            self.progress_queue.put(progress_data)

        def stop_check_callback():
            return not self.is_running

        try:
            self.last_result = module.main(
                progress_callback=progress_callback,
                stop_check_callback=stop_check_callback,
                input_file=input_file,
                output_file=output_file,
                state_db_path=state_db_path,
            )
        except Exception as exc:
            logging.exception("爬取失败")
            self.last_error = exc
        finally:
            self.is_running = False
            self.root.after(0, self.scraping_finished)

    def scraping_finished(self):
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        elapsed = time.time() - self.start_time if self.start_time else 0
        if self.last_error:
            self.status_bar.config(text=f"任务失败，耗时 {elapsed:.1f} 秒")
            messagebox.showerror("失败", f"任务失败: {self.last_error}\n已完成结果保存在状态数据库中，可再次点击开始/继续任务。")
            return
        self.status_bar.config(text=f"任务结束，耗时 {elapsed:.1f} 秒")
        processed = self.processed_var.get()
        messagebox.showinfo("完成", f"任务结束！\n已处理: {processed} 个任务\n耗时: {elapsed:.1f} 秒")

    def update_progress(self):
        try:
            while True:
                progress_data = self.progress_queue.get_nowait()
                processed = int(progress_data.get("processed", 0) or 0)
                total = int(progress_data.get("total", 0) or 0)
                rate = float(progress_data.get("rate", 0) or 0)
                message = progress_data.get("message", "")
                self.processed_var.set(str(processed))
                self.total_var.set(str(total))
                self.rate_var.set(f"{rate:.1f}/分钟")
                if total > 0:
                    percentage = min((processed / total) * 100, 100)
                    self.progress_bar["value"] = percentage
                    self.progress_var.set(f"{percentage:.1f}% - {message}")
                else:
                    self.progress_var.set(message)
        except queue.Empty:
            pass
        except Exception as exc:
            logging.error(f"进度更新错误: {exc}")
        self.root.after(100, self.update_progress)

    def on_closing(self):
        if self.is_running:
            if not messagebox.askokcancel("退出", "任务正在运行，退出后可再次打开程序继续未完成任务。确定退出吗？"):
                return
            self.stop_scraping()
        self.root.destroy()


def main():
    multiprocessing.freeze_support()
    root = tk.Tk()
    app = WortenScraperGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    main()
