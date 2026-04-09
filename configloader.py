import sys
import os
import toml  

class config:
    def __init__(self):
        self.config_path = ""
        self.config_data = {}
        self.load_config()

    def get_resource_path(self, filename):
        """
        1. 如果是打包后的 exe，PyInstaller 会把 sys._MEIPASS 设置为 _internal 的路径。
        2. 如果是普通运行，就使用当前文件所在目录。
        """
        if getattr(sys, 'frozen', False):
            # sys._MEIPASS 指向 _internal 临时目录
            base_path = sys._MEIPASS 
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        
        return os.path.join(base_path, filename)

    def load_config(self):
        # 这里指定要读取的文件名
        self.config_path = self.get_resource_path("config.toml")

        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self.config_data = toml.load(f)
            except Exception as e:
                print(f"ERROR: 读取配置文件失败: {e}")
        else:
            # 如果内部找不到，尝试在 EXE 同级目录找一下（作为备选方案）
            if getattr(sys, 'frozen', False):
                 exe_dir_path = os.path.join(os.path.dirname(sys.executable), "config.toml")
                 if os.path.exists(exe_dir_path):
                     try:
                        with open(exe_dir_path, 'r', encoding='utf-8') as f:
                            self.config_data = toml.load(f)
                        return
                     except:
                         pass
            
            print(f"CRITICAL ERROR: 找不到配置文件 {self.config_path}")

    def get_key(self, key):
        val = self.config_data.get(key)
        if val is None:
            print(f"WARNING: 键值 '{key}' 未在配置中找到，返回 None")
        return val