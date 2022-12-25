import os

ROOT_DIR = os.path.dirname(__file__)
# 日志文件夹
LOG_PATH = os.path.join(ROOT_DIR, 'log')
os.makedirs(LOG_PATH, exist_ok=True)

ES_HOST = {"host": "", "port": 8710}

INDEX_VIDEO = ""  # 影视媒资库索引
INDEX_VIDEO_WEB = ""  # 从网络中获取的影视名等信息
INDEX_NO_MEANING_WORDS = ""  # 无意义词库
