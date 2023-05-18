"""
开发dev/测试test/生产prod环境
"""
import os
import sys
import yaml
from common.log_utils import get_logger
from conf.path_config import project_dir


logger = get_logger(__name__)

# window平台采用默认参数
if sys.platform.startswith('win'):
    ENV = 'dev'  # 内网开发环境
    # ENV = 'test'   # 内网alpha环境
else:
    # 读取系统中的linux系统中ENV值  该值通过docker-compose.yaml的environment或 docker run -e ENV='dev'指定
    ENV = os.environ.get('ENV', 'dev')

config_file = os.path.join(project_dir, f'conf/config_{ENV}.yaml')
logger.info(f"load config: config_{ENV}.yaml")

with open(config_file, encoding='utf-8') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

PostgresqlConfig = config['postgresql']
TDengineConfig = config['tdengine']
