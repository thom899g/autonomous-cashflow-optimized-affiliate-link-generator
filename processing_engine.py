from flink import StreamExecutionEnvironment
from typing import Dict, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing-engine.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__