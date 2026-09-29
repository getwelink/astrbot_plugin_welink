"""Entry point.

A platform adapter registers itself through a decorator, so the only thing
this file has to do is make sure the module carrying that decorator is
imported. The Star class exists because AstrBot loads plugins by looking for
one; it has no commands of its own.
"""

from astrbot.api import logger
from astrbot.api.star import Context, Star

# Importing runs @register_platform_adapter. Do not remove.
from . import welink_adapter  # noqa: F401


class Main(Star):
    def __init__(self, context: Context) -> None:
        self.context = context
        logger.info(
            "WeLink 微信个人号适配器已加载。请在「平台适配器」中新增 welink 类型，"
            "并填写 base_url 和 api_key。"
        )
