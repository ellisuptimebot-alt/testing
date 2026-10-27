# plugins — пакет с командами бота.
# Каждый модуль сам регистрирует хендлеры через @client.on при импорте.

import importlib
import pkgutil

from config import logger


def load_all():
    loaded = []
    for mod in pkgutil.iter_modules(__path__):
        if mod.name.startswith('_'):
            continue
        importlib.import_module(f'{__name__}.{mod.name}')
        loaded.append(mod.name)
    logger.info(f"[PLUGINS] загружено: {', '.join(sorted(loaded))} ({len(loaded)})")
    return loaded
