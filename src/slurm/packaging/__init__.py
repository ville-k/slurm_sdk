"""
Packaging strategies for SLURM jobs.
"""

from typing import Any, Dict, Optional

from .base import PackagingStrategy
from .none import NonePackagingStrategy
from .container import ContainerPackagingStrategy
from .wheel import WheelPackagingStrategy


_STRATEGIES = {
    "wheel": WheelPackagingStrategy,
    "none": NonePackagingStrategy,
    "container": ContainerPackagingStrategy,
}


def get_packaging_strategy(config: Optional[Dict[str, Any]]) -> PackagingStrategy:
    """
    Get the packaging strategy based on the configuration.

    Args:
        config: Packaging configuration dictionary.
                Expected keys: 'type' (e.g., 'wheel', 'none').

    Returns:
        An instance of the appropriate PackagingStrategy subclass.
        Defaults to NonePackagingStrategy if config is None or type is missing/invalid.
    """
    if config is None:
        return NonePackagingStrategy()

    strategy_type = config.get("type")
    if not strategy_type:
        return NonePackagingStrategy()

    strategy_class = _STRATEGIES.get(strategy_type)
    if strategy_class:
        return strategy_class(config)
    else:
        return NonePackagingStrategy()


__all__ = [
    "get_packaging_strategy",
    "PackagingStrategy",
    "WheelPackagingStrategy",
    "NonePackagingStrategy",
    "ContainerPackagingStrategy",
]
