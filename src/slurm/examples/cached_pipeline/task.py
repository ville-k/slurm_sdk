from __future__ import annotations

import logging
from typing import Any, Dict

from slurm.logging import configure_logging

from slurm.examples.cached_pipeline.decorators import cached_task


@cached_task()
def build_feature_summary(dataset: str, epoch: int, scale: int = 1) -> Dict[str, Any]:
    """Build a deterministic feature summary for replay/caching demonstrations."""
    configure_logging()
    logger = logging.getLogger(__name__)

    token_sum = sum(ord(char) for char in dataset)
    raw_score = ((token_sum + (epoch * 17)) % 10_000) / 10_000.0
    score = round(raw_score * max(scale, 1), 6)

    logger.info(
        "Computed feature summary for dataset=%s epoch=%s scale=%s score=%s",
        dataset,
        epoch,
        scale,
        score,
    )

    return {
        "dataset": dataset,
        "epoch": epoch,
        "scale": scale,
        "token_sum": token_sum,
        "score": score,
    }
