"""Array items serialization and loading for native SLURM arrays."""

import pickle
from pathlib import Path
from typing import Any, List, Optional


ARRAY_ITEMS_FILENAME = "array_items.pkl"


def serialize_array_items(
    items: List[Any],
    target_dir: str,
    max_concurrent: Optional[int] = None,
) -> str:
    """Serialize array items to a pickle file.

    Args:
        items: List of items to serialize (one per array element).
        target_dir: Target directory to write the file to.
        max_concurrent: Optional throttling parameter.

    Returns:
        Path to the created array items file (relative to target_dir).
    """
    array_data = {
        "items": items,
        "count": len(items),
        "max_concurrent": max_concurrent,
    }

    # Write to target directory
    target_path = Path(target_dir) / ARRAY_ITEMS_FILENAME
    target_path.parent.mkdir(parents=True, exist_ok=True)

    with open(target_path, "wb") as f:
        pickle.dump(array_data, f)

    return ARRAY_ITEMS_FILENAME


def load_array_item(
    array_items_file: str,
    array_index: int,
) -> Any:
    """Load a specific item from the array items file.

    Args:
        array_items_file: Path to the pickled array items file.
        array_index: Index of the item to load (0-based).

    Returns:
        The item at the specified index.

    Raises:
        FileNotFoundError: If the array items file doesn't exist.
        IndexError: If the array index is out of range.
    """
    with open(array_items_file, "rb") as f:
        array_data = pickle.load(f)

    items = array_data["items"]

    if array_index < 0 or array_index >= len(items):
        raise IndexError(f"Array index {array_index} out of range [0, {len(items)})")

    return items[array_index]


def generate_array_spec(
    num_items: int,
    max_concurrent: Optional[int] = None,
) -> str:
    """Generate SLURM array specification string.

    Args:
        num_items: Number of array elements (length of items list).
        max_concurrent: Optional max concurrent tasks for throttling.

    Returns:
        Array spec string in format "0-N" or "0-N%M".

    Examples:
        >>> generate_array_spec(100)
        '0-99'
        >>> generate_array_spec(100, max_concurrent=10)
        '0-99%10'
    """
    if num_items <= 0:
        raise ValueError("Number of items must be positive")

    array_spec = f"0-{num_items - 1}"

    if max_concurrent is not None and max_concurrent > 0:
        array_spec = f"{array_spec}%{max_concurrent}"

    return array_spec


__all__ = [
    "serialize_array_items",
    "load_array_item",
    "generate_array_spec",
    "ARRAY_ITEMS_FILENAME",
]
