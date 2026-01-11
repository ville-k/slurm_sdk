"""Array items serialization and loading for native SLURM arrays."""

# nosec B403 - pickle required for serializing/deserializing array job items
import pickle
from pathlib import Path
from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .job import Job


ARRAY_ITEMS_FILENAME = "array_items.pkl"


def convert_job_items_to_placeholders(
    items: List[Any],
) -> Tuple[List[Any], List["Job"]]:
    """Convert Job objects in array items to JobResultPlaceholder instances.

    Recursively processes items to find and replace Job objects with placeholders
    that can be pickled. Also collects all found Jobs for dependency tracking.

    Args:
        items: List of array items (may contain Jobs, tuples with Jobs, dicts with Jobs)

    Returns:
        Tuple of (converted_items, found_jobs) where:
        - converted_items: Items with Jobs replaced by placeholders
        - found_jobs: List of all Job objects found (for dependency tracking)

    Examples:
        >>> job1 = Job(id="123", ...)
        >>> items = [job1, (job2, "arg"), {"data": job3, "x": 1}]
        >>> converted, deps = convert_job_items_to_placeholders(items)
        >>> # converted = [JobResultPlaceholder("123"),
        >>>              (JobResultPlaceholder("456"), "arg"),
        >>>              {"data": JobResultPlaceholder("789"), "x": 1}]
        >>> # deps = [job1, job2, job3]
    """
    from .job import Job
    from .task import JobResultPlaceholder

    found_jobs: List[Job] = []
    converted_items: List[Any] = []

    def convert_value(value: Any) -> Any:
        if isinstance(value, Job):
            found_jobs.append(value)
            return JobResultPlaceholder(value.id)
        elif isinstance(value, tuple):
            return tuple(convert_value(v) for v in value)
        elif isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [convert_value(v) for v in value]
        else:
            return value

    for item in items:
        converted_items.append(convert_value(item))

    return converted_items, found_jobs


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
    # nosec B301 - array items file created by SDK in serialize_array_items()
    with open(array_items_file, "rb") as f:
        array_data = pickle.load(f)  # nosec B301

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
