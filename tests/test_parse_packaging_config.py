"""Tests for the public parse_packaging_config function."""

from slurm.decorators import parse_packaging_config


def test_auto():
    result = parse_packaging_config("auto")
    assert result == {"type": "auto"}


def test_wheel():
    result = parse_packaging_config("wheel")
    assert result == {"type": "wheel"}


def test_none():
    result = parse_packaging_config("none")
    assert result == {"type": "none"}


def test_inherit():
    result = parse_packaging_config("inherit")
    assert result == {"type": "inherit"}


def test_container_bare():
    result = parse_packaging_config("container")
    assert result == {"type": "container"}


def test_container_with_image():
    result = parse_packaging_config("container:nvcr.io/nvidia/pytorch:24.01-py3")
    assert result == {
        "type": "container",
        "image": "nvcr.io/nvidia/pytorch:24.01-py3",
    }


def test_raw_image_reference():
    result = parse_packaging_config("python:3.11")
    assert result == {"type": "container", "image": "python:3.11"}


def test_with_kwargs():
    result = parse_packaging_config(
        "wheel", {"registry": "my-registry.com", "platform": "linux/amd64"}
    )
    assert result == {
        "type": "wheel",
        "registry": "my-registry.com",
        "platform": "linux/amd64",
    }


def test_none_input():
    result = parse_packaging_config("")
    assert result is None


def test_kwargs_default_to_empty():
    """parse_packaging_config works without kwargs argument."""
    result = parse_packaging_config("auto")
    assert result == {"type": "auto"}


def test_backward_compat_alias():
    """The private _parse_packaging_config still works."""
    from slurm.decorators import _parse_packaging_config

    result = _parse_packaging_config("auto", {})
    assert result == {"type": "auto"}
