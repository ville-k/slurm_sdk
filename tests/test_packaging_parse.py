from slurm import task


def test_packaging_container_string_does_not_set_image():
    def my_func() -> int:
        return 1

    t = task(my_func, packaging="container", packaging_dockerfile="Dockerfile")
    assert t.packaging is not None
    assert t.packaging["type"] == "container"
    assert "image" not in t.packaging


def test_packaging_container_image_string_sets_image():
    def my_func() -> int:
        return 1

    t = task(my_func, packaging="container:alpine:3.19")
    assert t.packaging is not None
    assert t.packaging["type"] == "container"
    assert t.packaging["image"] == "alpine:3.19"
