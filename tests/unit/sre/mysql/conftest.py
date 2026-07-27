import importlib.util
from pathlib import Path


def load_cookbook(fname: str | Path):
    """Load cookbook with dash in its name"""
    return importlib.import_module("cookbooks.sre.mysql.update-replication")

    # This allows running unit tests but does not solve the problem that mypy is unable to
    # infer type hints across cookbooks and unit tests
    path = Path(fname)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if not spec or not spec.loader:
        raise ImportError(f"Unable to load {fname}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
