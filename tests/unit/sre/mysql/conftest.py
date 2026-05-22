import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def load_cookbook():

    def _loader(fname: str | Path):
        path = Path(fname)
        spec = importlib.util.spec_from_file_location(path.stem.replace("-", "_"), path)
        if not spec or not spec.loader:
            raise ImportError(f"Unable to load {fname}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return _loader
