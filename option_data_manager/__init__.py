"""Import shim for local uv runs from Unicode workspace paths.

The production package lives in ``src/option_data_manager``. This shim keeps
``uv run`` and console entry points importable when editable-install path files
are mangled by the host path encoding.
"""

from __future__ import annotations

from pathlib import Path

_SOURCE_PACKAGE = Path(__file__).resolve().parent.parent / "src" / "option_data_manager"
__path__ = [str(_SOURCE_PACKAGE)]

_source_init = _SOURCE_PACKAGE / "__init__.py"
exec(compile(_source_init.read_text(encoding="utf-8"), str(_source_init), "exec"))

