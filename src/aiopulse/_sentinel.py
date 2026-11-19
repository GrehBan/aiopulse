from __future__ import annotations

from enum import Enum, auto
from typing import Final, final


@final
class _Sentinel(Enum):
    TOKEN = auto()


MISSING: Final[_Sentinel] = _Sentinel.TOKEN
