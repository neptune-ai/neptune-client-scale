from typing import (
    Callable,
    Optional,
)

RunCallback = Callable[[BaseException, Optional[float]], None]
