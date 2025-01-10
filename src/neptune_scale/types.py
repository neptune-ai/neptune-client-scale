from typing import (
    Callable,
    Literal,
    Optional,
)

RunMode = Literal["async", "offline", "disabled"]
RunCallback = Callable[[BaseException, Optional[float]], None]
