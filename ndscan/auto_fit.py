import logging
import oitg.fitting

from .parameters import ParamHandle
from .result_channels import ResultChannel
from typing import Callable, Dict, List, Tuple, Union


logger = logging.getLogger(__name__)


FIT_OBJECTS = {
    n: getattr(oitg.fitting, n) for n in ["lorentzian", "parabola", "rabi_flop"]
}


class AutoFitSpec:
    def __init__(self, fit_type: str, data: Dict[str, Union[ParamHandle, ResultChannel]], display_hints: Dict[str, dict] = {}):
        self.fit_type = fit_type
        if fit_type not in FIT_OBJECTS:
            logger.warning("Unknown fit type: '%s'", fit_type, exc_info=True)
        self.data = data
        self.display_hints = display_hints

    def has_data(self, scanned_axes: List[Tuple[str, str]]):
        for arg in self.data.values():
            if isinstance(arg, ParamHandle):
                if not arg._store:
                    return False
                if not arg._store.identity in scanned_axes:
                    return False
        return True

    def describe(
        self,
        get_axis_name: Callable[[Tuple[str, str]], str],
        get_channel_name: Callable[[str], str]
    ) -> Dict[str, any]:
        def describe_argument(obj):
            if isinstance(obj, ParamHandle):
                return get_axis_name(obj._store.identity)
            elif isinstance(obj, ResultChannel):
                return get_channel_name(obj.path)
            else:
                raise ValueError("Invalid fit argument source: {}".format(obj))
        return {
            "fit_type": self.fit_type,
            "data": {name: describe_argument(obj) for name, obj in self.data.items()},
            "display_hints": self.display_hints
        }
