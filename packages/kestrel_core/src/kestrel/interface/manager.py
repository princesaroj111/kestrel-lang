from __future__ import annotations

import importlib
import inspect
import itertools
import logging
import pkgutil
import sys
from copy import copy
from typing import Iterable, Mapping, Type

from kestrel.config.internal import CACHE_INTERFACE_IDENTIFIER
from kestrel.exceptions import (
    ConflictingInterfaceScheme,
    InterfaceNotConfigured,
    InterfaceNotFound,
    InvalidInterfaceImplementation,
)
from kestrel.interface.base import MODULE_PREFIX, AbstractInterface
from typeguard import typechecked

_logger = logging.getLogger(__name__)


# basically a scheme to interface mapping
@typechecked
class InterfaceManager(Mapping):
    def __init__(self, init_interfaces: Iterable[AbstractInterface] = []):
        interface_classes = _load_interface_classes()
        self.interfaces = list(init_interfaces)  # copy/recreate the list
        for iface_cls in interface_classes:
            try:
                iface = iface_cls()
                _logger.debug(f"Initialize interface {iface_cls.__name__}")
                self.interfaces.append(iface)
            except InterfaceNotConfigured as e:
                _logger.debug(f"Interface {iface_cls.__name__} not configured; ignored")

    def __getitem__(self, scheme: str) -> AbstractInterface:
        for interface in self.interfaces:
            if scheme in interface.schemes():
                return interface
        else:
            raise InterfaceNotFound(f"no interface loaded for scheme {scheme}")

    def __iter__(self) -> Iterable[str]:
        return filter(
            lambda x: x != CACHE_INTERFACE_IDENTIFIER,
            itertools.chain(*[i.schemes() for i in self.interfaces]),
        )

    def __len__(self) -> int:
        return sum(1 for _ in iter(self))

    def copy_with_virtual_cache(self) -> InterfaceManager:
        im = copy(self)
        # shallow copy refers to the same list, so create/copy a new one
        im.interfaces = copy(im.interfaces)
        # now swap in virtual cache
        cache = im[CACHE_INTERFACE_IDENTIFIER]
        im.interfaces.remove(cache)
        im.interfaces.append(cache.get_virtual_copy())
        return im

    def del_cache(self):
        cache = self[CACHE_INTERFACE_IDENTIFIER]
        self.interfaces.remove(cache)
        del cache

    def schemes(self, interface_type: type) -> Iterable[str]:
        return filter(
            lambda x: x != CACHE_INTERFACE_IDENTIFIER,
            itertools.chain(
                *[i.schemes() for i in self.interfaces if isinstance(i, interface_type)]
            ),
        )

    def list_datasources_from_scheme(self, scheme: str) -> Iterable[str]:
        return self[scheme].get_datasources()


def _load_interface_classes():
    interface_clss = []
    for itf_pkg_name in _list_interface_pkg_names():
        mod = importlib.import_module(itf_pkg_name)
        _logger.debug(f"Imported {mod} from package {itf_pkg_name}")
        cls = inspect.getmembers(
            sys.modules[itf_pkg_name], _is_class(AbstractInterface)
        )
        if not cls:
            raise InvalidInterfaceImplementation(
                f'no interface class found in package "{itf_pkg_name}"'
            )
        elif len(cls) > 1:
            raise InvalidInterfaceImplementation(
                f'more than one interface class found in package "{itf_pkg_name}"'
            )
        else:
            interface_cls = cls[0][1]
            _guard_scheme_conflict(interface_cls, interface_clss)
            interface_clss.append(interface_cls)
    return interface_clss


def _list_interface_pkg_names():
    pkg_names = [x.name for x in pkgutil.iter_modules()]
    itf_names = [pkg for pkg in pkg_names if pkg.startswith(MODULE_PREFIX)]
    return itf_names


def _is_class(cls):
    return lambda obj: inspect.isclass(obj) and issubclass(obj, cls)


@typechecked
def _guard_scheme_conflict(
    new_interface: Type[AbstractInterface],
    interfaces: Iterable[Type[AbstractInterface]],
):
    for interface in interfaces:
        for scheme_new in new_interface.schemes():
            for scheme_old in interface.schemes():
                if scheme_new == scheme_old:
                    raise ConflictingInterfaceScheme(
                        f"scheme: {scheme_new} conflicting between {new_interface.__name__} and {interface.__name__}"
                    )
