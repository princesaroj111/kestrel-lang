from pathlib import PurePosixPath, PureWindowsPath


class Path:
    """Wrapper class for handling file paths from either Windows or POSIX systems"""

    def __init__(self, raw_path: str):
        """Create a path object that respects the original path separator"""
        if "\\" in raw_path:
            self._path = PureWindowsPath(raw_path)
        elif "/" in raw_path:
            self._path = PurePosixPath(raw_path)
        else:
            # need some heuristics to guess the path type
            self._path = PureWindowsPath(raw_path)  # TODO: more advanced detection?

    def basename(self) -> str:
        """Returns the path with any leading directories removed"""
        return str(self._path.name)

    def dirname(self) -> str:
        """Returns the path with the last component removed, or "." if there's only 1 component"""
        return str(self._path.parent)
