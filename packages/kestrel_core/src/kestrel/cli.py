################################################################
#
# Kestrel Command-line Utilities
# - kestrel
# - ikestrel
#
################################################################

import argparse
import cmd
import logging

from kestrel.exceptions import KestrelError
from kestrel.session import Session
from pandas import DataFrame


def add_logging_handler(handler, if_debug):
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    current_logging_level = root_logger.getEffectiveLevel()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG if if_debug else logging.INFO)

    return handler, current_logging_level


def kestrel():
    parser = argparse.ArgumentParser(description="Kestrel Interpreter")
    parser.add_argument("huntflow", help="huntflow in .hf file")
    parser.add_argument(
        "-v", "--verbose", help="print verbose log", action="store_true"
    )
    parser.add_argument(
        "--debug", help="debug level log (default is info level)", action="store_true"
    )
    args = parser.parse_args()

    if args.verbose:
        add_logging_handler(logging.StreamHandler(), args.debug)

    with Session() as session:
        with open(args.huntflow, "r") as fp:
            huntflow = fp.read()
        outputs = session.execute(huntflow)
        results = "\n\n".join(
            [o.to_string() if isinstance(o, DataFrame) else str(o) for o in outputs]
        )
        print(results)


# TODO: fix #405 so we do not need this
CMDS = [  # command_no_result from kestrel.lark
    "APPLY",
    "DISP",
    "INFO",
    "SAVE",
]


def display_outputs(outputs):
    for i in outputs:
        print(i)


class IKestrel(cmd.Cmd):
    prompt = "> "

    def __init__(self, session: Session):
        self.session = session
        self.buf = ""
        super().__init__()

    def default(self, line: str):
        try:
            outputs = self.session.execute(line)
            display_outputs(outputs)
        except KestrelError as e:
            print(e)

    def completenames(self, text, *ignored):
        code, _start, _end = ignored
        if code.isupper():
            # Probably a command?
            results = [i for i in CMDS if i.startswith(code)]
        else:
            # Try all commands and vars
            results = [i for i in CMDS if i.lower().startswith(code)]
            results += [
                i for i in self.session.get_variable_names() if i.startswith(code)
            ]
        return results

    def completedefault(self, *ignored):
        _, code, start, end = ignored
        results = self.session.do_complete(code, end)
        stub = code[start:]
        return [stub + suffix for suffix in results]

    def do_EOF(self, _line: str):
        print()
        return True


def ikestrel():
    parser = argparse.ArgumentParser(description="Kestrel Interpreter")
    parser.add_argument(
        "-v", "--verbose", help="print verbose log", action="store_true"
    )
    parser.add_argument(
        "--debug", help="debug level log (default is info level)", action="store_true"
    )
    args = parser.parse_args()

    if args.verbose:
        add_logging_handler(logging.StreamHandler(), args.debug)

    with Session() as s:
        ik = IKestrel(s)
        ik.cmdloop()
