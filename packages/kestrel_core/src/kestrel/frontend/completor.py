import logging
import re
from datetime import datetime
from typing import Callable, Iterable, List, Tuple

import lark
from kestrel.config.utils import get_all_relations
from kestrel.frontend.parser import get_keywords, parse_without_transform
from kestrel.interface import InterfaceManager
from kestrel.interface.base import AnalyticsInterface, DatasourceInterface
from kestrel.mapping.data_model import get_all_entity_types
from kestrel.utils import timefmt
from typeguard import typechecked

_logger = logging.getLogger(__name__)

ISO_TS_RE = re.compile(r"\d{4}(-\d{2}(-\d{2}(T\d{2}(:\d{2}(:\d{2}Z?)?)?)?)?)?")


@typechecked
def do_complete(
    code: str,
    cursor_pos: int,
    itf_manager: InterfaceManager,
    varnames: List[str],
) -> Iterable[str]:
    _logger.debug("auto_complete function starts...")

    # do not care code after cursor position in the current version
    line = code[:cursor_pos]
    _logger.debug(f"line to auto-complete: {line}")

    # if the last char is a space, `line_to_parse = line`
    # otherwise, exclude the last token in `line_to_parse` to prompt the expected token
    last_word_prefix, line_to_parse = _split_last_token(line)
    _logger.debug(f"last word prefix: {last_word_prefix}")
    _logger.debug(f"line to parse: {line_to_parse}")

    try:
        ast = parse_without_transform(line_to_parse)

    except lark.exceptions.UnexpectedCharacters as e:
        suggestions = ["% illegal char in huntflow %"]
        _logger.debug(f"illegal character in `line_to_parse`, err: {str(e)}")

    except lark.exceptions.UnexpectedEOF as e:
        suggestions = ["% EOF auto-complete internal error, report to developers %"]
        # https://github.com/lark-parser/lark/issues/791
        # Lark updates may break this, check if it is the case
        # no need to use KestrelInternalError; not to break huntflow execution
        _logger.debug(f"Lark with LALR should not give this error: {str(e)}")

    except lark.exceptions.UnexpectedToken as e:
        error_token = e.token
        expected_tokens = e.accepts or e.expected
        expected_values = []
        keywords = set(get_keywords(False))
        relations = get_all_relations()
        entity_types = get_all_entity_types()
        for token in expected_tokens:
            _logger.debug("token: %s", token)
            if token == "VARIABLE":
                expected_values.extend(varnames)
            elif token == "ISOTIMESTAMP":
                if last_word_prefix:
                    if last_word_prefix.startswith("t'"):
                        ts_prefix = last_word_prefix[2:]
                        ts_complete = _do_complete_timestamp(ts_prefix)
                        exp_value = "t'" + ts_complete + "'"
                    else:
                        exp_value = _do_complete_timestamp(last_word_prefix)
                else:
                    exp_value = timefmt(datetime.now())
                expected_values.append(exp_value)
            elif token == "DATASRC_SIMPLE":
                _logger.debug("auto-complete data source")
                expected_values.extend(
                    _do_complete_interface(
                        last_word_prefix,
                        itf_manager.schemes(DatasourceInterface),
                        itf_manager.list_datasources_from_scheme,
                    )
                )
            elif token == "ANALYTICS_SIMPLE":
                _logger.debug("auto-complete analytics")
                expected_values.extend(
                    _do_complete_interface(
                        last_word_prefix,
                        itf_manager.schemes(AnalyticsInterface),
                        itf_manager.list_datasources_from_scheme,
                    )
                )
            elif token == "ENTITY_TYPE":
                expected_values.extend(entity_types)
            elif token == "PROJECT_FIELD":  # not precise
                expected_values.extend(entity_types)
            elif token == "RELATION":
                expected_values.extend(relations)
            elif token == "REVERSED":
                expected_values.append("BY")
            elif token == "EQUAL":
                expected_values.append("=")
            elif token == "ATTRIBUTE":
                # TODO: attribute completion
                # https://github.com/opencybersecurityalliance/kestrel-lang/issues/79
                _logger.debug(f"TODO: ATTRIBUTE COMPLETION")
            elif token == "ENTITY_ATTRIBUTE_PATH":
                # TODO: attribute completion
                # https://github.com/opencybersecurityalliance/kestrel-lang/issues/79
                _logger.debug(f"TODO: ATTRIBUTE COMPLETION")
            elif token == "COMMA":
                expected_values.append(",")
            elif token in keywords:
                if last_word_prefix and last_word_prefix.islower():
                    token = token.lower()
                expected_values.append(token)
            else:
                # token not handled
                continue
        expected_values = sorted(expected_values)
        _logger.debug(f"expected values: {expected_values}")

        # turn `expected_values` into `suggestions`
        _p = last_word_prefix
        _e = expected_values
        suggestions = [t[len(_p) :] for t in _e if t.startswith(_p)] if _p else _e
        suggestions = [x for x in set(suggestions) if x]
        _logger.debug(f"suggestions: {suggestions}")

    else:
        suggestions = []

        # handle optional components
        if ast:
            if ast.children:
                stmt = ast.children[-1].children[0]
                cmd = (
                    stmt.children[1].data.value
                    if stmt.data.value == "assignment"
                    else stmt.data.value
                )
                if cmd == "disp":
                    for clause in ("attr_clause", "limit_clause", "offset_clause"):
                        if not list(stmt.find_data(clause)):
                            suggestions.append("ATTR")
                elif cmd in ("expression", "find") and not list(
                    stmt.find_data("where_clause")
                ):
                    suggestions.append("WHERE")
                elif cmd in ("get", "find") and not list(stmt.find_data("timerange")):
                    suggestions.append("START")
                elif cmd == "apply" and not list(stmt.find_data("args")):
                    suggestions.append("WITH")
            else:
                suggestions = ["DISP", "APPLY", "EXPLAIN", "INFO", "SAVE", "DESCRIBE"]

        suggestions = [x for x in set(suggestions) if x]
        _p = last_word_prefix
        suggestions = (
            [t[len(_p) :] for t in suggestions if t.startswith(_p)]
            if _p
            else suggestions
        )
        _logger.debug(f"suggestions from optional components: {suggestions}")

    return suggestions


@typechecked
def _end_with_blank_or_comma(s: str) -> bool:
    return s[-1] in [" ", "\t", "\n", "\r", "\f", "\v", ","] if s else True


@typechecked
def _split_last_token(s: str) -> Tuple[str, str]:
    last = ""
    if not _end_with_blank_or_comma(s):
        while not _end_with_blank_or_comma(s):
            last = s[-1] + last
            s = s[:-1]
    return last, s


@typechecked
def _do_complete_timestamp(ts_prefix: str) -> str:
    valid_ts_formats = [
        "%Y",
        "%Y-%m",
        "%Y-%m-%d",
        "%Y-%m-%dT%H",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ]
    matched = ISO_TS_RE.match(ts_prefix)
    if matched:
        for ts_format in valid_ts_formats:
            _logger.debug(f"Match timestamp {ts_prefix} with format {ts_format}")
            try:
                ts = datetime.strptime(matched.group(), ts_format)
            except:
                _logger.debug(f"Timestamp match failed")
            else:
                ts_complete = timefmt(ts)
                _logger.debug(f"Timestamp completed: {ts_complete}")
                break
        else:
            ts_complete = "% TS auto-complete internal error, report to developers %"
            # no need to use KestrelInternalError; not to break huntflow execution
            _logger.debug(
                f"TS auto-complete internal error: `valid_ts_formats` is incomplete"
            )
    else:
        ts_complete = "% illegal ISO 8601 timestamp prefix %"
        _logger.debug(f"illegal ISO 8601 timestamp prefix: {ts_prefix}")
    return ts_complete


@typechecked
def _do_complete_interface(
    last_word_prefix: str,
    schemes: Iterable[str],
    list_names_from_scheme: Callable,
) -> Iterable[str]:
    if last_word_prefix and "://" in last_word_prefix:
        scheme, _ = last_word_prefix.split("://")
        if scheme in schemes:
            names = list_names_from_scheme(scheme)
            paths = [scheme + "://" + name for name in names]
            _logger.debug(f"auto-complete interface {scheme}: {paths}")
            expected_values = paths
    else:
        expected_values = [scheme + "://" for scheme in schemes]
    return expected_values
