import logging
from collections import OrderedDict, defaultdict
from functools import reduce
from typing import Any, Iterable, List, Optional, Tuple, Union

import numpy
import yaml
from kestrel.exceptions import IncompleteDataMapping
from kestrel.ir.filter import ReferenceValue
from kestrel.mapping.transformers import run_transformer, run_transformer_on_series
from kestrel.utils import list_folder_files
from pandas import DataFrame, Int64Dtype
from typeguard import typechecked

_logger = logging.getLogger(__name__)


PROMOTE_LIST_OP = {
    "=": "IN",
    "!=": "NOT IN",
}


def _reverse_value_map(obj: dict):
    result = defaultdict(list)
    for k, v in obj.items():
        if isinstance(v, list):
            for i in v:
                result[i].append(k)
        else:
            result[v].append(k)
    return result


def _add_mapping(obj: dict, key: str, mapping: dict):
    """Add `key` -> `mapping` to `obj`, appending if necessary"""
    existing_mapping = obj.get(key)
    if existing_mapping:
        if isinstance(existing_mapping, str):
            existing_mapping = [{"ocsf_field": existing_mapping}]
        elif isinstance(existing_mapping, dict):
            existing_mapping = [existing_mapping]
    else:
        existing_mapping = []
    native_value = mapping.get("native_value")
    if "ocsf_value" not in mapping and isinstance(native_value, dict):
        # There's a dict instead of a transformer function; reverse that
        mapping["ocsf_value"] = _reverse_value_map(native_value)
    existing_mapping.append(mapping)
    obj[key] = existing_mapping


def _reverse_dict(obj: dict, k: str, v: dict):
    """Reverse a single OCSF -> native mapping and add it to `obj`"""
    key = v["native_field"]
    mapping = {i: j for i, j in v.items() if i != "native_field"}
    mapping["ocsf_field"] = k
    _add_mapping(obj, key, mapping)


def _add_attr(obj: dict, key: str, value: str):
    """Add `key` -> `value` to `obj`, appending if necessary"""
    if key not in obj:
        obj[key] = value
    else:
        existing = obj[key]
        if isinstance(existing, str) and existing != value:
            obj[key] = [existing, value]
        elif value not in existing:
            existing.append(value)


def reverse_mapping(obj: dict, prefix: str = None, result: dict = None) -> dict:
    """Reverse the mapping of `obj`

    Newly loaded mapping from disk is OCSF -> native mapping. This function
    takes in such mapping, and reverse it to native -> OCSF mapping, which can
    be used by the frontend. The result mapping is flattened!

    To call the function: `reverse_mapping(ocsf_to_native_mapping)`

    Parameters:
        obj: mapping loaded from disk (OCSF -> native)
        prefix: key path to `obj`; used by the recursive function itself
        result: intermediate result mapping; used by the recursive function itself

    Returns:
        native -> OCSF mapping
    """
    if result is None:
        result = {}
    for k, v in obj.items():
        k = ".".join((prefix, k)) if prefix else k
        # Recurse if necessary
        if isinstance(v, str):
            _add_attr(result, v, k)
        elif isinstance(v, list):
            # Need to handle multiple mappings
            for i in v:
                if isinstance(i, str):
                    _add_attr(result, i, k)
                elif "native_field" in i:
                    _reverse_dict(result, k, i)
                else:
                    # Need to "deep" merge with current results
                    reverse_mapping(i, k, result)
        elif isinstance(v, dict):
            # First determine if this is a complex mapping or just another level
            if "native_field" in v:
                _reverse_dict(result, k, v)
            else:
                # Need to "deep" merge with current results
                reverse_mapping(v, k, result)

    return result


def _get_map_triple(d: dict, prefix: str, op: str, value) -> tuple:
    mapped_op = d.get(f"{prefix}_op")
    transform = d.get(f"{prefix}_value")
    _logger.debug("transform = %s (%s)", type(transform), transform)
    if transform:
        if isinstance(transform, dict):
            new_value = transform.get(value, value)  # FIXME
        else:
            new_value = run_transformer(transform, value)
    else:
        new_value = value
    if mapped_op:
        new_op = mapped_op
    elif isinstance(new_value, list):
        # Do we need to change the op here?
        new_op = PROMOTE_LIST_OP.get(op, op)
    else:
        new_op = op
    return (d[f"{prefix}_field"], new_op, new_value)


@typechecked
def translate_comparison_to_native(
    to_native_nested_map: dict, field: str, op: str, value: Any
) -> list:
    """Translate the (`field`, `op`, `value`) triple using data model map

    This function may be used in datasource interfaces to translate a comparison
    in the OCSF data model to the native data model, according to the data model
    mapping in `to_native_nested_map`.

    This function translates the (`field`, `op`, `value`) triple into a list of
    translated triples based on the provided data model map. The data model map
    is a dictionary that maps fields from one data model to another. For
    example, if you have a field named "user.name" in your data model, but the
    corresponding field in the native data model is "username", then you can use
    the data model map to translate the field name.

    Parameters:
        to_native_nested_map: OCSF to native nested mapping (directly from the YAML)
        field: The field name to be translated.
        op: The comparison operator.
        value: The value to be compared against.

    Returns:
        A list of translated triples.

    Raises:
        KeyError: If the field cannot be found in the data model map.
    """
    _logger.debug("comp_to_native: %s %s %s", field, op, value)
    result = []
    try:
        node = reduce(dict.__getitem__, field.split("."), to_native_nested_map)
        if isinstance(node, list):
            for i in node:
                if isinstance(i, dict):
                    result.append(_get_map_triple(i, "native", op, value))
                else:
                    result.append((i, op, value))
        elif isinstance(node, dict):
            result.append(_get_map_triple(node, "native", op, value))
        elif isinstance(node, str):
            result.append((node, op, value))
    except KeyError:
        # Pass-through
        _logger.debug(f"no mapping for {field}, pass through")
        result.append((field, op, value))
    _logger.debug("comp_to_native: return %s", result)
    return result


@typechecked
def translate_comparison_to_ocsf(
    to_ocsf_flattened_field_map: dict,
    field: str,
    op: str,
    value: Union[str, int, float, List[str], List[int], ReferenceValue],
) -> list:
    """Translate the (`field`, `op`, `value`) triple

    This function is used in the frontend to translate a comparison in
    the STIX (or, in the future, ECS) data model to the OCSF data
    model, according to the data model mapping in `to_ocsf_flattened_field_map`.

    This function translates the (`field`, `op`, `value`) triple into a list of
    translated triples based on the provided data model map. The data model map
    is a dictionary that maps fields from one data model to another. For
    example, if you have a field named "user.name" in your data model, but the
    corresponding field in the native data model is "username", then you can use
    the data model map to translate the field name.

    Parameters:
        to_ocsf_flattened_field_map: a flattened native-to-ocsf field mapping
        field: The field name to be translated.
        op: The comparison operator.
        value: The value to be compared against.

    Returns:
        A list of translated triples.

    Raises:
        KeyError: If the field cannot be found in the data model map.

    """
    _logger.debug("comp_to_ocsf: %s %s %s", field, op, value)
    result = []
    mapping = to_ocsf_flattened_field_map.get(field)
    if isinstance(mapping, str):
        # Simple 1:1 field name mapping
        result.append((mapping, op, value))
    elif isinstance(mapping, list):
        for i in mapping:
            if isinstance(i, dict):
                result.append(_get_map_triple(i, "ocsf", op, value))
            else:
                result.append((i, op, value))
    return result


@typechecked
def load_default_mapping(
    data_model_name: Optional[str],
    mapping_pkg: str = "kestrel.mapping",
    submodule: str = "fields",
):
    result = {}
    for f in list_folder_files(mapping_pkg, submodule, data_model_name, "yaml"):
        with open(f, "r") as fp:
            result.update(yaml.safe_load(fp))
    return result


@typechecked
def get_all_entity_types() -> List[str]:
    return list(set(load_default_mapping(None, submodule="types").values()))


@typechecked
def check_entity_identifier_existence_in_mapping(
    data_model_mapping: dict,
    entity_identifiers: dict,
    interface_information: Optional[str] = None,
):
    for entity_name, ids in entity_identifiers.items():
        if entity_name in data_model_mapping:
            entity = data_model_mapping[entity_name]
            for idx in ids:
                try:
                    reduce(dict.__getitem__, idx.split("."), entity)
                except KeyError:
                    msg_body = f"Identifier '{idx}' for entity '{entity_name}' is missing in data mapping"
                    appendix = (
                        f" at '{interface_information}'"
                        if interface_information
                        else ""
                    )
                    raise IncompleteDataMapping(msg_body + appendix)


@typechecked
def _get_from_mapping(mapping: Union[str, list, dict], key) -> list:
    result = []
    if isinstance(mapping, list):
        for i in mapping:
            if isinstance(i, dict):
                result.append(i[key])
            else:
                result.append(i)
    elif isinstance(mapping, dict):
        result.append(mapping[key])
    elif isinstance(mapping, str):
        result.append(mapping)
    return result


@typechecked
def translate_projection_to_native(
    to_native_nested_map: dict,  # mapping for translation
    ocsf_base_field: Optional[str],  # translate this base field
    attrs: Optional[Iterable] = None,  # known attributes (in OCSF) to project
    native_table_schema: Optional[List[str]] = None,  # additional filter
) -> list:
    result = []

    if ocsf_base_field:
        try:
            base_map = reduce(
                dict.__getitem__, ocsf_base_field.split("."), to_native_nested_map
            )
        except KeyError:
            _logger.warning(f"No mapping for base projection field: {ocsf_base_field}")
            base_map = {}
    else:
        # event does not have ocsf_base_field
        base_map = to_native_nested_map

    if attrs:
        # project specified attributes
        for attr in attrs:
            try:
                mapping = reduce(dict.__getitem__, attr.split("."), base_map)
                result.extend(
                    [
                        (i, attr)
                        for i in _get_from_mapping(mapping, "native_field")
                        if native_table_schema
                        and i in native_table_schema
                        or not native_table_schema
                    ]
                )
            except KeyError:
                # TODO: think better way than pass-through, e.g., raise exception
                _logger.warning(
                    f"mapping not found for entity: '{ocsf_base_field}' and attribute: '{attr}'; treat it as no mapping needed"
                )
                result.append((attr, attr))
    else:
        # project all attributes known for the entity
        # or event if no ocsf_base_field specified
        # filter by native_table_schema

        # filter only the ocsf_base_field part of the map
        for native_field, mapping in reverse_mapping(base_map).items():
            if not native_field.endswith("*"):
                result.extend(
                    [
                        (native_field, i)
                        for i in _get_from_mapping(mapping, "ocsf_field")
                        if native_table_schema
                        and native_field in native_table_schema
                        or not native_table_schema
                    ]
                )

    # De-duplicate list while maintaining order
    final_result = list(OrderedDict.fromkeys(result))
    _logger.debug("proj_to_native: return %s", final_result)

    return final_result


@typechecked
def translate_entity_projection_to_ocsf(
    to_ocsf_flattened_field_map: dict, native_projection: str
) -> str:
    _map = to_ocsf_flattened_field_map
    if not native_projection.endswith("*"):
        native_projection += ".*"
    ocsf_projection = _map.get(native_projection, native_projection)
    if isinstance(ocsf_projection, list):
        ocsf_projection = ocsf_projection[0]
    ocsf_projection = ocsf_projection[:-2]
    return ocsf_projection


@typechecked
def translate_attributes_projection_to_ocsf(
    to_ocsf_flattened_field_map: dict,
    native_type: str,
    entity_type: str,
    attrs: Iterable[str],
) -> Tuple:
    _map = to_ocsf_flattened_field_map
    result = []
    for attr in attrs:
        mapping = _map.get(attr)
        if not mapping and native_type:  # try extend with STIX style
            mapping = _map.get(f"{native_type}:{attr}")
        if not mapping and native_type:  # try extend with ECS style
            mapping = _map.get(f"{native_type}.{attr}")
        if mapping:
            ocsf_fields = _get_from_mapping(mapping, "ocsf_field")
            if entity_type and entity_type != "event":
                # Need to restrict attributes of that entity type
                prefix = f"{entity_type}."
                ocsf_attrs = [
                    field[len(prefix) :]
                    for field in ocsf_fields
                    if field.startswith(prefix)
                ]
                result += ocsf_attrs
        else:  # not found; pass through
            result.append(attr)
    final_result = list(OrderedDict.fromkeys(result))
    return tuple(final_result)


def _get_transformer(obj: dict) -> Optional[Union[str, dict]]:
    # First look for native -> ocsf value transformer
    ocsf_value = obj.get("ocsf_value")
    if not ocsf_value:
        # Next check if there's a ocsf -> native transformer
        native_value = obj.get("native_value")
        if isinstance(native_value, dict):
            # If it's a simple value map, we can reverse it
            ocsf_value = _reverse_value_map(native_value)
    return ocsf_value


def _get_transformers(mappings: List[Union[str, dict]]) -> List[Union[str, dict]]:
    """Collect value transformers from mappings, discarding duplicates"""
    result = []  # Need a list instead of set since dicts aren't hashable
    for mapping in mappings:
        if "ocsf_value" in mapping or "native_value" in mapping:
            transformer = _get_transformer(mapping)
            if transformer and transformer not in result:
                result.append(transformer)
    return result


@typechecked
def translate_dataframe(df: DataFrame, to_native_nested_map: dict) -> DataFrame:
    # Translate results into Kestrel OCSF data model
    # The column names of df are already mapped
    for col in df.columns:
        try:
            mapping = reduce(dict.__getitem__, col.split("."), to_native_nested_map)
        except KeyError:
            _logger.debug("No mapping for %s", col)
            mapping = None
        if isinstance(mapping, dict):
            mapping = [mapping]
        if isinstance(mapping, list):
            transformer_names = _get_transformers(mapping)
            if len(transformer_names) > 0:
                if len(transformer_names) > 1:
                    raise NotImplementedError("Multiple to OCSF value transformers")
                else:
                    transformer_name = transformer_names.pop()
                    if isinstance(transformer_name, dict):
                        # Not actually a named function; it's a literal value map
                        value_map = {}
                        for k, vl in transformer_name.items():
                            if len(vl) > 1:
                                raise NotImplementedError(
                                    "Multiple to OCSF value mapping"
                                )
                            else:
                                value_map[k] = vl[0]
                        # use .apply intead of .replace to handle type correctly
                        df[col] = df[col].apply(
                            lambda x: value_map[x] if x in value_map else x
                        )
                    else:
                        s = run_transformer_on_series(
                            transformer_name, df[col].dropna()
                        )
                        df[col] = s
                        # if the series is integers, use Int64 (Nullable int) to allow NaN/NA
                        # if not, pandas will use float64 by default, which gives .0
                        if s.dtype == numpy.int64:
                            df[col] = df[col].astype(Int64Dtype())
    df = df.replace({numpy.nan: None})
    return df
