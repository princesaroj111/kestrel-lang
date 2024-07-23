# parse Kestrel syntax, apply frontend mapping, transform to IR

import logging
from itertools import chain
from typing import Iterable

import lark
import yaml
from kestrel.config.utils import get_all_relations, load_relation_configs
from kestrel.frontend.compile import _KestrelT
from kestrel.ir.graph import IRGraph
from kestrel.ir.instructions import Return
from kestrel.mapping.data_model import reverse_mapping
from kestrel.utils import list_folder_files, load_data_file
from pandas import DataFrame
from typeguard import typechecked

_logger = logging.getLogger(__name__)


MAPPING_MODULE = "kestrel.mapping"

# cache mapping in the module
frontend_mappings = {}

# cache relation table in the module
relation_tables = {}


@typechecked
def get_relation_table(table: str) -> DataFrame:
    global relation_tables
    if table not in relation_tables:
        relation_tables[table] = load_relation_configs(table)
    return relation_tables[table]


@typechecked
def get_frontend_mapping(submodule: str, do_reverse_mapping: bool = False) -> dict:
    global frontend_mappings
    if submodule not in frontend_mappings:
        mapping = {}
        for f in list_folder_files(MAPPING_MODULE, submodule, extension="yaml"):
            with open(f, "r") as fp:
                mapping_ind = yaml.safe_load(fp)
            if do_reverse_mapping:
                mapping_ind = reverse_mapping(mapping_ind)
            mapping.update(mapping_ind)
        frontend_mappings[submodule] = mapping
    return frontend_mappings[submodule]


@typechecked
def get_keywords(including_relations: bool = True):
    grammar = load_data_file("kestrel.frontend", "kestrel.lark")
    parser = lark.Lark(grammar, parser="lalr")
    alphabet_patterns = filter(lambda x: x.pattern.value.isalnum(), parser.terminals)
    all_relations = get_all_relations()
    keywords = [x.pattern.value for x in alphabet_patterns] + all_relations
    keywords_lower = map(lambda x: x.lower(), keywords)
    keywords_upper = map(lambda x: x.upper(), keywords)
    keywords_comprehensive = list(chain(keywords_lower, keywords_upper))
    return keywords_comprehensive


@typechecked
def parse_kestrel_and_update_irgraph(
    stmts: str, irgraph: IRGraph, entity_identifier_map: dict
) -> Iterable[Return]:
    """Parse Kestrel code block and update the input IRGraph

    Parameters:
        stmts: Kestrel code block (statements)
        irgraph: existing IRGraph (used for reference resolution; will be updated)
        entity_identifier_map: identifiers for each entity, required by FIND

    Returns:
        List of Return instructions in the current code block
    """
    lp = lark.Lark(
        load_data_file("kestrel.frontend", "kestrel.lark"),
        parser="lalr",
        transformer=_KestrelT(
            irgraph,
            get_frontend_mapping("fields", True),
            get_frontend_mapping("types"),
            get_relation_table("entity"),
            get_relation_table("event"),
            entity_identifier_map,
        ),
    )
    return lp.parse(stmts)


@typechecked
def parse_without_transform(
    stmts: str,
) -> lark.tree.Tree:
    """Parse Kestrel code block and not transform; for syntax error check"""
    lp = lark.Lark(
        load_data_file("kestrel.frontend", "kestrel.lark"),
        parser="lalr",
    )
    return lp.parse(stmts)
