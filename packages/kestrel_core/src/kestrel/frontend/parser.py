# parse Kestrel syntax, apply frontend mapping, transform to IR

import logging
from itertools import chain

import yaml
from lark import Lark
from typeguard import typechecked

from kestrel.frontend.compile import _KestrelT
from kestrel.mapping.data_model import reverse_mapping
from kestrel.utils import list_folder_files, load_data_file

_logger = logging.getLogger(__name__)


MAPPING_MODULE = "kestrel.mapping"

# cache mapping in the module
frontend_mapping = {}


@typechecked
def get_frontend_mapping(submodule: str, do_reverse_mapping: bool = False) -> dict:
    global frontend_mapping
    if submodule not in frontend_mapping:
        mapping = {}
        for f in list_folder_files(MAPPING_MODULE, submodule, extension="yaml"):
            with open(f, "r") as fp:
                mapping_ind = yaml.safe_load(fp)
            if do_reverse_mapping:
                mapping_ind = reverse_mapping(mapping_ind)
            mapping.update(mapping_ind)
        frontend_mapping[submodule] = mapping
    return frontend_mapping[submodule]


@typechecked
def get_keywords():
    # TODO: this Kestrel1 code needs to be updated
    grammar = load_data_file("kestrel.frontend", "kestrel.lark")
    parser = Lark(grammar, parser="lalr")
    alphabet_patterns = filter(lambda x: x.pattern.value.isalnum(), parser.terminals)
    # keywords = [x.pattern.value for x in alphabet_patterns] + all_relations
    keywords = [x.pattern.value for x in alphabet_patterns]
    keywords_lower = map(lambda x: x.lower(), keywords)
    keywords_upper = map(lambda x: x.upper(), keywords)
    keywords_comprehensive = list(chain(keywords_lower, keywords_upper))
    return keywords_comprehensive


# Create a single, reusable transformer
_parser = Lark(
    load_data_file("kestrel.frontend", "kestrel.lark"),
    parser="lalr",
    transformer=_KestrelT(
        type_map=get_frontend_mapping("types"),
        field_map=get_frontend_mapping("fields", True),
    ),
)


def parse_kestrel(stmts):
    return _parser.parse(stmts)
