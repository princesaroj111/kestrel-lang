import yaml
from kestrel.utils import list_folder_files

MAPPING_MODULE = "kestrel.mapping"

# cache type mapping
type_mapping = {}


def get_type_from_projection(proj) -> str:
    global type_mapping
    if not type_mapping:
        for f in list_folder_files(MAPPING_MODULE, "types", extension="yaml"):
            with open(f, "r") as fp:
                mapping_ind = yaml.safe_load(fp)
                type_mapping.update(mapping_ind)
    if proj in type_mapping:
        return type_mapping[proj]
    else:
        return "unkown_entity"
