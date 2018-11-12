import json
import os
from importers.new_taxonomy.settings import resources_folder

def concept_to_taxonomy(values):
    json_dict = {}
    for value in values:
        json_dict[value["concept_id"]] = {}
        json_dict[value["concept_id"]]["legacyId"] = value["legacy_id"]
        json_dict[value["concept_id"]]["type"] = value["type"]
        json_dict[value["concept_id"]]["label"] = value["label"]
    #print(len(values))
    #print(len(json_dict))
    with open(resources_folder + "concept_to_taxonomy.json", "w") as fout:
        json.dump(json_dict, fout, sort_keys=True,  indent=4, separators=(',', ': '))


def save_taxonomy_to_concept_as_json(values):
    result = taxonomy_to_concept(values)
    dump_json("taxonomy_to_concept.json", result)

def taxonomy_to_concept(values):
    py_dict = {}
    for value in values:
        py_dict[value["type"]] = {}
        py_dict[value["type"]][value["legacy_id"]] = {}
        py_dict[value["type"]][value["legacy_id"]]["conceptId"] = value["concept_id"]
        py_dict[value["type"]][value["legacy_id"]]["legacyId"] = value["legacy_id"]
        py_dict[value["type"]][value["legacy_id"]]["preferredTerm"] = value["label"]
        py_dict[value["type"]][value["legacy_id"]]["type"] = value["type"]
    #print(len(values))
    #print(len(py_dict))
    return py_dict


def dump_json(file_name, data):
    with open(resources_folder + file_name , "w") as fout:
        json.dump(data, fout , sort_keys=True, indent=4, separators=(',', ': '))


def unpickle_json(file_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(dir_path + "/" + file_name, "r") as fin:
        data = json.load(fin)
        for k, v in data.items():
            print(k, v)
