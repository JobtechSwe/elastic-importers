import json
import os

def concept_to_taxonomy(values):
    json_dict = {}
    for value in values:
        json_dict[value["concept_id"]] = {}
        json_dict[value["concept_id"]]["legacyId"] = value["legacy_id"]
        json_dict[value["concept_id"]]["type"] = value["type"]
        json_dict[value["concept_id"]]["label"] = value["label"]
    #print(len(values))
    #print(len(json_dict))
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(dir_path + "/concept_to_taxonomy.json", "w") as fout:
        json.dump(json_dict, fout,)


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
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(dir_path + "/taxonomy_to_concept.json", "w") as fout:
        json.dump(py_dict, fout)


def unpickle_json(file_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(dir_path + "/" + file_name, "r") as fin:
        data = json.load(fin)
        for k, v in data.items():
            print(k, v)
