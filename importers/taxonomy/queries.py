OCCUPATIONS_QUERY = """
query OccupationsQuery {
  concepts(type: "occupation-name", include_deprecated: true) {
    type
    id
    preferred_label
    deprecated_legacy_id
    broader(type: "ssyk-level-4") {
      type
      id
      preferred_label
      ssyk_code_2012
      broader(type: "occupation-field") {
        type
        id
        preferred_label
        deprecated_legacy_id
      }
    }
    replaced_by {
      type
      id
      preferred_label
      deprecated_legacy_id
      broader(type: "ssyk-level-4"){
        id
        type
        preferred_label
        ssyk_code_2012
        broader(type: "occupation-field") {
          type
          id
          preferred_label
          deprecated_legacy_id
        }
      }
    }
    related(type: "occupation-collection") {
      type
      id
      preferred_label
      deprecated_legacy_id
    }
  }
}
"""

REGION_QUERY = """
query MyQuery {
  concepts(type: "country", id: "i46j_HmG_v64") {
    narrower {
      type
      id
      national_nuts_level_3_code_2019
      preferred_label
    }
  }
}
"""

MUNICIPALITY_QUERY = """
query municipality {
  concepts(type: "municipality" ) {
    id
    preferred_label
    deprecated_legacy_id
    lau_2_code_2015
  }
}
"""

GENERAL_QUERY = """
query GeneralQuery {
  concepts(type: %s) {
    type
    id
    preferred_label
    deprecated_legacy_id
  }
}
"""

QUERY_WITH_REPLACED = """
query ReplacedQuery {
  concepts(type: %s, include_deprecated: true){
    type
    id
    preferred_label
    deprecated_legacy_id
    replaced_by{
      type
      id 
      preferred_label
      deprecated_legacy_id
    }
  }
}
"""