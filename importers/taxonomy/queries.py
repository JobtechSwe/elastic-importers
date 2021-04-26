OCCUPATIONS_QUERY = """
query MyQuery {
  concepts(type: "occupation-name", include_deprecated: true) {
    type
    id
    preferred_label
    deprecated_legacy_id
    broader(type: "ssyk-level-4") {
      type
      id
      preferred_label
      deprecated_legacy_id
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
        deprecated_legacy_id
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

GENERAL_QUERY = """
query MyQuery {
  concepts(type: %s) {
    type
    id
    preferred_label
    deprecated_legacy_id
  }
}
"""

QUERY_WITH_REPLACED = """
query MyQuery {
  concepts(type: %s){
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