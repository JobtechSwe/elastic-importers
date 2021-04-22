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