

def log_import_metrics(log, importer_name, items_count):
    json_log_msg = {'importer': importer_name, 'imported_count': items_count}
    log.info(json_log_msg)