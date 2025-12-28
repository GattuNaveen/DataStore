class BaseConfig:
    def __init__(self, config):
        self.catalog =  config["catalog"]
        self.schema = config["schema"]
        self.tableName = config["tableName"]
        self.path = config["path"]
        self.table_type = config["table_type"]
        self.hashColumns = config.get("hashColumns", [])
