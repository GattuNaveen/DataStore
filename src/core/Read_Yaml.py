import yaml

class ReadYaml:
    @staticmethod
    def read_yaml(path, key):
        with open(path, "r") as f:
            data = yaml.safe_load(f)
            return data.get(key, None)  # Use .get() to handle missing keys gracefully
