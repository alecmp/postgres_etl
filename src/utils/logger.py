import logging
import yaml

def setup_logger(config_file="../config/logger_config.yaml"):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    logging.basicConfig(**config)
    return logging.getLogger("etl_logger")

# Esempio di configurazione in YAML
# config/logger_config.yaml
# ---
# version: 1
# formatters:
#   simple:
#     format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# handlers:
#   console:
#     class: logging.StreamHandler
#     formatter: simple
# loggers:
#   etl_logger:
#     handlers: [console]
#     level: DEBUG
# root:
#   level: INFO
