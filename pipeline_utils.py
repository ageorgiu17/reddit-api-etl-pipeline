import os


def get_config_param(config_name, config_dict=None, default_value=None):
    if config_name in os.environ:
        return os.environ[config_name]
    elif default_value is not None:
        return default_value
    else:
        return config_dict[config_name]
