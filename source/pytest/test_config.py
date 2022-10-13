from source.config.configurations import EnvConfig, FileConfig

###### configuration test cases ###
def test_EnvConfig():
    assert EnvConfig().env == 'local'

def test_FileConfig():

    assert FileConfig(EnvConfig().env).config_dict['battery_monitor']['format'] == 'csv'

