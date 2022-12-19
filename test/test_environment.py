from yetl.flow._environment import Environment


def test_environment():

    environment = Environment()

    assert environment.environment_settings.root == "./test/config"
    assert environment.environment_settings.environment == "local_test"
