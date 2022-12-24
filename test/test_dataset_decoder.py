from yetl.flow.dataset._decoder import _parse_name
from yetl.flow.parser._constants import YetlTableProperties


def test_decoder_parse_name():

    name = "schema_create_if_not_exists"
    expected = YetlTableProperties.SCHEMA_CREATE_IF_NOT_EXISTS.value
    actual = _parse_name(name)

    assert expected == actual


def test_decoder_parse_name():

    name = "schema_property_doesnt_exist_so_will_fail"
    actual = ""
    expected = "An error occured decoding yetl properties in pydantic from schema_property_doesnt_exist_so_will_fail to yetl.schema.propertyDoesntExistSoWillFail. 'yetl.schema.propertyDoesntExistSoWillFail' is not a valid YetlTableProperties"
    try:
        actual = _parse_name(name)
    except Exception as e:
        actual = str(e)

    assert expected == actual
