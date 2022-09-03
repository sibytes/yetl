from ..parser.parser import parse_functions
from ..parser._constants import NAME, REPLACE, ARGS


def execute_replacements(sentence: str, dataset):

    parsed: dict = parse_functions(sentence)

    for fn in parsed:

        function_name = fn.get(NAME)
        replace = fn.get(REPLACE)
        args = fn.get(ARGS)

        f = built_in_functions.get(function_name)
        result = f(function_name, dataset, *args)
        sentence = sentence.replace(replace, result)

    sentence = sentence.replace("{{", "").replace("}}", "")

    return sentence


def timeslice(config_name: str, dataset, attribute: str):

    formatted = None

    if hasattr(dataset, attribute) and hasattr(dataset.context, config_name):
        format_str = getattr(dataset, attribute)
        formatted = dataset.context.timeslice.strftime(format_str)

    return formatted


built_in_functions = {"timeslice": timeslice}
