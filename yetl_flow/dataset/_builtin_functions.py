import regex
from ._constants import NAME, REPLACE, ARGS


def parse_functions(sentence: str):

    rgx_function = regex.compile(r"(?si)(?|{0}(.*?){1}|{1}(.*?){0})".format("{{", "}}"))
    functions = rgx_function.findall(sentence)
    funcs = []
    for f in functions:
        for fp in f:
            func: str = fp
            args = func[func.find("(") + 1 : func.find(")")]
            args = args.split(",")
            args = [a.strip() for a in args]
            function_name = func[: func.find("(")].strip()
            function = {NAME: function_name, REPLACE: func, ARGS: args}
            funcs.append(function)

    return funcs


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
