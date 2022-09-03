parsed = [
    {
        "name": "timeslice",
        "replace": "timeslice(path_date_format)",
        "args": ["path_date_format"],
    },
    {
        "name": "timeslice",
        "replace": "timeslice(file_date_format)",
        "args": ["file_date_format"],
    },
]


from datetime import datetime

s1 = "file:/Users/shaunryan/AzureDevOps/yetl/data/landing/{{timeslice(path_date_format)}}/customer_{{timeslice(file_date_format)}}.csv"
s2 = (
    "file:/Users/shaunryan/AzureDevOps/yetl/data/landing/20220712/customer_20220712.csv"
)


start_pos = s1.find("{")

if start_pos > 0:
    dte_format_attribute = parsed[0]["args"][0]
    print(dte_format_attribute)
    dte_format = "%Y%m%d"
    l = len(datetime.now().strftime(dte_format))

    end_pos = start_pos + l
    slice_str = s2[start_pos:end_pos]

    slice = datetime.strptime(slice_str, dte_format)
    print(slice)
