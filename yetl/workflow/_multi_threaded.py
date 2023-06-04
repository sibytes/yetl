from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from ._notebook import Notebook
from typing import List


# execute a notebook using databricks workflows
def _execute_notebook(notebook: Notebook, dbutils):
    """Execute a notebookd using databricks workflows"""
    msg = {
        "_message": f"Executing notebook {notebook.path} parameters {notebook.parameters}",
        "status": "executing",
        "notebook": notebook.path,
    }
    print(msg["_message"], flush=True)

    try:
        result = dbutils.notebook.run(
            notebook.path, notebook.timeout, notebook.parameters
        )
        msg = {
            "_message": f"Succeeded notebook {notebook.path}",
            "status": "succeeded",
            "notebook": notebook.path,
        }
        print(msg["_message"], flush=True)
        return result

    except Exception as e:
        if notebook.retry < 1:
            msg = {
                "_message": f"notebook {notebook.path} failed.",
                "status": "failed",
                "error": str(e),
                "notebook": notebook.path,
            }
            print(msg["_message"], flush=True)
            raise Exception(msg["_message"])

        msg = {
            "_message": f"Retrying notebook {notebook.path}",
            "status": "executing",
            "notebook": notebook.path,
        }
        print(msg["_message"], flush=True)
        notebook.retry -= 1


def _try_future(future: Future, catch=False):
    return future.result()


def execute_notebooks(notebooks: List[Notebook], maxParallel: int, dbutils):
    msg = {
        "_message": f"Executing {len(notebooks)} with maxParallel of {maxParallel}",
        "notebooks": len(notebooks),
        "maxParallel": maxParallel,
    }
    print(msg["_message"], flush=True)

    with ThreadPoolExecutor(max_workers=maxParallel) as executor:
        results = [
            executor.submit(_execute_notebook, notebook, dbutils)
            for notebook in notebooks
            if notebook.enabled
        ]

        # the individual notebooks handle their errors and pass back a packaged result
        # we will still need to handle the fact that the notebook execution call may fail
        # or a programmer missed the handling of an error in the notebook task
        # that's what tryFuture(future:Future) does
        results_list = [_try_future(r) for r in as_completed(results)]

        print(
            f"Finished executing {len(notebooks)} with maxParallel of {maxParallel}",
            flush=True,
        )
        return results_list
