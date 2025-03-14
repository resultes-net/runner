import time

import workflow as wf

if __name__ == "__main__":
    while True:
        wf.retrieve_github_stars(
            [
                "PrefectHQ/prefect",
                "pydantic/pydantic",
                "huggingface/transformers"
            ]
        )
        time.sleep(10)
