import os
import pathlib as pl

import workflow as wf

if __name__ == "__main__":
    containing_dir_path = pl.Path(__file__).parent
    os.chdir(containing_dir_path)
    
    wf.retrieve_github_stars.serve(
        parameters={
            "repos": ["python/cpython", "prefectHQ/prefect"],
        }
    )
