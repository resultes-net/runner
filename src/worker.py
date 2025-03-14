import workflow as wf

if __name__ == "__main__":
    wf.retrieve_github_stars.serve(
        parameters={
            "repos": ["python/cpython", "prefectHQ/prefect"],
        }
    )
