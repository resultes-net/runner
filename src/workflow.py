import httpx
from prefect import flow, task


@task(log_prints=True)
def get_stars_for_repo(repo: str) -> int:
    response = httpx.Client().get(f"https://api.github.com/repos/{repo}")
    stargazer_count = response.json()["stargazers_count"]
    print(f"{repo} has {stargazer_count} stars")
    return stargazer_count


@flow
def retrieve_github_stars(repos: list[str]) -> list[int]:
    return get_stars_for_repo.map(repos).wait()