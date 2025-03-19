import re
import subprocess as sp
import sys

DIGEST_PATTERN = re.compile("latest: digest: (?P<digest>sha256:[a-z0-9]+) size: [0-9]+$")


def main() -> None:
    if len(sys.argv) != 2:
        print(f"ERROR: Usage: {sys.argv[0]} <image>")
        sys.exit(-1)

    image = sys.argv[1]

    args = [
        "docker",
        "push",
        image
    ]

    completed_process = sp.run(args, capture_output=True, text=True, check=True)
    text = completed_process.stdout

    lines = text.splitlines()
    last_line = lines[-1]

    match = DIGEST_PATTERN.search(last_line)
    assert match

    digest = match["digest"]
    print(digest)


if __name__ == "__main__":
    main()
