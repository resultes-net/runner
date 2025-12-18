import collections.abc as _cabc

class LineBuilder:
    def __init__(self) -> None:
        self._started_line = ""

    def add_bytes_and_get_new_lines(self, bytes: str) -> _cabc.Sequence[str]:
        continued_line, *next_lines = bytes.split("\n")

        if not next_lines:
            self._started_line += continued_line
            return []

        first_line = self._started_line + continued_line
        *intermediate_lines, last_started_line = next_lines

        new_lines = [first_line, *intermediate_lines]

        self._started_line = last_started_line

        return new_lines
