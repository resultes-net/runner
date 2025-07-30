import concurrent.futures as _cf
import dataclasses as _dc
import pathlib as _pl

import swift_multithreaded as _swmt


@_dc.dataclass
class Context:
    jobs_dir_path: _pl.Path
    swift: _swmt.Swift
    executor: _cf.Executor
