"""
Constants and utility classes useful for the rest of the library.
You care about STDOUT, which may be passes to a stderr parameter to indicte the two streams should be joined.
"""


class _StderrIsStdout:
    pass


STDOUT = _StderrIsStdout()
