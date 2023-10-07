from typing import Dict
from dataclasses import dataclass
from enum import Enum, auto
import os
import random
import string


class HostOS(Enum):
    Linux = auto()


@dataclass(frozen=True)
class Host:
    name: str
    os: HostOS

    def mktemp(self, identifier: str) -> str:
        if self.os == HostOS.Linux:
            return f'/tmp/pydatatask-{"".join(random.choice(string.ascii_lowercase) for _ in range(8))}-{identifier}'
        else:
            raise TypeError(self.os)

    def mk_http_get(self, filename: str, url: str, headers: Dict[str, str]) -> str:
        if self.os == HostOS.Linux:
            headers_str = " ".join(f'--header "{key}={val}"' for key, val in headers.items())
            return f"""
            URL='{url}'
            FILENAME='{filename}'
            wget -O- $URL {headers_str} >$FILENAME || curl $URL {headers_str} >$FILENAME
            """
        else:
            raise TypeError(self.os)

    def mk_http_post(self, filename: str, url: str, headers: Dict[str, str]) -> str:
        if self.os == HostOS.Linux:
            headers_str = " ".join(f'--header "{key}={val}"' for key, val in headers.items())
            return f"""
            URL='{url}'
            FILENAME='{filename}'
            wget -O- $URL {headers_str} --post-file $FILENAME || curl $URL {headers_str} --data @$FILENAME
            """
        else:
            raise TypeError(self.os)


_uname = os.uname()
if _uname.sysname == "Linux":
    LOCAL_OS = HostOS.Linux
else:
    raise ValueError(f"Unsupported local system {_uname.sysname}")

LOCAL_HOST = Host("localhost", LOCAL_OS)
