"""This module houses classes and instances related to host tracking.

Pydatatask needs to be able to know how to make resources accessible regardless of where they are. To this end, there
can be e.g. dicts of urls keyed on Hosts, indicating that a given target resource needs to be accessed through a
different url depending on which host is accessing it.
"""

from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum, auto
import getpass
import os
import random
import string


class HostOS(Enum):
    """The operating system provided by a host."""

    Linux = auto()


@dataclass(frozen=True)
class Host:
    """A descriptor of a host."""

    name: str
    os: HostOS

    def mktemp(self, identifier: str) -> str:
        """Generate a temporary filepath for the host system."""
        if self.os == HostOS.Linux:
            return (
                f"/tmp/pydatatask-{getpass.getuser()}-"
                f'{identifier}-{"".join(random.choice(string.ascii_lowercase) for _ in range(8))}'
            )
        else:
            raise TypeError(self.os)

    def mk_http_get(
        self, filename: str, url: str, headers: Dict[str, str], verbose: bool = False, handle_err: str = ""
    ) -> str:
        """Generate a shell script to perform an http download for the host system."""
        if self.os == HostOS.Linux:
            headers_str = " ".join(f'--header "{key}: {val}"' for key, val in headers.items())
            return f"""
            URL="{url}"
            FILENAME="$(mktemp)"
            ERR_FILENAME=$(mktemp)
            if [ -d "$FILENAME" ]; then echo "mk_http_get target $FILENAME is a directory" && false; fi
            wget {'-v' if verbose else '-q'} -O- $URL {headers_str} >>$FILENAME 2>>$ERR_FILENAME || \\
                curl -f {'-v' if verbose else ''} $URL {headers_str} >>$FILENAME 2>>$ERR_FILENAME || \\
                (echo "download of $URL failed:" && cat $ERR_FILENAME $FILENAME
                {handle_err}
                false)
            rm $ERR_FILENAME
            cat $FILENAME >"{filename}"
            rm $FILENAME
            """
        else:
            raise TypeError(self.os)

    def mk_http_post(
        self,
        filename: str,
        url: str,
        headers: Dict[str, str],
        output_filename: Optional[str] = None,
        verbose: bool = False,
        handle_err: str = "",
    ) -> str:
        """Generate a shell script to perform an http upload for the host system."""
        if self.os == HostOS.Linux:
            output_redirect = ">>$OUTPUT_FILENAME" if output_filename else ">/dev/null"
            headers_str = " ".join(f'--header "{key}: {val}"' for key, val in headers.items())
            return f"""
            URL="{url}"
            FILENAME="{filename}"
             {'OUTPUT_FILENAME="$(mktemp)"' if output_filename else ''}
            ERR_FILENAME=$(mktemp)
            if ! [ -e "$FILENAME" ]; then echo "mk_http_post target $FILENAME does not exist" && false; fi
            if ! [ -f "$FILENAME" ]; then echo "mk_http_post target $FILENAME is not a file" && false; fi
            wget {'-v' if verbose else '-q'} -O- $URL {headers_str} --post-file $FILENAME 2>>$ERR_FILENAME {output_redirect} || \\
                curl -f {'-v' if verbose else ''} $URL {headers_str} -T $FILENAME -X POST 2>>$ERR_FILENAME {output_redirect} || \\
                (echo "upload of $URL failed:" && cat $ERR_FILENAME {'$OUTPUT_FILENAME ' if output_filename else ''}
                {handle_err}
                false)
            rm $ERR_FILENAME
            {f'cat $OUTPUT_FILENAME >"{output_filename}"; rm $OUTPUT_FILENAME' if output_filename else ''}
            """
        else:
            raise TypeError(self.os)

    def mk_unzip(self, output_filename: str, input_filename: str) -> str:
        """Generate a shell script to unpack an archive for the host system."""
        if self.os == HostOS.Linux:
            return f"""
            mkdir -p {output_filename}
            cd {output_filename}
            tar -xf {input_filename}
            cd -
            """
        else:
            raise TypeError(self.os)

    def mk_zip(self, output_filename: str, input_filename: str) -> str:
        """Generate a shell script to pack an archive for the host system."""
        if self.os == HostOS.Linux:
            return f"""
            cd {input_filename}
            tar -cf {output_filename} .
            cd -
            """
        else:
            raise TypeError(self.os)

    def mk_mkdir(self, filepath: str) -> str:
        """Generate a shell script to make a directory for the host system."""
        if self.os == HostOS.Linux:
            return f"mkdir -p {filepath}"
        else:
            raise TypeError(self.os)


_uname = os.uname()
if _uname.sysname == "Linux":
    LOCAL_OS = HostOS.Linux
else:
    raise ValueError(f"Unsupported local system {_uname.sysname}")

LOCAL_HOST = Host("localhost", LOCAL_OS)
