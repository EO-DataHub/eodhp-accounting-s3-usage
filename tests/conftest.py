import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture(
    params=[
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": True,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": True,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": True,
            "with_hardlink": False,
        },
        {
            "with_files": True,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": True,
        },
        {
            "with_files": True,
            "with_sparse": True,
            "with_subdirs": True,
            "with_symlink": True,
            "with_hardlink": True,
        },
        {
            "with_files": False,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": False,
            "with_sparse": False,
            "with_subdirs": True,
            "with_symlink": False,
            "with_hardlink": False,
        },
        {
            "with_files": False,
            "with_sparse": False,
            "with_subdirs": False,
            "with_symlink": True,
            "with_hardlink": False,
        },
    ]
)
def test_dir(request, block_size):
    """
    Simulates a directory containing workspaces block stores. Depending on the settings, each
    workspace will contain:

    * (with_files=True) 3 files containing 500 bytes, 60000 bytes and 2 million bytes.
    * (with_sparse=True) 3 files with size 6GiB but only containing 500, 60000 and 2 million
      written bytes.
    * (with_subdirs=True) a ten-level-deep subdirectory tree with (if with_files=True) a 10kB file
      at the bottom.
    * (with_symlink=True) a symlink pointing to the 2 million byte file from with_files=True,
      whether it exists or not.
    * (with_hardlink=True) a second hard link to each of the with_files=True files.

    The workspaces will have names workspace0, workspace1, ...
    """

    with TemporaryDirectory() as tmpdir_str:
        tmpdir = Path(tmpdir_str)

        def round_up(size):
            return size + (block_size - (size % block_size))

        # dir_size = EFSSamplerMessager.count_size(tmpdir)

        expected_size = block_size

        if request.param["with_files"]:
            with open(tmpdir / "500-byte-file", "wb") as fh:
                fh.write(b"1234" * 125)
                expected_size += round_up(500)

            with open(tmpdir / "60000-byte-file", "wb") as fh:
                fh.write(b"1234" * 15000)
                expected_size += round_up(60000)

            with open(tmpdir / "2000000-byte-file", "wb") as fh:
                fh.write(b"12345678" * 250000)
                expected_size += round_up(2_000_000)

            if request.param["with_hardlink"]:
                os.link(tmpdir / "500-byte-file", tmpdir / "link-to-500-byte-file")
                os.link(tmpdir / "60000-byte-file", tmpdir / "link-to-60000-byte-file")
                os.link(tmpdir / "2000000-byte-file", tmpdir / "link-to-2000000-byte-file")

        if request.param["with_files"]:
            six_gib = 6 * 1024 * 1024 * 1024
            with open(tmpdir / "6GiB-sparse-500-byte-file", "wb") as fh:
                fh.write(b"1234" * 100)
                fh.seek(six_gib - 4 * 25)
                fh.write(b"1234" * 25)

                # File contains two partial blocks.
                expected_size += round_up(400) + round_up(100)

            with open(tmpdir / "6GiB-sparse-60000-byte-file", "wb") as fh:
                fh.write(b"1234" * 10000)
                fh.seek(six_gib - 4 * 5000)
                fh.write(b"1234" * 5000)
                expected_size += round_up(40000) + round_up(20000)

            with open(tmpdir / "6GiB-sparse-2000000-byte-file", "wb") as fh:
                fh.write(b"12345678" * 200000)
                fh.seek(six_gib - 8 * 50000)
                fh.write(b"12345678" * 50000)
                expected_size += round_up(8 * 200000) + round_up(8 * 50000)

        if request.param["with_subdirs"]:
            subdir = tmpdir / "1" / "2" / "3" / "4" / "5" / "6" / "7" / "8" / "9" / "10"
            os.makedirs(subdir)
            expected_size += block_size * 10

            if request.param["with_files"]:
                with open(tmpdir / "10-kibyte-file", "wb") as fh:
                    fh.write(b"1" * 10 * 1024)
                    expected_size += round_up(10 * 1024)

        if request.param["with_symlink"]:
            os.symlink(tmpdir / "500-byte-file", tmpdir / "symlink-to-500-byte-file")

        yield (tmpdir, expected_size / (1024.0**3))


@pytest.fixture
def block_size():
    # The space used by a file will vary by underlying FS.
    # It also varies by number of entries but we don't add many.
    return int(subprocess.run(["stat", "-fc", "%s", "."], capture_output=True).stdout)
