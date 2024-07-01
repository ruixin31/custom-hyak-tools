#!/opt/hyak-user-tools/bin/python3.9
# -*- coding: utf-8 -*-
import re
import sys
import pathlib

file_path = pathlib.Path(__file__).parent.resolve()
sys.path.append(str(file_path))
from hyakalloc.cli import main

if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    sys.exit(main())
