#!/usr/bin/env python3
#

import sys
import json
import yaml
from termcolor.color                            import CS, CF

def pre(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def prej(d):
    pre(json.dumps(d, sort_keys=True, indent=4))

def prey(d):
    pre(yaml.dump(d, default_flow_style=False, indent=4, explicit_start=True))
