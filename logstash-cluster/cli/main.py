#!/usr/bin/env python
# -*- coding: utf-8

# Copyright 2017-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

from __future__ import unicode_literals
from __future__ import print_function
import click
from cli import Cli
import os


# Disable Warning: Click detected the use of the unicode_literals
# __future__ import.
click.disable_unicode_literals_warning = True


@click.command()
def cli():
    """Creates and calls Saws.
    Args:
        * None.
    Returns:
        None.
    """
    try:
        cli = Cli(os.environ.get('ATOMIX_HOST', 'localhost'), int(os.environ.get('ATOMIX_PORT', 5678)))
        cli.run()
    except (EOFError, KeyboardInterrupt):
        cli.set_return_value(None)


if __name__ == "__main__":
    cli()