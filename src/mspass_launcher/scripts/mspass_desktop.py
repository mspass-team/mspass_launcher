#!/usr/bin/env python3
# -*- coding: utf-8 -*
# Author:  Prof. Gary L. Pavlis
"""
This file containa a main program that creates and uses a GUI for
launching and controlling MsPASS in a desktop environment.

Usage:
    mspass [-f configfile -v]

where configfile is an alternative configuration file for the launcher and
GUI.   The default uses one found in the default $MSPASSHOME/data/yaml
diretory called mspass_launcher.yaml.

Use -v to run verbose - echoes configuration data on launch

Created on Sun Apr 27 07:45:54 2025

@author: pavlis
"""
import sys
import argparse
from mspass_launcher.desktop import MsPASSDesktopGUI


def main(args=None):
    """
    main function for mspass desktop gui.  Uses argparse to define yaml configuration
    file.
    """
    if args is None:
        args = sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="mspass-desktop",
        usage="%(prog)s [-f configfile -v]",
        description="Launch MsPASS Desktop GUI",
    )
    parser.add_argument(
        "-c",
        "--config_file",
        nargs=1,
        default="MsPASSDesktopGUI.yaml",
        help="Change default ",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose mode",
    )
    args = parser.parse_args(args)
    config_file = args.config_file
    if args.verbose:
        gui = MsPASSDesktopGUI(configuration=config_file,verbose=True)
    else:
        gui = MsPASSDesktopGUI(configuration=config_file)


if __name__ == "__main__":
    main()
