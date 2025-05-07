#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created May 2025

@author: Gary L Pavlis
"""

import os

def datafile(filename,
      format="yaml",
      dirs=[".","../data/yaml","../data/pf"],
     try_mspass_home=False,
    )->str:
    """
    Return first file found in a search path of directories. 
    
    This function implements ideas covered in a discussion page for mspass. 
    (#641 found here:  https://github.com/mspass-team/mspass/discussions/641).
    Main idea is to provide a robust search for a stock data file.  
    Algorithm first searches for the first occurence of the file in the 
    `dirs` list.  If it doesn't find it any directory listed there it 
    tests the `try_mspass_home` boolean.  If that is set it tries to 
    retrieve the environment variable MSPASS_HOME and searches the 
    same dir list with MSPASS_HOME as the top level in a path.  
    Returns a path.  A None return means it failed.  
    
    :param filename:  file name to search for
    :type filename:  string (no default - required arg)
    :param format:  expected format of filename.  Note the function is pedantic 
      about file names and with raise a ValueError exception of the file name is 
      inconsistent with format.  Currenly accepts two format:  (a) "yaml" in which 
      case filename is required to end with ".yaml" or ".yml".   (b) "pf" in which 
      case the file name must end in ".pf".  Actual tests use os.path and pull the 
      "file extension" part of the filename.
    :type format:  str - must be one of "yaml" or "pf" or a ValueError exception 
       will be raised.  
    :param dirs:  list paths to search for filename 
    :type dirs:  list of str
    :param try_mspass_home:  boolean handling fallback search as described above. 
       That is, if filename is not found in the dirs list the search over dirs 
       is repeated with a lead value of MSPASS_HOME (if defined)
    """
    prog = "datafile"
    if format not in ["yaml","pf"]:
        message = prog + ":  unsupported format=" + format +"\n"
        message += "Must be yaml or pf"
        raise ValueError(message)
    
    # first make sure file name is valid - enforce pedantic rule about extension
    file_extension = os.path.splitext(filename)[1]
    match format:
        case "yaml":
            if file_extension not in [".yaml",".yml"]:
                message = prog + ":  illegal file name for yaml file=" + filename +"\n"
                message += "format argument demands file be a yaml file\n"
                message += "A valid yaml data file name must end in .yaml or .yml"
                raise ValueError(message)
        case "pf":
            if file_extension != ".pf":
                message = prog + ":  illegal file name for pf file=" + filename +"\n"
                message += "format argument demands file be an Antelope pf format file\n"
                message += "A valid pf data file name must end in .pf"
                raise ValueError(message)
    dirlist = dirs
    # extend the search if MSPASS_HOME is defined
    mspass_home = os.environ.get("MSPASS_HOME")
    if mspass_home:
        for d in dirs:
            split_d = os.path.split(d)
            dir = mspass_home
            for sd in split_d:
                dir = os.path.join(dir, sd)
            # needed to handle . and .. in a path
            dir = os.path.normpath(dir)
            dirlist.append(dir)
            
    # now find the first occurence of filename in the list of directories
    for dir in dirlist:
        file_path = os.path.join(dir,filename)
        if os.path.isfile(file_path) and os.access(file_path, os.R_OK):
            return file_path
    # use the function return model instead of throwing an exception
    # callers need to trap this condition with a file not found error message
    return None