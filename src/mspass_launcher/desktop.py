#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file containa a main program that creates and uses a GUI for 
launching and controlling MsPASS in a desktop environment.  

Usage:
    mspass [-f configfile]
    
where configfile is an alternative configuration file for the launcher and 
GUI.   The default uses one found in the default $MSPASSHOME/data/yaml 
diretory called mspass_launcher.yaml.

Created on Sun Apr 27 07:45:54 2025

@author: pavlis
"""
import os
import platform
import time
import subprocess
import threading
import tkinter as tk
import yaml
import json
from mspass_launcher.util import datafile

class MsPASSDesktopCluster:
    """
    Class to manage an instance of MsPASS with docker compose. 

    This class provides the "engine" used within the MsPASSDesktopGUI.
    All the GUI code is then isolated to that class that uses this one.  
    The class creates and manages an instance of  MsPASS cluster 
    using docker compose.   Because docker compose is a command line 
    tool most of the methods use the python subprocess.run function 
    to run appropriate docker compose incantations.  

    The configuration of the cluster is driven by a yaml file loaded 
    by the constructor.  That file should be a lightly edited version 
    of the master stored in this repository in the file 
    data/yaml/MsPASSDesktopCluster.yaml.  In particular, the yaml 
    file must define four services run in four launches of the docker 
    container with the tags:  mspass-db, mspass-fronend, mspass-scheduler, 
    and mspass-worker.  

    Because the primary purpose of this class is to encapsulate an 
    abstraction of a running MsPASS cluster for the GUI, only user's 
    interested in modifying the GUI will likely need to ever examine 
    this class.   
    """ 

    def __init__(self,
                 configuration="DesktopCluster.yaml",
                 ):
        """
        Constructor for this class.   
    
        The constructor currently does only one thing which is to 
        load the configuration file name.   The main initializations 
        are done in the launch_cluster method.  That violates the 
        normal OOP rule of "construction is initialization" but 
        is appropriate for this application.  The reason is that 
        the GUI creates an instance of this class on startup but 
        doesn't run the launch_cluster method until the "launch" 
        button is pressed by the user.  That was done to create a
        more intuitive gui where when it first launches the only 
        active part of the gui is the launch button.  Once the 
        launch button is active other actions possible.
        
        Emphasize the only thing loaded is a file name.  The complexity 
        is it uses a rule-based search for data files that allows the 
        yaml configuration file to be stored other than in the current 
        directory.
        """
        self.docker_configuration_file = mspass_yaml_file(configuration)
    def launch_cluster(self):
        """
        Launch an instance of MsPASS with docker compose using configuration 
        file loaded by constructor.  
        
        This method does little more than run the command line for docker 
        compose up using the configuration file 
        """
        runline=[]
        runline.append("docker")
        runline.append("compose")
        runline.append("-f")
        # could test the type of configuration to be str but docker-compose 
        # will fail if this is not a valid file name
        runline.append(self.docker_configuration_file)
        runline.append("up")
        runline.append("-d")
        runout=subprocess.run(runline,capture_output=True,text=True)
        print(runout.stdout)
        # TODO:  this needs a better handler
        if len(runout.stderr)>0:
            print(runout.stderr)
    def launch_jupyter_client(self,browser=None):
        """
        Launches a jupyter lab client in a browser.  
        
        The mspass-frontend service runs a jupyter lab server.  The CLI 
        way to connect to that server is to cut-and-paste the url that is 
        posted to stdout when the mspass container is launched with the 
        "frontend"  role.   This method automates that by running the 
        class url method (see below) and then trying to launch the browser 
        using subprocess.run.  Note experience has shown that for 
        both firefox and safari if an instance is already running this 
        function will create a new tab to run jupyter lab.  It will 
        literally launch the browser only if one isn't already running,.  
        
        :param browser:  name of web browser to attempt to launch.  
           It needs to match a name that would resolve on the command 
           line.  Different launch lines are needed for different 
           operating systems.  This function tries to handle that 
           
        """
        url = self.url()
        host_os = platform.system()
        if browser is None:
            browser = get_default_browser()     
        runline = []
        match host_os:
            case "Darwin":
                runline.append("open")
                runline.append("-a")
                runline.append(browser)
                runline.append(url)
            case "Linux":
                runline=[browser, url]
            case "Windows":
                print("MsPASSDesktopCluster.launch_jupyter_client: windows browser launching not yet implemented")
                print("You will need to enter url in a browser window manually")
            case _:
                message = "MsPASSDesktopCluster.launch_jupyter_client:  cannot handle this operating system"
                message += "platform.system returned {}\n".format(host_os)
                message += "Only know how to handle one of:  Darwin, Linux, or Windows\n"
                message += "Submit a pull request with a fix and for now use the CLI cut-and-paste method to connect to jupyter"
                print(message)
        if len(runline)>0:
            try:
                subprocess.run(runline,capture_output=True,text=True,check=True)
            except subprocess.CalledProcessError as e:
                print(f"MsPASSDesktopCluster.launch_jupyter_client:  subprocess run call failed with return code = {e.returncode}:")
                print(f"Stderr from subprocess:\n{e.stderr}")
                print("Launch jupyter window manually as described in mspass documentation")
        
    def run(self,filename,container="mspass-scheduler",return_output=False)->str:
        """
        Runs a python script assumed to be contained in file defined by filename (arg0) value. 

        This method run a python script defined by filename in the 
        mspass-scheduler container.   It does that using the subprocess.run 
        method running docker-compose exec.  It is designed to be bombproof meaning 
        it has an error handler to catch any exception.  The approach instead is
        an assumption that a human being will normally be examining the string the 
        function emits.  By default it emits a cryptic completed message if successful 
        and error messages if something fails.  
        
        :param filename:   required file name of script to be run.  Assumed to be a
           a valid file name or path.  run will fail if the path is not found and 
           you should see a message to that fact emitted.
        :type filename:  str (no default - required argument) 
        :param container:  name of service of the container where the script 
          should be run.  Default is mspass-scheduler and should not normally be 
          changed.  On a single node system (i.e. all desktops at this time) 
          any of the running containers should work but the default should 
          normally be used.
        :type container:  string
        :param return_output:  boolean controlling how stdout from the python 
           run is handled.  By default set False, which causes stdout to be 
           dropped and only a summary message indicating success is returned. 
           That is default to avoid having to return potentially huge outputs.  
           When True the return is the content of stdout from the python run. 
           Note that if there are any errors in a run the output is ALWAYS 
           returned along with a dump of stderr from the process.  
        :return:  str containing one of three things: (1) summary finished 
          message (default for now errors), (2) if there are errors the 
          return will be  dump of stderr from the process followed by a dump 
          of stdout from the process, or (3) a dump of stdout when the 
          script ran with no errors and return_put was set True. 
        """
        runline = ["docker","compose"]
        runline.append("-f")
        runline.append(self.docker_configuration_file)
        runline.append("exec")
        runline.append(container)
        runline.append("python")
        runline.append(filename)
        try:
            runout = subprocess.run(runline,capture_output=True,text=True,check=True)
            if return_output:
                return runout.stdout
            else:
                return("python script ="+filename+" ran with no errors")
        except subprocess.CalledProcessError as e:
            return_message = "MsPASSDesktopCluster.run:   subprocess run call failed\n"
            return_message += "Error code returned = {}".format(e.returncode)
            return_message += "Stderr from subprocess was the following:\n"
            return_message += e.stderr
            # ignore return_output if there is an error as stdout is nearly always needed then
            return_message += "Output of the script was the following:\n"
            return_message += runout.stdout
            return return_message
        except Exception as e:
            return_message= "MsPASSDesktopCluster.run:  something caused an unexpected exception to be thrown\n"
            return_message += str(e)
            return return_message
    def status(self,service,form="summary"):
        """
        Return status of one of the standard mspass services.
        
        This class assumes mspass is running with four services with the 
        four tags that define that service:  mspass-frontend, mspass-db, 
        mspass-scheduler and mspass-worker.   arg0 when the method is called 
        must be one of those 4 keywords or the method will throw a RuntimeError
        exception.   By default it returns a string with the value of the "state" 
        attribute returned by docker-compose ps.   Callers can test that value 
        to verify a service is running.   If the form kwarg value is changed to 
        "all" the function will return a python dict containing the complete 
        set of attributes posted by docker-compose ps for the requested service. 
        """
        prog = "MsPASSDesktopCluster.status"
        if service in ["mspass-db","mspass-frontend","mspass-scheduler","mspass-worker"]:
            runline=[]
            runline.append("docker")
            runline.append("compose")
            runline.append("-f")
            runline.append(self.docker_configuration_file)
            runline.append("ps")
            runline.append("--format")
            runline.append("json")
            runline.append(service)
            runout=subprocess.run(runline,capture_output=True,text=True)
            if runout.stderr:
                message = prog + ":  " + "docker compose ps command failed.  Error output follows\n"
                outlines=runout.stderr.splitlines()
                for l in outlines:
                    message += l
                raise RuntimeError(message)
            else:
                outlines = runout.stdout.splitlines()
                # when running docker compose ps with a container name there should only be 2 lines
                if len(outlines) != 1:
                    message = prog + ":  " + "unexpected output from docker compose ps\n"
                    message += "Expected only one line in json format but got {}\n".len(outlines)
                    message += "Check docker documentation and consider using an older version of docker until this can be fixed"
                    raise RuntimeError(message)
                else:
                    jsonout = json.loads(outlines[0])
                    # docker ps returns a list with a dict inside on some macos versions
                    # this is needed to handle that anomaly
                    # note not json.loads issue but what docker ps spit out 
                    if isinstance(jsonout,list):
                        jsonout = jsonout[0]
                    if form=="all":
                        return jsonout
                    else:
                        return jsonout ["State"]        
        else:
            message = prog + ":  " + "invalid value for arg0={}\n".format(service)
            message += "Must be one of:  mspass-db, mspass-frontend, mspass-scheduler, or mspass-worker"
            raise RuntimeError(message)
    def shutdown(self,verbose=False):
        """
        Shut down the cluster cleanly.   
        
        The proper way to shut down a set of containers managed by docker compose is
        to run the docker compose down command.  That is what this method does.  
        """
        runline=[]
        runline.append("docker")
        runline.append("compose")
        runline.append("-f")
        runline.append(self.docker_configuration_file)
        runline.append("down")
        runout=subprocess.run(runline,capture_output=True,text=True)
        if verbose:
            print("stdout from DecktopLauncher.shutdown")
            print(runout.stdout)
            print("stderr from DesktopLauncher.shutdown")
            print(runout.stderr)
    def url(self)->str:
        """
        Runs docker-compose to extract url of jupyter server that is running. 
        
        The logs command of docker compose returns the same data echoed 
        on the command line when the container launches the jupyer server.  
        This method captures that data and parses out the url using the 
        function in this module called `extract_jupyter_url`.  
        """
        runline=[]
        runline.append("docker")
        runline.append("compose")
        runline.append("-f")
        # could test the type of configuration to be str but docker-compose 
        # will fail if this is not a valid file name
        runline.append(self.docker_configuration_file)
        runline.append("logs")
        runline.append("mspass-frontend")
        frontend_query = subprocess.run(runline,capture_output=True,text=True)
        query_out = frontend_query.stdout
        url = extract_jupyter_url(query_out)
        return url

    def __del__(self):
        """
        Class destructor. 
        
        The destructor is called when an object goes out of scope. 
        This instance is little more than a call to self.shutdown()
        which shuts down all the containers as gracefully as possible.  
        """
        self.shutdown()  
    
class MsPASSDesktopGUI:
    def __init__(self,
                  configuration="MsPASSDesktopGUI.yaml",
                  ):
        """
        Creates GUI in initial state with launch button enabled and 
        all other button's disabled.   It also instantiates an instance of 
        MsPASSDesktopCluster which is the skeleton of the engine without 
        any components running.  The launch_cluster button is required to 
        launch cluster to start processing. 
        
        This class currently doesn't use the pure construction is initialization 
        norm for OOP.   The "Run" button in this implementation launches a
        secondary window with a set of additional widgets that are added to 
        self.   That works in python even if it is a bit evil.  We probably 
        should initialize them NULL in the constructor and then let the 
        run method redefine them. 
        
        The constructor is driven by a yaml file loaded from a file 
        name defined by the configuration argument.  
        """
        # initialize this as None 
        # launch_cluster puts something valid into this variable 
        # shutdown test for None to know if it can call shutdown 
        self.engine = None

        # first read yaml file that sets defines some gui 
        # configuration data - store this in the dict self.config_data
        self.config_data = parse_yaml_file(configuration)
        self.docker_compose_filename = self.config_data["docker_compose_yaml_file"]
        self.minsize_x = self.config_data["minimum_window_size_x"]
        self.minsize_y = self.config_data["minimum_window_size_y"]
        self.log_window_size_x = self.config_data["log_window_size_x"]
        self.log_window_size_y = self.config_data["log_window_size_y"]
        self.grid_padding = self.config_data["grid_padding"]
        self.engine_startup_delay_time = self.config_data["engine_startup_delay_time"]
        self.status_monitor_time_interval = self.config_data["status_monitor_time_interval"]
        if "web_browser" in self.config_data:
            self.browser = self.config_data["web_browser"]
        else:
            self.browser = None
        self.container_run_directory = self.config_data["container_run_directory"]
        
        self.window = tk.Tk()
        self.window.title("MsPASS")

        self.window.columnconfigure(0, minsize=self.minsize_x, weight=1)
        self.window.rowconfigure(1, minsize=self.minsize_y, weight=1)
        
        # frame for top row of status Text windows
        # db status
        self.frm_status_buttons = tk.Frame(self.window, relief=tk.RAISED, bd=2)
        self.label_db_status = tk.Label(self.frm_status_buttons, text="MongoDB")
        self.label_db_status_display = tk.Label(self.frm_status_buttons,
                                      height=1,
                                      bg="red",
                                      text="Down",   # initial value - updated continuous in gui
                        )
        self.label_db_status.grid(row=0, column=0, padx=self.grid_padding, pady=self.grid_padding)
        self.label_db_status_display.grid(row=1, column=0, padx=self.grid_padding, pady=self.grid_padding)

        # scheduler status
        self.label_scheduler_status = tk.Label(self.frm_status_buttons, text="scheduler")
        self.label_scheduler_status_display = tk.Label(self.frm_status_buttons,
                                      height=1,
                                      bg="red",
                                      text="Down",   # initial value - updated continuous in gui
                        )
        self.label_scheduler_status.grid(row=0, column=1, padx=self.grid_padding, pady=self.grid_padding)
        self.label_scheduler_status_display.grid(row=1, column=1, padx=self.grid_padding, pady=self.grid_padding)
        
        # worker status
        self.label_worker_status = tk.Label(self.frm_status_buttons, text="worker")
        self.label_worker_status_display = tk.Label(self.frm_status_buttons,
                                      height=1,
                                      bg="red",
                                      text="Down",   # initial value - updated continuous in gui
                        )
        self.label_worker_status.grid(row=0, column=2, padx=self.grid_padding, pady=self.grid_padding)
        self.label_worker_status_display.grid(row=1, column=2, padx=self.grid_padding, pady=self.grid_padding)
        
        # frontend status
        self.label_frontend_status = tk.Label(self.frm_status_buttons, text="frontend")
        self.label_frontend_status_display = tk.Label(self.frm_status_buttons,
                                      height=1,
                                      bg="red",
                                      text="Down",   # initial value - updated continuous in gui
                        )
        self.label_frontend_status.grid(row=0, column=3, padx=self.grid_padding, pady=self.grid_padding)
        self.label_frontend_status_display.grid(row=1, column=3, padx=self.grid_padding, pady=self.grid_padding)
        
        self.frm_status_buttons.pack(fill=tk.BOTH)
        
        # create window showing url of jupyter server
        self.frm_url=tk.Frame(self.window, relief=tk.RAISED, bd=2)
        self.label_url = tk.Label(self.frm_url,text="Jupyter URL")
        self.text_url = tk.Text(self.frm_url,width=50,height=1)
        self.label_url.grid(row=0,column=0,padx=self.grid_padding,pady=self.grid_padding)
        self.text_url.grid(row=0,column=1,padx=self.grid_padding,pady=self.grid_padding)
        self.frm_url.pack(fill=tk.BOTH)
        
        # row of action buttons at the bottom of the main window
        self.frm_buttons = tk.Frame(self.window, 
                                    relief=tk.RAISED, 
                                    bd=2)
        self.btn_launch = tk.Button(self.frm_buttons, text="Launch",command=self.launch_cluster_callback,state="normal")
        self.btn_shutdown = tk.Button(self.frm_buttons, text="Shutdown",command=self.shutdown_callback,state="disabled")
        self.btn_jupyter = tk.Button(self.frm_buttons, text="Jupyter",command=self.launch_jupyter_callback,state="disabled")
        self.btn_diagnostics = tk.Button(self.frm_buttons, text="Diagnostics",command=self.launch_diagnostics_callback,state="disabled")
        self.btn_run = tk.Button(self.frm_buttons, text="Run",command=self.run_button_callback,state="disabled")
        self.btn_exit = tk.Button(self.frm_buttons, text="Exit", command=self.exit_callback,state="disabled")
        
        self.btn_launch.grid(row=0, column=0, sticky="ns", padx=self.grid_padding, pady=self.grid_padding)
        self.btn_jupyter.grid(row=0,column=1, sticky="ns", padx=self.grid_padding, pady=self.grid_padding)
        self.btn_diagnostics.grid(row=0,column=2, sticky="ns", padx=self.grid_padding, pady=self.grid_padding)
        self.btn_run.grid(row=0,column=3, sticky="ns", padx=self.grid_padding, pady=self.grid_padding)
        self.btn_shutdown.grid(row=0,column=4, sticky="ns", padx=self.grid_padding, pady=self.grid_padding)
        self.btn_exit.grid(row=0,column=5, sticky="ns", padx=self.grid_padding, pady=self.grid_padding)
        self.frm_buttons.pack(fill=tk.BOTH)
        self.window.mainloop()
    def launch_status_monitor(self):
        """
        As the name implies this launches the status monitoring section.
        
        Status is monitored by a set of widgets that turn Green when a service 
        is running and red if they are not running.   This method does little 
        more than setup the thread that does the actual work using the 
        algorithm self._status_monitor.  That needs to be a separate thread to 
        implement period checking that otherwise would block the GUI.  
        """
        self.monitor_stop_event = threading.Event()
        self.monitor_thread = threading.Thread(target=self._status_monitor)
        self.monitor_thread.start()
    def _status_monitor(self):
        """
        Monitor status with docker compose at an interval defined by class.
        
        This function is run on a separate thread and used to continuously 
        update the array of status labels at an interval defined by 
        the class attribute self.status_monitor_time_interval.  
        """
        # note this loop is terminated by the stop event which is shut down 
        # cleanly in the destructor
        while not self.monitor_stop_event.is_set():
            for service in ["mspass-db","mspass-scheduler","mspass-worker","mspass-frontend"]:
                match service:
                    case "mspass-db":
                        stat = self.engine.status(service)
                        self.label_db_status_display.config(text=stat)
                        if stat=="running":
                            self.label_db_status_display.config(bg="green")
                    case "mspass-scheduler":
                        stat = self.engine.status(service)
                        self.label_scheduler_status_display.config(text=stat)
                        if stat=="running":
                            self.label_scheduler_status_display.config(bg="green")
                    case "mspass-worker":
                        stat = self.engine.status(service)
                        self.label_worker_status_display.config(text=stat)
                        if stat=="running":
                            self.label_worker_status_display.config(bg="green")
                    case "mspass-frontend":
                        stat = self.engine.status(service)
                        self.label_frontend_status_display.config(text=stat)
                        if stat=="running":
                            self.label_frontend_status_display.config(bg="green")
            time.sleep(self.status_monitor_time_interval)

    
        
    def launch_cluster_callback(self):
        """
        As the name implies this launches an instance of the engine 
        for the GUI defined by the class in this module called 
        MsPASSDesktopCluster.   It launches the engine, waits a configurable 
        time, and then launches the status monitor.  Finally, it enables the 
        work other buttons and disable itself before exiting.  
        """
        self.engine = MsPASSDesktopCluster(configuration=self.docker_compose_filename)
        self.engine.launch_cluster()
        # needed because otherwise the status monitor will throw an exception if 
        # a service is not defined.  
        # TODO:  this could probably be avoided by letting undefined make status red with a different label
        time.sleep(self.engine_startup_delay_time)

        self.launch_status_monitor()
        
        self.btn_launch.config("disabled")
        self.btn_jupyter.config(state="normal")
        self.btn_diagnostics.config(state="normal")
        self.btn_run.config(state="normal")
        self.btn_shutdown.config(state="normal")
        self.btn_exit.config(state="normal")
        
    def launch_jupyter_callback(self):
        """
        callback for the Jupyter button. 
        
        It tries to launch the jupyter lab browser window. 
        It also inserts the url it resolves into the url text widget. 
        """
        self.engine.launch_jupyter_client(self.browser)
        jupyter_url = self.engine.url()
        self.text_url.insert("1.0",jupyter_url)
        
        
        
    def launch_diagnostics_callback(self):
        """
        Launches the dask diagnostics window in a browser tab.   It uses the 
        same approach as the jupyter lab launcher for resolving the browser.  
        If the Jupyter button doesn't work this one won't either. 
        """
        jupyter_url = self.engine.url()
        # if valid the url always has this magic string
        anchor = "lab?token="
        index = jupyter_url.find(anchor)
        if index < 0:
            message  = "MsPASSDesktopGUI.launch_diagnostics_callback:  malformed url\n"
            message += "parsed url for jupyter server = " + jupyter_url + "\n"
            message += "Unable to extract host url base name\n"
            message += "Try launching diagnostic window by entering the correct url in browser"
            print(message)
            return
        # search backward from index to find ":" at start of port number
        i = index
        base_url = ""
        while i > 0:
            if jupyter_url[i] == ":":
                base_url = jupyter_url[0:i]
                break
            i -= 1
        if base_url:
            diag_url = base_url + ":8787/status"
            host_os = platform.system()
            if self.browser is None:
                self.browser = get_default_browser()
            runline = []
            match host_os:
                case "Darwin":
                    runline.append("open")
                    runline.append("-a")
                    runline.append(self.browser)
                    runline.append(diag_url)
                case "Linux":
                    runline=[self.browser, diag_url]
                case "Windows":
                    print("diagnostic window launcher function - windows browser launching not yet implemented")
                    print("You will need to enter url in a browser window manually")
                    self.btn_diagnostics.config(state="disabled")
            if len(runline)>0:
                try:
                    runout=subprocess.run(runline,capture_output=True,text=True,check=True)
                except subprocess.CalledProcessError as e:
                    print(f"launch_diagnostics_callback:  subprocess run call failed with return code = {e.returncode}:")
                    print(f"Stderr from subprocess:\n{e.stderr}")
                    print("Launch diagnostic window manually as described in dask documentation")
        else:
            message  = "MsPASSDesktopGUI.launch_diagnostics_callback:  malformed url\n"
            message += "parsed url for jupyter server = " + base_url + "\n"
            message += "string does not contain expected :8888 port number in url\n"
            message += "Try launching diagnostic window by entering the correct url in browser"
            print(message)
            return
    def container_run_callback(self):
        """
        Callback function for the run button in the secondary window launched by the 
        other run button in the main gui window.   This method is what actually runs the 
        python defined in the GUI.  That file name and directory are passed to the 
        engine's run method.  Too many run levels here - this one is the middle of three
        of a call stack. 
        """
        dir = self.entry_dir_name.get()
        script_file = self.entry_script_name.get()
        if script_file:
            runline=["docker", "compose", "-f"]
            runline.append(self.docker_compose_filename)
            runline.append("exec")
            # TODO:  this probably should be a name set in the yaml file for flexibility
            runline.append("mspass-scheduler")
            runline.append("python")
            fullpath = dir + "/" + script_file
            runline.append(fullpath)
            runout = subprocess.run(runline,capture_output=True,text=True)
            print("script "+fullpath+"finished.  Stdout from run:")
            print(runout.stdout)
            if runout.stderr:
                print("stderr from script:")
                print(runout.stderr)
        else:
            print("Entry box for python file name is empty - enter a file name and push the run button again")
        
    def run_button_callback(self):
        """
        Command run when the main display run button is pushed. 
        
        This method opens a new window and build the run window gui.   
        """
        self.run_window = tk.Toplevel()
        self.frm_run_entries = tk.Frame(self.run_window, relief=tk.RAISED, bd=2)
        self.label_dir_name = tk.Label(self.frm_run_entries,text="Directory inside container where script can be found")
        self.entry_dir_name = tk.Entry(self.frm_run_entries,width=40)
        self.entry_dir_name.insert(0,"/home")
        self.label_dir_name.grid(row=0, column=0, sticky="wns", padx=self.grid_padding, pady=self.grid_padding)
        self.entry_dir_name.grid(row=0, column=1, sticky="wns", padx=self.grid_padding, pady=self.grid_padding)
        
        self.label_script_name = tk.Label(self.frm_run_entries,text="File name of python file to be run")
        self.entry_script_name = tk.Entry(self.frm_run_entries,width=40)
        self.label_script_name.grid(row=1, column=0, sticky="wns", padx=self.grid_padding, pady=self.grid_padding)
        self.entry_script_name.grid(row=1, column=1, sticky="wns", padx=self.grid_padding, pady=self.grid_padding)
        self.frm_run_entries.pack()
        
        self.btn_run_script = tk.Button(self.run_window, text="Run it", command=self.container_run_callback)
        self.btn_run_script.pack()
    def shutdown_callback(self):
        """
        Method run when the shutdown button is pushed.   
        
        It reduces the gui to the initial state.  I have't figure how to have it handle the jupyter 
        lab and dask diagnostic windows in a web browser.  The diagnostics window will still function 
        on a restart but the jupyter server will not.  
        """
        # this must be done first to kill the monitor thread
        # sleep to match time delay to be sure the monitor thread has exited before
        # the engine is stopped
        self.monitor_stop_event.set()   # needed to tell status monitor thread tp exit
        time.sleep(self.status_monitor_time_interval) 
        self.engine.shutdown()
        self.btn_launch.config(state="normal")
        self.btn_jupyter.config(state="disabled")
        self.btn_diagnostics.config(state="disabled")
        self.btn_run.config(state="disabled")
        self.btn_shutdown.config(state="disabled")
        self.btn_exit.config(state="disabled")
        self.label_db_status_display.config(bg="red",text="Down")
        self.label_scheduler_status_display.config(bg="red",text="Down")
        self.label_worker_status_display.config(bg="red",text="Down")
        self.label_frontend_status_display.config(bg="red",text="Down")
        self.engine = None
    def exit_callback(self):
        """
        Called when the exit button is pushed.  
        
        This method first calls shutdown, the closes all windows, and then 
        exits python.  
        """
        self.shutdown_callback()
        self.window.quit()
        self.window.destroy()
        exit()

    def __del__(self):
        """
        Class destructor. 
        
        The destructor is called when an object goes out of scope. 
        This instance is little more than a call to self.engine.shutdown()
        which shuts down all the containers as gracefully as possible.  
        """
        if self.engine:
            self.engine.shutdown()
    
def extract_jupyter_url(outstr)->str:
    """
    Parses output string from launching jupyer lab to extract the url 
    needed to connet to the jupyer server.
    
    Launchers can capture stdout from launching jupter with docker 
    or aptainer and use this function to return the connection url 
    to the jupyter server.  
    
    The algorithm used here is a simple search for the string "http://" 
    that the current jupyter server posts.   Output has two options and 
    the algorithm always selects the one with a ipv 4 address by veriying the 
    line has three "." characters after http://.  
    """
    test_str="http://"
    lines=outstr.splitlines()
    url_lines=[]
    for l in lines:
        if test_str in l:
            i = l.find(test_str)
            url_lines.append(l[i:])

    # select the first url with 3 or more "." symbols and assume 
    # tha is a valid ipv4 address
    for url in url_lines:
        if url.count(".")>=3:
            return url
    
    print("Error parsing jupyter server output:  returning default url with no token value")
    return "http://localhost:8888"

def mspass_yaml_file(filename)->str:
    """
    Performs a standard search for a yaml file in MsPASS.
    
    This functions standardizes
    the search for the file to be read in the same way used in the 
    MsPASS Schema constructor.   That is, if filename is set to 
    a valid string the function assumes the string defines a file 
    name in the current directory.  If that file does not exist there 
    the function tries searching for the same file in two places:
        1) $MSPASS_HOME/data/yaml is used if and only if MSPASS_HOME 
           is defined.
        2) MSPASS_INSTALL_DIR/data/yaml where MSPASS_INSTALL_DIR 
           is the top level directory of where mspass was installed 
           in your system.  This works because the installation tree
           has a data/yaml directory with defaults yaml files.
           
    Note the function does NOT test for existence of the file 
    name or path it returns.  Caller should handle that condition 
    if necessary.
    
    :param filename:   name of file to find
    :type filename:  str 
    :return:  str containg a full path to file.
    """
    if not isinstance(filename,str):
        message = "mspass_yaml_file:  arg0 must be a string defining a file name\n"
        message += "Actual type={}".format(type(filename))
        raise TypeError(message)
    # note this resolves true if filename is a file in the current 
    # directory or an absolute path to some file
    if os.path.isfile(filename):
        config_file=filename
    else:
        if "MSPASS_HOME" in os.environ:
            config_file = os.path.join(
                os.path.abspath(os.environ["MSPASS_HOME"]), 
                "data/yaml", 
                filename,
            )
        else:
            config_file = os.path.abspath(
                os.path.join(os.path.dirname(__file__), 
                             "../data/yaml", 
                             filename,
                             )
            )
    return config_file
        

def parse_yaml_file(filename=None,
                    default_file_name="MsPASSDesktopGUI.yaml")->dict:
    """
    Parses a yaml configuration file with the yaml module.  When 
    successful it returns a dict with key-value pairs that the 
    caller can parse to use as needed.  This functions standardizes
    the search for the file to be read in the same way used in the 
    MsPASS Schema constructor.   That is, if filename is set to 
    a valid string the function assumes the string defines a file 
    name in the current directory.  If that file does not exist 
    the function tries searching for the same file in two places:
        1) $MSPASS_HOME/data/haml
        2) ../data/yaml
    
    If filename is None (default) the function tries to find a 
    standard file in $MSPASS_HOME/data/yaml/`default_file_name` 
    where `default_file_name` is the value passed via that kwarg
    value.  
    
    """
    # this was derived from a similar parsing for schema.py
    if filename is None:
        fname = default_file_name
    else:
        fname = filename
        
    file_path = datafile(fname)
    
    if file_path is None:
        message = "parse_yaml_file:   yaml file=" + fname + " not found in standard search path"
        raise RuntimeError(message)
            
    try:
        with open(file_path, "r") as stream:
            result_dic = yaml.safe_load(stream)
        return result_dic
    except yaml.YAMLError as e:
        message = "parse_yaml_file:  Failure parsing configuration file={}\n".format(file_path)
        message += "Message posted:  {}".format(e)
        raise RuntimeError(e)
    except EnvironmentError as e:
        message = "parse_yaml_file:  Open failed on yaml file={}\n".format(file_path)
        message += e
        raise RuntimeError(message)
    except Exception as e:
        message="Unexpected exception thrown by yaml.safe_load\n"
        message += "Message posted: {}".format(e)
        raise RuntimeError(message)
        
def get_default_browser()->str:
    """
    Fetch default browser defined for the host operating system.
    
    Return is the name of the browser defined as the default for the 
    operating system from which this function is run.  Works only for 
    linux, windows, and macos (darwin).  If any of the os specific 
    tests fail for any reason the functionr turns a global default 
    that is currently set as "FireFox".
    """
    # Use this if any of anything goes wrong in os specific code blocks
    failure_browser_name="FireFox"
    host_os = platform.system()
    match host_os:
        case "Linux":
            try:
                process = subprocess.Popen(['xdg-settings', 'get', 'default-web-browser'], stdout=subprocess.PIPE)
                output, error = process.communicate()
                if error:
                    raise Exception(error)
                default_browser = output.decode('utf-8').strip()
            except FileNotFoundError:
                print("xdg-settings not found. Cannot determine default browser.")
                print("Using global default=",failure_browser_name)
                default_browser = failure_browser_name
            except Exception as e:
                print(f"Error getting default browser: {e}")
                print("Using global default=",failure_browser_name)
                default_browser = failure_browser_name
        case "Darwin":
            # obtained from google - does not work with current macos
            try:
                process = subprocess.Popen(['osascript', '-e', 'POSIX path of (get frontmost application)'], stdout=subprocess.PIPE)
                output, error = process.communicate()
                if error:
                    raise Exception(error)
                default_browser = output.decode('utf-8').strip()
            except Exception as e:
                print(f"Error getting default browser: {e}")
                print("Using global default=",failure_browser_name)
                default_browser = failure_browser_name
        case "Windows":
            import winreg
            try:
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\http\shell\open\command")
                default_browser = winreg.QueryValueEx(key, None)[0]
                default_browser = default_browser.split('"')[1] if '"' in default_browser else default_browser.split()[0]
                winreg.CloseKey(key)
            except FileNotFoundError:
                print("Default browser information not found.")
                print("Using global default=",failure_browser_name)
                default_browser = failure_browser_name
        case _:
            print("Do not know how to handle host operating system with name="+host_os)
            print("Using global default=",failure_browser_name)
            default_browser = failure_browser_name
    return default_browser
            

