#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created May 2025

@author: Gary L Pavlis

This file contains a base class for a cluster launcher.
It has a complete implementation for HPC clusters called
HPCClusterLauncher.   It is designed to use a configuration
file to to create the services needed to run a job on an HPC
cluster.  It is mainly designed for batch processing, but the
interactive_session method can be used to print an output from
launching the jupyter server that has comparable functionality to
the original shell scripts used for launching mspass.
"""
from abc import ABC, abstractmethod
import yaml
import subprocess

# import mock_subprocess as subprocess
import copy
from mspass_launcher.util import datafile


class BasicMsPASSLauncher(ABC):
    """
    Base class constructor loads common attribute from a yaml file.

    The base class should read attributes to be set in self that are
    common to all superclasses.  Superclasses should read the same file and
    parse additional attributes not in the base class.

    For convenience the dictionary created from the yaml file is
    stored as self.yaml.dict.   That allows superclasses to not have
    reload the yaml file by running super()._init__ with a yaml
    file.   When that is done additional attributes can be parsed from
    self.yaml_dict.
    """

    def __init__(
        self,
        configuration_file,
    ):
        """
        Base class constructor loading core attributes.

        This constructor loads common attributes or superclass launchers.
        The expectation is superclasses will normally contain a file-based
        constructor and the thing superclasses do is call this
        method with the super().__init__ python idiom.

        :param configuration file:  file name of yaml file to
          to loaded.   Uses the default rules of the
          mspass_launcher.util datafile function to find the file
          in a default search path.  See docstring for that
          function.
        """
        file_path = datafile(configuration_file)
        self.yaml_dict = self._parse_yaml_file(file_path)
        self.container = self.yaml_dict["container"]
        self.working_directory = self.yaml_dict["working_directory"]
        self.log_directory = self.yaml_dict["log_directory"]
        self.database_directory = self.yaml_dict["database_directory"]
        self.worker_directory = self.yaml_dict["worker_directory"]
        self.workers_per_node = self.yaml_dict["workers_per_node"]
        if "worker_memory_limit" in self.yaml_dict:
            self.worker_memory_limit = self.yaml_dict["worker_memory_limit"]
        else:
            self.worker_memory_limit = None
        self.primary_node_workers = self.yaml_dict["primary_node_workers"]
        self.cluster_subnet_name = self.yaml_dict["cluster_subnet_name"]

    def _parse_yaml_file(self, file_path) -> dict:
        """
        Parses the yaml configuration file for this class and returns
        the result as  python dictionary.  The dictionary returned
        is saved as a self variable so superclasses can parse additional
        attributes without the baggage of reading and parsing the file
        again.   A bit unusual but workable in this case because
        configuration files are never expected to be large so storing
        """
        try:
            with open(file_path, "r") as stream:
                result_dic = yaml.safe_load(stream)
            return result_dic
        except yaml.YAMLError as e:
            print(f"Failure parsing configuration file={file_path}")
            print(f"Message posted: {e}")
            raise RuntimeError("HPCClusterLauncher Constructor failed")
        except EnvironmentError as e:
            print(f"Open failed on yaml file={file_path}")
            print(f"Message posted: {e}")
            raise RuntimeError("HPCClusterLauncher Constructor failed")
        except Exception as e:
            print("Unexpected exception thrown by yaml.safe_load")
            print(f"Message posted: {e}")
            raise RuntimeError("HPCClusterLauncher Constructor failed")

    @abstractmethod
    def launch(self):
        """
        Concrete implementations should implement this method that
        launches all the required MsPASS components.
        """
        pass

    @abstractmethod
    def status(self):
        """
        Concrete implementations should implement this method that
        returns some form of status information that a master script
        can use to verify all the mspass components are functioning.
        """
        pass

    @abstractmethod
    def run(self, python_file):
        """
        Concrete implementations should implement this method that
        runs the scritp in python_file on the MsPASS cluster managed
        by the object.
        """
        pass


class HPCClusterLauncher(BasicMsPASSLauncher):
    """
    Launcher to run on an HPC cluster.

    This class provides a mechanism to run a containerized version of
    changing only the configuratin file.   The job schduler enter only
    in tryig to grok the list of compute nodes assigned to a job.

    This class acts similar to shell-script launcers for HPC developed
    at TACC.   By using python, however, is is more configurable and and
    has some added features.  There are currently three major enhancement this
    implmntation adds oaver the shell script approach:
        1.  The same laucher works for single node and multimode jobs
            with the same configuration.  It does that by automatically
            launching workers on the primary node if slurm says there is
            only one node allocated for the job.
        2.  The containers are managed much more cleanly as subprocesses
            spawned on the primary node by an instance of this launcher.
            That provides a cleaner exit when the job finishes.
        3.  It extends the base class by adding a "terminate" method
            which can be used to have the containers exit gracefully.
            It also provides a mechanism to relaunch a cluster in a
            different configuration in the middle of a job.  Not as helpful
            as it could be with slurm because resources are allocated at the
            start of the job and are fixed for the duration of "a job".
    """

    def __init__(
        self,
        configuration_file="HPCClusterLauncher.yaml",
        auto_launch=True,
        verbose=False,
    ):
        """
        Build an instance of this class from a yaml file.

        This classes uses the base class constructor to parse the
        actual yaml file.  It expects to find a dictionary
        it can fetch with the key "HPC_cluster" containing attributes
        specific to this class.  (what MongoDB would call a subdocument)
        That approach more cleanly separates what attributes are
        needed only by this superclass.  It also allows alternative
        implementations that are variants of this to be used with the
        same site-specific configuration file with alternative keys.
        i.e. a user should feel free to implement a variant of this
        launcher but the key.  Then this or the alternative can be
        run using a common configuration files.

        Key-value pairs in the yaml file that define the configuration
        to use are best documented separately.  See User's Manual
        (TODO:  not yet written)

        :param configuration file:  file name of yaml file to
          to loaded.   Note this string should normally be a
          file in the working directory which the python interpreter
          instantiating an instance of this class is run.  Alternatively
          you can specify a full path for the file.  In that case
          the function will detect that fact and use that full path.
          If undefined (None) an Antelope like approach is tried wherein
          the constructor will check if the env MSPASS_HOME is defined
          and if it is it looks there for a file called "mspass_cluster.yaml".
          If MSPASS_HOME is not defined, it checks for the default file
          name ("mspass_cluster.yaml") in ../data/yaml.
        :type coniguration_file: string
        :param auto_launch:   boolean that when set True (default) will
          call the `launch` method if the construtor completes without error.
          This is the default as it makes the object follow the common
          OOP recommendation that "constrution is initialization"
          Similarly the object as a destuctor defined that automatically
          releases resources the object manages (in this case the containerized
          componnts) when it goes out of scope.
        :param verbose:  When True print out information useful for
          debugging a configuration issue.   Use when setting up
          a new configuration to verify it is what you want.  It will also 
          make all method calls verbose unless explicitly silenced by 
          setting the method kwarg verbose arg to False.

        """
        message0 = "HPCClusterLauncher constructor:  "
        if verbose:
            print("Loading configuration file=", configuration_file)
            self.verbose = True
        else:
            self.verbose = False
        super().__init__(configuration_file)
        # The base class constructor creates this image of the yaml
        # file.  It only extracts common attributes.  Here we
        # translate that external representation to attributes needed
        # for this concrete implementation
        cluster_config = self.yaml_dict["HPC_cluster"]
        self.container_run_command = cluster_config["container_run_command"]
        self.container_run_args = cluster_config["container_run_args"]
        self.container_env_flag = cluster_config["container_env_flag"]
        # at present this is local version of mpiexec
        self.remote_worker_run_command = cluster_config["remote_worker_run_command"]
        # we need to allow this to be optional - default to none
        if "mpi_arg_list" in cluster_config:
            self.mpi_arg_list = cluster_config["mpi_arg_list"]
        else:
            self.mpi_arg_list = None
        self.task_scheduler = cluster_config["task_scheduler"]
        
        acceptable_implementations=["openmpi","mpich","tacc"]
        self.mpi_implementation = cluster_config["mpi_implementation"]
        if self.mpi_implementation not in acceptable_implementations:
            message = "HPCClusterLauncher constructor:  "
            message += "Illegal value for mpi_implementation={}\n".format(self.mpi_implementation)
            message += "Must be one of:  " + str(acceptable_implementations)
            raise ValueError(message)

        # This last complex block sets hostnames that
        # define the MsPASS frameworK;  database, scheduler, workers, and primary
        # note primary as a minimum means the host to run the python/jupyter
        # script
        js = cluster_config["job_scheduler"]
        if js == "slurm":
            if verbose:
                print("job scheduler set as slurm")
            ph = cluster_config["primary_host"]
            dbh = cluster_config["database_host"]
            sh = cluster_config["scheduler_host"]
            wh = cluster_config["worker_hosts"]
            self.hostlist_filename = cluster_config["hostlist_filename"]
            if "remote_processes_per_node" in cluster_config:
                ppn = cluster_config["remote_proceses_per_node"]
            else:
                # Use this to mean leave this blank in hostname file
                ppn = None
            if (ph == "auto") or (dbh == "auto") or (sh == "auto") or (wh == "auto"):
                # this executes a slurm command to fetch nodes assigned to
                # this job
                runline = ["scontrol", "show", "hostname"]
                comout = subprocess.run(
                    runline,
                    capture_output=True,
                    text=True,
                )
                hostlist = comout.stdout.split()

                print("Debug: hostlist returned by scontrol=",hostlist)
                if len(hostlist) <= 1:
                    if self.primary_node_workers == 0:
                        message = message0
                        message += (
                            "scontrol command yielded an empty list of hostnames\n"
                        )
                        message += "Cannot continue"
                        raise RuntimeError(message)
                    else:
                        comout = subprocess.run(
                            ["hostname"], capture_output=True, text=True
                        )
                        hostlist = [comout.stdout]
                        # set as an empty list instead of a None - less confusing
                        self.remote_workers = list()
                else:
                    print("Debug:  calling _write_hostlist method with hostlist=",hostlist)
                    self.remote_workers = self._write_hostlist(hostlist,ppn)
                    print("Debug:   list of remote hosts returned by that method=",self.remote_workers)
                # comout contans a list of host names. By default for
                # auto use the first in the list as primary
                primary = hostlist[0].strip()  # needed because of appended newline
                if ph == "auto":
                    self.primary_node = copy.deepcopy(primary)
                else:
                    self.primary_node = ph
                if dbh == "auto":
                    self.database_host = copy.deepcopy(primary)
                else:
                    self.database_host = dbh
                if sh == "auto":
                    self.scheduler_host = copy.deepcopy(primary)
                else:
                    self.scheduler_list = sh
                self.worker_hosts=list()
                # this gets a bit complicaed for input flexibility of 
                # worker host list - default will be auto but allow
                # list of names as command separated string or a 
                # list created from the yaml file.  They differ in 
                # syntax only in that a list is created if enclosed in []
                if isinstance(wh,str):
                    if "," in wh:
                        # accept a comma separated string listing hosgts
                        # eg. "ch1,ch2,ch3"
                        self.worker_hosts=wh.split(",")
                    elif wh == "auto":
                        # note worker_hoss exclude primary
                        for i in range(1, len(hostlist), 1):
                            self.worker_hosts.append(
                                hostlist[i].strip()
                                )  # strip needed to remove newline

                        if len(self.worker_hosts) <= 0 and self.primary_node_workers == 0:
                            message = message0
                            message += "Illegal configuration\n"
                            message += "scontrol  returned only a single hostname "
                            message += "but primary_node_workers was set to 0\n"
                            message += "To run on a single node set primary_node_workers to a postive value\n"
                            message += "To run on multiple nodes change your slurm commands at the top of this job"
                            raise RuntimeError(message)
                    else:
                        # assume there is one node listed only as wh
                        self.worker_hosts.append(wh)
     
                elif isinstance(wh,list):
                    # accept yaml input as a list of hostnames
                    for host in wh:
                        self.worker_hosts.append(host)
                    
                if verbose:
                    print("Primary node name=", self.primary_node)
                    print("database hostname=", self.database_host)
                    print("scheduler hostname=", self.scheduler_host)
                    print("Worker hostname(s)=", self.worker_hosts)
            if cluster_config["setup_tunnel"]:
                s = cluster_config["tunnel_setup_command"]
                print(
                    "Attempting to set up ssh communication tunnel to node=",
                    self.primary_node,
                )
                print("Using this command line: {} {}".format(s, self.primary_node))
                # IMPORTANT:  actual implementation requires last arg
                # to be primary hostname
                arglist = s.split()
                arglist.append(self.primary_node)
                comout = subprocess.run(runline, capture_output=True, text=True)
                print(
                    "Successfully created tunnels to allow connection to ",
                    self.primary_node,
                )
            # these are set by the launch method but it is good practice
            # to initialize them here
            self.scheduler_process = None
            self.dbserver_process = None
            self.primary_worker_process = None
            self.remote_worker_process = None
            self.jupyter_process = None
            if auto_launch:
                self.launch(verbose=verbose)
        else:
            message = message0
            message += "Cannot handle job_scheduler={}\n".format(js)
            message += "Currently only support slurm"
            raise ValueError(message)

    def __del__(self):
        """
        Class destructor.

        The destrutor is called when an object goes out of scope.
        This instance is little more than a call to self.shutdown()
        which shuts down all the containers as gracefully as possible.
        """
        self.shutdown()

    def launch(self, verbose=None):
        """
        Call this method to launch all the MsPASS containerized components.

        The MsPASS framework requires three containerized components to
        be running to work correctly:  (1) scheduler, (2) workers, and (3)
        an instance of MongoDB.  This method launches those components using
        instructions parsed from a configuration file when the object is
        constructed.   The components are spawned as subprocesses from the
        primary node with the subprocess.Popen function.   That runs the
        containers in the background with process information cached in this
        object as self attibutes called "self.scheduler_process",
        "self.dbserver_process", and "self.remote_worker_process".
        If workers are run on the primary there will also be a defined
        valued for "self.primary_worker_process".
        
        Set boolean verbose to override object global verbose setting
        """
        if verbose:
            verbose = self.verbose
        runline = self._initialize_container_runargs()
        runline.append(self.container_env_flag)
        envlist = "MSPASS_ROLE=scheduler,MSPASS_WORK_DIR={}".format(
            self.working_directory
        )
        envlist += ",MSPASS_SCHEDULER={}".format(self.task_scheduler)
        envlist += ",MSPASS_SCHEDULER_ADDRESS={}".format(self.primary_node)
        runline.append(envlist)
        runline.append(self.container)
        # We have to use this lower level function in subprocess
        # for two reason:  (a) nonblocking launch to run the container
        # from a new process and (b) keeping the output allows graceful
        # shutdown in the shutdown method
        self.scheduler_process = subprocess.Popen(
            runline,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        if verbose:
            print("Successfully launched scheduler")
            print("launch line:")
            print(runline)
        # now do a similar thing for database
        # note this implementation doesn't handle shrarding
        runline = self._initialize_container_runargs()
        runline.append(self.container_env_flag)
        envlist = "MSPASS_ROLE=db,"
        envlist += "MSPASS_WORK_DIR={},".format(self.working_directory)
        envlist += "MSPASS_DB_DIR={}".format(self.database_directory)
        runline.append(envlist)
        runline.append(self.container)

        self.dbserver_process = subprocess.Popen(
            runline,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        if verbose:
            print("Successfully launched db")
            print("launch line:")
            print(runline)
        # Now launch workers on hosts that are not primaary host
        #worker_run_args = self._build_worker_run_args()
        if len(self.worker_hosts) > 0:
            print("launching workers on remote hosts=",self.worker_hosts)
            worker_run_args=list()
            worker_run_args.append(self.remote_worker_run_command)
            worker_run_args.append("-hostfile")
            worker_run_args.append(self.hostlist_filename)
            if self.mpi_arg_list:
                for arg in self.mpi_arg_list:
                    worker_run_args.append(arg)
            # this private function creates the list of args to 
            # launch mspass container as the run argument
            container_args = self._build_worker_run_args()
            for arg in container_args:
                worker_run_args.append(arg)
            print("Debug - remote worker launch string to test")
            s=""
            for a in worker_run_args:
                s += a
                s += " "
            print(s)
            self.remote_worker_process = subprocess.Popen(
                #worker_run_args,
                s,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            if verbose:
                print("Successfully launched remote node workers")
                print("launch line:")
                print(worker_run_args)
        # do not trap error of no worker nodes and no workers
        # assigned to primary - assume constructor traps that condition.
        if self.primary_node_workers > 0:
            # we have to launch this container differently soo
            # we have to prepare a somewhat different run line
            # could not make this work with worker_arg unless we used
            # shell=True.   In that case we build a command line instead of
            # a list like runline
            runline = self._initialize_container_runargs()
            srun = ""
            for s in runline:
                srun += s
                srun += " "
            srun += self.container_env_flag
            srun += " "
            envlist = "MSPASS_ROLE=worker,"
            envlist += "MSPASS_WORK_DIR={},".format(self.working_directory)
            # envlist += "MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host)
            envlist += "MSPASS_SCHEDULER_ADDRESS={},".format(self.scheduler_host)
            envlist += "MSPASS_DB_ADDRESS={},".format(self.database_host)
            envlist += 'MSPASS_WORKER_ARG="--nworkers={} --nthreads 1"'.format(
                self.primary_node_workers
            )
            srun += envlist + " " + self.container
            self.primary_worker_process = subprocess.Popen(
                srun,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            if verbose:
                print(
                    f"Successfully launched {self.primary_node_workers} workers on primary node"
                )
                print("launch line:")
                print(srun)

        # Exit immmeditaly if any of the contaienrs  have exited
        if self.status(verbose=False) == 0:

            def stat_message(c):
                if self.status(container=c, verbose=False):
                    m = c + " container is running\n"
                else:
                    m = c + "container is NOT running\n"
                return m

            message = "HPCClusterLauncher:  cluster initiation failed\n"
            for con in ["db", "scheduler", "primary_worker"]:
                m = stat_message(con)
                message += m
            raise RuntimeError(message)

    def shutdown(self):
        """
        Shut down the services containers gracefully.

        This method should always be called before exiting a python
        job when the script is finished using mspass.  It shuts the
        cluster containers down cleanly using the Popen method
        called terminate.   If terminate files the handler use a kill.
        """
        if self.verbose:
            self.status(verbose=True)
            def print_process_outputs(process,service_name):
                """
                Inline function used to standardize printing of 
                stdout and stderr in verbose mode.  Assumes process is
                the return from a call to Popen and the process has 
                already been terminated.  Be sure that is true of the 
                program will hang on the call to the communicate method.
                i.e. it will wait until the process exits which would 
                be never in the context of this method.
                """
                stdout,stderr = process.communicate()
                print("STDOUT from service = ",service_name)
                print(stdout.decode('utf-8'))
                print("STDERR from service = ",service_name)
                print(stderr.decode('utf-8'))
        if self.jupyter_process:
            try:
                self.jupyter_process.terminate()
                self.jupyter_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(
                    "Jupyter notebook server (frontend) did not respond to terminate method"
                )
                print("Reverting to less graceful kill")
                self.jupyter_process.kill()
            if self.verbose:
                print_process_outputs(self.jupyter_process,"jupyter server")
        if self.remote_worker_process:
            try:
                self.remote_worker_process.terminate()
                self.remote_worker_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(
                    "mpirun driving worker container on remote nodes did not respond to terminate method"
                )
                print("Reverting to less graceful kill")
                self.remote_worker_process.kill()
            if self.verbose:
                print_process_outputs(self.remote_worker_process,"remote_workers")
        if self.primary_worker_process:
            try:
                self.primary_worker_process.terminate()
                self.primary_worker_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print(
                    "Worker container on primary node did not respond to terminate method"
                )
                print("Reverting to less graceful kill")
                self.primary_worker_process.kill()
            if self.verbose:
                print_process_outputs(self.primary_worker_process,"primary_node_workers")
        # terminate the scheduler
        try:
            self.scheduler_process.terminate()
            self.scheduler_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("Scheduler container did not respond to terminate method")
            print("Reverting to less graceful kill")
            self.scheduler_process.kill()
        if self.verbose:
            print_process_outputs(self.scheduler_process,"scheduler")
        # now database - should always be running so no need for None test
        try:
            self.dbserver_process.terminate()
            self.dbserver_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("Database server container did not respond to terminate method")
            print("Reverting to less graceful kill")
            self.dbserver_process.kill()
        if self.verbose:
            print_process_outputs(self.dbserver_process,"MonogDB_server")

    def run(self, pyscript):
        """
        Runs pyscript the primary node using this cluster.

        This method runs a python script on the primary node.
        It always runs in batch mode and assumes a python script
        s the input.  We need a different method to run jupyter
        notebooks.  Blocks until the script exits.
        """
        # this can be made more elaborate.  Here I just run
        # a script
        print("Trying to run python script file=", pyscript)
        if self.verbose:
            print("Status of services running when this job was launched")
            self.status(verbose=True)
            print("/////////////////////////")
            print("Starting run process")
        runline = []
        # I am going to hard code this for now
        runline.append("apptainer")
        runline.append("run")
        crarg = self.container_run_args.split()
        for arg in crarg:
            runline.append(arg)
        runline.append("--env")
        envlist = "MSPASS_ROLE=frontend"
        envlist += ",MSPASS_WORK_DIR={}".format(self.working_directory)
        envlist += ",MSPASS_DB_ADDRESS={}".format(self.database_host)
        envlist += ",MSPASS_SCHEDULER_ADDRESS={}".format(self.scheduler_host)
        runline.append(envlist)
        runline.append(self.container)
        runline.append("--batch")
        runline.append(pyscript)
        print("running script file= ", pyscript)

        runout = subprocess.run(runline, capture_output=True, text=True)
        print("stdout from this job")
        print(runout.stdout)
        print("stderr from this job")
        print(runout.stderr)

    def interactive_session(self):
        """
        Use this method to launch the jupyter server to initiate an
        interactive session.  Will print the output from jupyter
        when it launches to use current cut-paste method to connect to
        the jupyter server.
        """
        print("Launching frontend container running juptyer server")
        print("Use cut-and-paste of url printed below to connect")
        runline = self._initialize_container_runargs()
        runline.append("--env")
        envlist = "MSPASS_ROLE=frontend,"
        envlist += "MSPASS_WORK_DIR={}".format(self.working_directory)
        runline.append(envlist)
        runline.append(self.container)
        self.jupyter_process = subprocess.Popen(
            runline, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = self.jupyter_process.communicate()
        print(stdout)
        print(stderr)

    def status(self, container="all", verbose=None) -> int:
        """
        Check the status of one or more of the containers managed by this object.

        We often need to know if a container is still running.   This method
        allows one to check if the required contaienrs to run mspass are
        running.  By default it checks all containers.  One can ask for only
        one using one of the key strings this function uses to define the
        instance of the mspass container.  Valid values for arg0 are:
            "all" - check all
            "db"  - check only the container running MongoDB
            "scheduler" - check only the contaner running the dask or
                or spark scheduler
            "primary_worker" - check status of the worker container running on
                the primary node.  Note there is currently no support for
                workers spwaned on other nodes.

        Any other values for arg0 will cause this method to throw a
        ValueError exception.

        :param container: container keywords noted above for arg0.  i.e.
           must be one of "db","scheduler", "primary_worker", or "all" (default)
        :type container:  string
        :param verbose:  boolean that when True (default) uses print to
           post a status message for container(s) requested.  When false
           prints nothing and assumes the return will be handled
        :return:  int status.  1 means the container(s) tested were all
           running.  0 means one or more have died.
        """
        if verbose is None:
            verbose = self.verbose
            
        all_containers = ["db", "scheduler", "primary_worker", "remote_workers"]
        if container == "all":
            statlist = all_containers
        else:
            if container in all_containers:
                statlist = [container]
            else:
                message = "HPCClusterLauncher.status:  component={}".format(container)
                message += " invalid\n"
                message += "Must be one of: "
                for c in all_containers:
                    message += c + " "
                raise ValueError(message)

        def verbose_message(container_name, poll_return):
            if poll_return is None:
                print(container_name, " is running")
            else:
                print(container_name, " has exited with code=", poll_return)

        retval = 1
        for container in statlist:
            if container == "db":
                stat = self.dbserver_process.poll()
            elif container == "scheduler":
                stat = self.scheduler_process.poll()
            elif container == "primary_worker":
                # this can be None if number workers set 0
                if self.primary_worker_process is not None:
                    stat = self.primary_worker_process.poll()
                else:
                    print("No primary worker process was launched")
            elif container == "remote_worker":
                if self.remote_worker_process is not None:
                    stat = self.remote_worker_process.poll()
                else:
                    print("No remote worker process was launched")
            if verbose:
                verbose_message(container, stat)
            if stat:
                retval = 0

        return retval

    def _initialize_container_runargs(self) -> list:
        """
        This private method creates the initial list of args
        used to run a container driven by two key-value pairs
        in the configuration file:  "container_run_command" and
        "conainer_run_args".   There are two because the first is
        commonly just "apptaier run" while the second may contain
        optional run args like bind arguments.   Note in this
        class environment variables are always handled separately.

        Returns a list that is is the starting point for the list of
        args used for subprocess.run and subprocess.Popen.
        """
        crargs = []
        rtmp = self.container_run_command.split()
        for arg in rtmp:
            crargs.append(arg)
        rtmp = self.container_run_args.split()
        for arg in rtmp:
            crargs.append(arg)
        return crargs
    def _write_hostlist(self,allhosts,ppn)->list:
        if len(allhosts)<=1:
            message = "HPCClusterLauncher._write_hostlist:  coding error. length of hostlist received less than 2"
            raise ValueError(message)
        # create list of only the remote nodes 
        # created here as all but the 0 component
        remote_nodes=list()
        for i in range(1,len(allhosts)):
            remote_nodes.append(allhosts[i])
        with open(self.hostlist_filename,"w") as fp:
            if ppn:
                # this should use the new match-case construct but 
                # too many HPC systems default python is older than 3.10
                # so we use an if elif sequenc here
                outlines=list()
                if self.mpi_implementation == "openmpi":
                    for host in remote_nodes:
                        s = "{} slots{}".format(host,ppn)
                        outlines.append(s)
                elif self.mpi_implementation == "mpich":
                    for host in remote_nodes:
                        s = "{}:{}".format(host,ppn)
                        outlines.append(s)
                elif self.mpi_implementation == "tacc":
                    print("HPCClusterLauncher:  ppn node ignored when mps_implementation is tacc")
                    print("At TACC all nodes are exclusive access")
                    for host in remote_nodes:
                        outlines.append(host)
                else:
                    message = "HPCClusterLauncher._write_hostlist:  "
                    message += "illegal value for self.mpi_implementation.\n"
                    message += "Coding error - this should have been caught in the constructor"
                    raise ValueError(message)
                fp.writelines(outlines)
            else:
                # there is not implementation dependency unless 
                # ppn (processors per node) is requested.
                # in this case we just write node names
                fp.writelines(remote_nodes)
        return remote_nodes

    def _build_worker_run_args(self) -> list:
        """
        Private method that constructs the command to be run by 
        mpirun on worker nodes.  It returns a list of arguments 
        that should be appended to the arglist that begins 
        with self.remote_launch_command (usually "mpirun")
        """
        # simillar to launch method to generate run  line for container
        arglist=list()
        for arg in self.container_run_command.split():
            arglist.append(arg)
        for arg in self.container_run_args.split():
            arglist.append(arg)
        # apptainer mthod for setting environment variables loaded
        # in contaer
        arglist.append('--env')
        envlist = 'MSPASS_ROLE=worker,'
        envlist += 'MSPASS_WORK_DIR={},'.format(self.working_directory)
        envlist += 'MSPASS_SCHEDULER_ADDRESS={},'.format(self.scheduler_host)
        # testing only - removed comma
        envlist += 'MSPASS_DB_ADDRESS={},'.format(self.database_host)
        #envlist += 'MSPASS_DB_ADDRESS={},'.format(self.database_host)
        #envlist += 'MSPASS_WORKER_ARG='--nworkers={} --nthreads 1''.format(
        #    self.workers_per_node
        #)
        # trying this to split up worker_arg
        envlist += 'MSPASS_WORKER_ARG="'
        envlist += '--nworkers={} '.format(self.workers_per_node)
        envlist += '--nthreads=1"'
        if self.worker_memory_limit:
            # need leading comma here as this one is optional
            # if left trailing the last it would be a syntax error
            envlist += ",MSPASS_DASK_WORKER_MEMORY_LIMIT={}".format(self.worker_memory_limit)    
        arglist.append(envlist)
        arglist.append(self.container)
        return arglist
