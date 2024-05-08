"""
hyakqos contains the QosResourceQuery class, and the QosResource class
"""

from collections import defaultdict
import re
import subprocess
import pwd
import grp
import sys
from rich.console import Console
from rich.table import Table
from rich import box

class QosResource:
    """
    The QosResource class performs an scontrol query for the QOS name given
    on instantiation. The data that gets generated on instantiation is:
    string: account, string: partition, dictionary: resource_data.
    """
    # resource_pattern regex has 3 groups:
    # a word and two numbers in the format "word=numbers(numbers)"
    resource_pattern = re.compile(r"(\w*)=(\d*|N*)\((\d*)\)")
    # grptres_pattern regex has 2 groups:
    # 1. a line starting with "   GrpTRES=" and 2. the rest of that line
    grptres_pattern = re.compile(r"(?:\s*GrpTRES=)(.*)")

    def __init__(self, qos_name: str):
        self.qos_name = qos_name
        if "-" in self.qos_name:
            self.account, qos_suffix = self.qos_name.split('-', 1)
            if qos_suffix.endswith("mem"):
                self.partition = f"compute-{qos_suffix}"
            else:
                self.partition = qos_suffix
        else:
            self.account = self.qos_name
            self.partition = "compute"
        self.resource_data = {}
        self.__query_qos()

    def __query_qos(self):
        scontrol_flags = ["scontrol", "show", "assoc_mgr", "flags=qos", "qos=" + self.qos_name]
        scontrol_output = subprocess.run(scontrol_flags, capture_output=True,
            encoding='utf-8', check=False).stdout
        grptres_line = re.search(self.grptres_pattern, scontrol_output)
        resource_list = re.findall(self.resource_pattern, grptres_line[1])
        for item in resource_list:
            total = int(item[1].replace('N', '0'))
            used = int(item[2])
            free = total - used
            self.resource_data[item[0]] = { "total" : total,
                                       "used"  : used,
                                       "free"  : free }

class QosResourceQuery:
    """
    The QosResourceQuery class takes a query type (user|group) and a search term.
    Use the run_query() method to perform the sacctmgr query based on that data. This
    will create a list (qos_list) and a resource dictionary (qos_resoure_dict), which
    can be accessed via e.g.:
    instance.qos_resource_dict["uwit"].resource_data["cpu"]["total"]) or
    instance.qos_resource_dict["uwit"].account or
    instance.qos_resource_dict["uwit"].partition
    """
    def __init__(self, query_type: str, query_search_term: str, query_cluster: str):
        self.query_type = query_type
        self.query_search_term = query_search_term
        self.query_cluster = query_cluster
        self.qos_list = []
        self.qos_resource_dict = {}
        self.query_partition = ""
        self.partition_filter = False
        self.print_ckpt = False
        self.ckpt_free_cpu = ""
        self.ckpt_free_gpu = ""
        self.ckpt_job_limit = ""
        self.debug = False
        if self.query_type == "user" or self.query_type == "group":
            self.__validate_query_search_term()

    def debugging(self, debugopt: bool):
        """
        Send true/false to enable/disable debugging.
        Input: boolean
        """
        self.debug = debugopt

    def __validate_query_search_term(self):
        search_term_without_dashes_or_underscores = ''.join([c for c in self.query_search_term if c not in '-_'])
        if not search_term_without_dashes_or_underscores.isalnum():
            if self.debug:
                raise ValueError("Input should be alphanumeric, no spaces or special characters.")
            else:
                print("Error: Input should be alphanumeric, no spaces or special characters.")
                sys.exit(1)
        if self.query_type == "user":
            try:
                pwd.getpwnam(self.query_search_term)
            except KeyError as error:
                if self.debug:
                    raise KeyError("Error: User '%s' not found." % self.query_search_term) from error
                else:
                    print("Error: User '%s' not found." % self.query_search_term)
                    sys.exit(1)
        if self.query_type == "group":
            try:
                grp.getgrnam(self.query_search_term)
            except KeyError as error:
                if self.debug:
                    raise KeyError("Error: Group '%s' not found." % self.query_search_term) from error
                else:
                    print("Error: Group '%s' not found." % self.query_search_term)
                    sys.exit(1)


    def __filter_by_cluster(self, sacctmgr_output):
        """
        The input here is a multiline string with lines that look like:
            cluster|uwit,uwit-bigmem,uwit-gpu-2080ti
            klone|uwit,uwit-bigmem,uwit-gpu-2080ti
        So here's the process.
            1. Create a list from the lines of the string when:
                a. the line begins with the cluster we query for ("klone")
            2. Remove the prefix and pipe (so, "klone|")
            4. Join the separate lines with a comma.
            5. Split all elemenets (by comma) into a new list.
        """
        qos_list_by_cluster = ','.join(
            [ qos_line.removeprefix(self.query_cluster + "|")
                for qos_line in sacctmgr_output.split()
                if qos_line.startswith(self.query_cluster + "|")]
            ).split(',')
        return qos_list_by_cluster

    def __sacctmgr_query(self):
        """
        This method runs the sacctmgr query, filters by cluster,
        and returns the raw QOS list.
        """
        sacctmgr_flags = ["sacctmgr", "show", "user", "-nPs", "format=cluster,qos"]
        if self.query_type == "user":
            index = sacctmgr_flags.index("user") + 1
            sacctmgr_flags.insert(index, self.query_search_term)
        sacctmgr_output = subprocess.run(sacctmgr_flags, capture_output=True,
            encoding='utf-8', check=False).stdout
        qos_list_by_cluster = self.__filter_by_cluster(sacctmgr_output)
        return qos_list_by_cluster

    def __filter_out_ckpt(self, qos_name):
        """
        Takes a qos name, and returns False if it's 'ckpt' or 'normal'
        """
        if "ckpt" in qos_name or "normal" in qos_name:
            return False
        return True

    def __filter_by_group(self, qos_name):
        """
        Takes a qos name, and returns False when it's a group query AND
        the qos name doesn't start with the search term.
        """
        if self.query_type == "group":
            if not qos_name.startswith(self.query_search_term):
                return False
        return True

    def __filter_by_partition(self, qos_name):
        """
        Takes a qos name, and returns False when the partition filter is on AND
        the partition name isn't at the end of the qos name. It also returns
        False when the partition is "compute" and the qos name has any "-" in it
        """
        if self.partition_filter:
            if self.query_partition == "compute":
                if "-" in qos_name:
                    return False
            elif not qos_name.endswith(self.query_partition):
                return False
        return True

    def __filter_qos_list(self, unfiltered_qos_list):
        """
        Takes an unfiltered list of qos names as input and checks the qos names
        against the various filter methods. If it passes, and isn't in the list
        already, it adds it to the filtered list and sorts when complete.
        """
        filtered_qos_list = []
        for qos_name in unfiltered_qos_list:
            if qos_name not in filtered_qos_list \
                and self.__filter_out_ckpt(qos_name) \
                and self.__filter_by_partition(qos_name) \
                and self.__filter_by_group(qos_name):
                filtered_qos_list.append(qos_name)
        filtered_qos_list.sort()
        return filtered_qos_list

    def __generate_qos_list(self):
        unfiltered_qos_list = self.__sacctmgr_query()
        self.qos_list = self.__filter_qos_list(unfiltered_qos_list)

    def __generate_qos_resource_dict(self):
        for qos_name in self.qos_list:
            if qos_name:
                self.qos_resource_dict[qos_name] = QosResource(qos_name)

    def run_query(self):
        """
        This method performs the query described during instantiation, either a user
        or group sacctmgr query. First it generates a list of QOSes, then it
        generates a dictionary mapping QOS names to QosResource objects, e.g.:
        { "uwit" : QosResource("uwit") }
        """
        self.__generate_qos_list()
        self.__generate_qos_resource_dict()

    def run_ckpt_query(self):
        """
        This method queries ckpt resources & job limit, then turns on the ckpt printing flag
        """
        self.print_ckpt = True
        # Regex matches groups from sinfo lines like:
        #   81/39/0/120         gpu:2080ti:8        gpu:2080ti:8(IDX:0-7
        #   3072/10232/160/13464(null)              gpu:0
        # and retrieves:
        # 1. the number of nodes
        # 2. the second column (avail cpus) of the */*/*/*,
        # 3. the final column (total gpus) of gpu:2080ti:X
        # 4. the final column (used gpus) of gpu:2080ti:8(IDX:0-7
        sacctmgr_show_job_limit = [
        "/usr/bin/sacctmgr", "show", "association", "where", "account=ckpt", "format=GrpJobs", "--noheader", "--parsable2"
        ]
        job_limit = subprocess.run(sacctmgr_show_job_limit, capture_output=True,
            encoding='utf-8', check=False).stdout.strip()
        if job_limit:
            self.ckpt_job_limit = job_limit

        sinfo_pattern = re.compile(
            r"(\d*) *\d*\/(\d*)\/\d*\/\d*(?:\(\w*\)|) *(?:gpu:(\w*):|)(\d|) *(?:gpu:\w*:|)(\d|)")

        sinfo_flags = ["sinfo", "-hp", "ckpt", "-O", "Nodes,CPUsState,Gres,GresUsed"]
        sinfo_output = subprocess.run(sinfo_flags, capture_output=True,
            encoding='utf-8', check=False).stdout

        free_cpu = []
        all_gpu = defaultdict(int)
        free_gpu = defaultdict(int)

        for line in sinfo_output.split('\n'):
            has_resources = re.match(sinfo_pattern, line)
            if has_resources:
                nodes_num, avail_cpu, gpu_type, total_gpu, used_gpu = has_resources.groups()
                if total_gpu and used_gpu:
                    gpu_calc = (int(total_gpu) - int(used_gpu)) * int(nodes_num)
                    free_gpu[gpu_type] += gpu_calc
                    all_gpu[gpu_type] += int(total_gpu) * int(nodes_num)
                free_cpu.append(int(avail_cpu))

        self.ckpt_free_cpu = str(sum(free_cpu))
        self.ckpt_free_gpu = "\n".join([ f"{gpu}: {free_gpu[gpu]}/{all_gpu[gpu]}" for gpu in free_gpu ])

    def filter_by_partition(self, _query_partition: str):
        """
        This method turns on partition filtering & sets the instance's
        query_partition string.
        """
        self.partition_filter = True
        self.query_partition = _query_partition

    def print(self):
        """
        This method calls the two private prints, based on if the
        data has been generated or not.
        """
        if self.query_type:
            self.__print_resource_table()
        if self.print_ckpt:
            self.__print_ckpt_table()

    def __print_ckpt_table(self):
        table_title = "Checkpoint Resources"
        table = Table(title=table_title, box=box.ROUNDED)
        table.add_column("", justify="right")
        table.add_column("CPUs", justify="right")
        table.add_column("GPUs", justify="right")
        table.add_row("Idle:", self.ckpt_free_cpu, self.ckpt_free_gpu)
        if self.ckpt_job_limit:
            table.caption = f"Checkpoint is currently limited to {self.ckpt_job_limit} jobs"
            table.caption_style = "bgcolor default"
            table.width = len(table.caption) + 6
        if table.rows:
            console = Console()
            console.print(table)
        else:
            if self.debug:
                raise LookupError("No data for ckpt available.")
            else:
                print("Error: No data for ckpt available.")

    def __print_resource_table(self):
        """
        This method prints the contents of the qos_resource_dict in a user-friendly
        way.
        """
        if self.query_type == "user":
            table_title = "Account resources available to user: %s" % self.query_search_term
        elif self.query_type == "group":
            table_title = "Resources available to account: %s" % self.query_search_term
        elif self.query_type == "all":
            table_title = "All accounts & available resources"
        table = Table(title=table_title, box=box.ROUNDED)
        if self.partition_filter:
            table.caption = "Filtered by partition: %s" % self.query_partition
            table.caption_style = "bgcolor default"
        table.add_column("Account", justify="right")
        table.add_column("Partition", justify="right")
        table.add_column("CPUs", justify="right")
        table.add_column("Memory", justify="right")
        table.add_column("GPUs", justify="right")
        for qos_data in self.qos_resource_dict.values():
            table.add_row(qos_data.account,
                          qos_data.partition,
                          str(qos_data.resource_data["cpu"]["total"]),
                          str(int(qos_data.resource_data["mem"]["total"]/1024))+"G",
                          str(qos_data.resource_data["gpu"]["total"]),
                          "TOTAL" )
            table.add_row("", "",
                          str(qos_data.resource_data["cpu"]["used"]),
                          str(int(qos_data.resource_data["mem"]["used"]/1024))+"G",
                          str(qos_data.resource_data["gpu"]["used"]),
                          "USED" )
            table.add_row("", "",
                          str(qos_data.resource_data["cpu"]["free"]),
                          str(int(qos_data.resource_data["mem"]["free"]/1024))+"G",
                          str(qos_data.resource_data["gpu"]["free"]),
                          "FREE", end_section=True )
        if table.rows:
            console = Console()
            console.print(table)
        else:
            err = [ self.query_type, "'%s'" % self.query_search_term ]
            if self.query_partition:
                err.append("and partition")
                err.append("'%s'" % self.query_partition)
            if self.debug:
                raise LookupError("No data for %s" % ' '.join(err))
            else:
                print("Error: No data for %s" % ' '.join(err))
