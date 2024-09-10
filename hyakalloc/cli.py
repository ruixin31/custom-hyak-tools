"""
Hyakalloc.py takes an optional user/group, and partition, generates a list
of QOSes and their current resource usage, and prints it to stdout in a table.
"""

import argparse
import getpass
from hyakalloc.hyakqos import QosResourceQuery
from hyakalloc.hyakmxcheck import HyakMxCheck

def create_parser():
    """
    Generate command line options and help text.
    Input: None
    Output: ArgParse parser object
    """
    parser = argparse.ArgumentParser(description='Queries Hyak allocation for users or groups.')
    # Mutually exclusive, optional argument group for user/group query
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-a", "--all", action='store_true',
                        help="(Optional) Query all accounts & partitions.")
    group.add_argument("-c", "--ckpt", action='store_true',
                        help="(Optional) Query available resources in checkpoint.")
    group.add_argument("-u", "--user", default='', type=str,
                        help="(Optional) Query a specific user.")
    group.add_argument("-g","--group", default='', type=str,
                        help="(Optional) Query a specific group (Hyak Account).")
    # Optional partition query argument
    parser.add_argument("-p", "--partition", default='', type=str,
                        help="(Optional) Filter by partition name.")
    # Optional minimum number of GPUs per node in checkpoint
    parser.add_argument("-t","--task-gpu-count", default=0, type=int,
                        help="(Optional) Specify how many gpu per node you need in checkpoint. Default is 0.")
    # Optionally display all checkpoint resources as opposed to just resources with favorable GPUs
    parser.add_argument("-f","--full-ckpt", action='store_true',
                        help="(Optional) Display ckpt-all resources, as opposed to default of only favorable GPUs.")
    # Optionally display fairshare value associated with all checkpoint resources
    parser.add_argument("-s","--fairshare", action='store_true',
                        help="(Optional) Display fairshare value associated with all checkpoint resources.")
    # Hidden argument for choosing cluster name, defaults to 'klone'
    parser.add_argument("--cluster", default='klone', type=str,
                        help=argparse.SUPPRESS)
    parser.add_argument("--debug", action='store_true',
                        help=argparse.SUPPRESS)
    return parser

def main():
    """
    Pull in arguments from CLI, determine which queries to run, and print the results.
    """

    arguments = create_parser().parse_args()

    checkpoint_only = arguments.ckpt
    run_checkpoint_query = False

    query_all = arguments.all
    query_user = arguments.user
    query_group = arguments.group

    query_partition = arguments.partition
    
    query_task_gpu_count = arguments.task_gpu_count
    query_full_ckpt = arguments.full_ckpt
    query_fairshare = arguments.fairshare

    query_clustername = arguments.cluster
    debug = arguments.debug

    if checkpoint_only:
        query_inputs = (None, None, None)
        run_checkpoint_query = True
    elif query_all:
        query_inputs = ("all", None, query_clustername)
        run_checkpoint_query = True
    elif query_user:
        query_inputs = ("user", query_user, query_clustername)
    elif query_group:
        query_inputs = ("group", query_group, query_clustername)
    else:
        current_user = getpass.getuser()
        if current_user == "root":
            query_inputs = ("all", None, query_clustername)
        else:
            query_inputs = ("user", current_user, query_clustername)
        run_checkpoint_query = True

    my_query = QosResourceQuery(*query_inputs)

    if run_checkpoint_query:
        my_query.run_ckpt_query(query_task_gpu_count, query_full_ckpt)

    if query_fairshare:
        if not query_user:
            query_user = getpass.getuser()
        my_query.run_fairshare_query(query_user)

    if debug:
        my_query.debugging(True)

    if query_partition:
        my_query.filter_by_partition(query_partition)

    if not checkpoint_only:
        my_query.run_query()

    my_query.print()

    maintenance = HyakMxCheck()
    if maintenance.is_upcoming():
        print(maintenance.notice())


if __name__ == "__main__":
    main()
