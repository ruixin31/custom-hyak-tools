"""
hyakstorage_query.py parses '.hyakstorage.csv' files from home directories and
shared directories (contrib/gscratch). Options are:
-
"""
from collections import defaultdict
from typing import NamedTuple
from dataclasses import dataclass
import argparse
import getpass
import pathlib
import csv
import grp
import pwd
import rich, rich.box, rich.table, rich.console

CSV_FILENAME=".hyakstorage.csv"

def parse_arguments() -> argparse.Namespace:
    """
    Input: None
    Returns: argparse.Namespace object ("parsed" arguments):
        search_for: string (can be path or group name)

        print_my_homedir: bool (default False)
        print_my_gscratch_dirs: bool (default False)
        print_my_contrib_dirs: bool (default False)

        show_usage_by_group: bool (default False)
        show_usage_by_user: bool (default False)

        sort_by_disk: bool (default False but True if no other sort selected)
        sort_by_files: bool (default False)
    """
    parser = argparse.ArgumentParser(prog="hyakstorage")
    selection_arguments = parser.add_argument_group('selection options')
    selection_arguments.add_argument(
        "-m", "--home", action='store_true', dest='print_my_homedir',
        help="print storage report for my home directory"
        )
    selection_arguments.add_argument(
        "-g", "--gscratch", action='store_true', dest='print_my_gscratch_dirs',
        help="print storage report for my gscratch directories"
        )
    selection_arguments.add_argument(
        "-c", "--contrib", action='store_true', dest='print_my_contrib_dirs',
        help="print storage report for my contrib directories"
        )
    viewing_arguments = parser.add_argument_group('view options')
    viewing_arguments.add_argument(
        "-p", "--show-group", action='store_true', dest='show_usage_by_group',
        help="show usage by groups"
        )
    viewing_arguments.add_argument(
        "-u", "--show-user", action='store_true', dest='show_usage_by_user',
        help="show usage by users"
        )
    sorting_options = parser.add_mutually_exclusive_group()
    sorting_options.add_argument(
        "-f", "--by-files", action='store_true', dest='sort_by_files',
        help="sort by file usage"
        )
    sorting_options.add_argument(
        "-d", "--by-disk", action='store_true', dest='sort_by_disk',
        help="sort by disk usage"
        )
    search_arguments = parser.add_argument_group('search option')
    search_arguments.add_argument(
        "search_for", nargs='?', type=str, default='', metavar='path or groupname',
        help="show usage for this path or group"
        )

    # Additional argument validation:
    _args = parser.parse_args()

    # If the user didn't sort by files, we should sort by disk
    if not _args.sort_by_files:
        parser.set_defaults(sort_by_disk=True)

    # Make mutual exclusion between searching & selecting folders
    user_selected_folders = any((
        _args.print_my_homedir,
        _args.print_my_gscratch_dirs,
        _args.print_my_contrib_dirs
    ))
    user_provided_search_term = True if _args.search_for else False
    if user_selected_folders and user_provided_search_term:
        parser.error("folder selection and searching are mutually exclusive")

    # If user didn't pick any folders, show homedir and gscratch dirs
    if not user_selected_folders:
        parser.set_defaults(print_my_homedir=True, print_my_gscratch_dirs=True)

    return parser.parse_args()
class UsageCSVDataFields(NamedTuple):
    disk_used: int
    disk_quota: int
    files_used: int
    files_quota: int

@dataclass
class UsageCSVLine():
    value: str
    type: str
    _disk_used: str
    _disk_quota: str
    _files_used: str
    _files_quota: str

    def __post_init__(self) -> None:
        self.data = UsageCSVDataFields(
            int(self._disk_used),
            int(self._disk_quota),
            int(self._files_used),
            int(self._files_quota)
        )

class UsageReportRow(NamedTuple):
    title: str
    disk_usage: str
    file_usage: str

class UsageReportTable(NamedTuple):
    header: str
    rows: list[UsageReportRow]


def parse_usage_csv(path_to_report: pathlib.Path) -> dict:
    parsed_csv = defaultdict(dict)
    parsed_csv["path"] = path_to_report
    with path_to_report.open(mode="r", encoding="utf-8", newline="") as csvfile:
        for line in csv.reader(csvfile):
            parsed_line = UsageCSVLine(*line)
            if parsed_line.type == "fileset":
                parsed_csv[parsed_line.type] = parsed_line.data
            else:
                parsed_csv[parsed_line.type][parsed_line.value] = parsed_line.data
    #pprint(parsed_csv)
    return parsed_csv

def format_percentage(numerator:int , denominator: int) -> str:
    return f"{numerator/denominator:.0%}"

def format_disk_totals_strings(used: int, quota: int) -> tuple[str]:
    divided = f"{used}GB / {quota}GB"
    percent = format_percentage(used, quota)
    return (divided, percent)

def format_files_totals_strings(used: int, quota: int) -> tuple[str]:
    divided = f"{used} / {quota} files"
    percent = format_percentage(used, quota)
    return (divided, percent)

def get_usage_data_for_specific_user(parsed_csv: dict, user: str) -> UsageCSVDataFields:
    if user in parsed_csv["user"]:
        return parsed_csv["user"][user]
    else:
        return None

def get_totals_strings(data_fields: UsageCSVDataFields) -> tuple[str]:
    disk_divided, disk_percent = format_disk_totals_strings(data_fields.disk_used, data_fields.disk_quota)
    files_divided, files_percent = format_files_totals_strings(data_fields.files_used, data_fields.files_quota)
    return (disk_divided, disk_percent, files_divided, files_percent)

def make_totals_rows(row_title: str, csv_data_fields: UsageCSVDataFields) -> UsageReportRow:
    disk_divided, disk_percent, files_divided, files_percent = get_totals_strings(csv_data_fields)
    top_row = UsageReportRow(row_title, disk_divided, files_divided)
    bottom_row = UsageReportRow("", disk_percent, files_percent)
    return [top_row, bottom_row]

def print_homedir_report(homedir_csv_path=None) -> None:
    if homedir_csv_path is None:
        my_homedir = pathlib.Path.home()
        homedir_csv_path = my_homedir / CSV_FILENAME
    if not homedir_csv_path.exists():
        return
    parsed_homedir_csv = parse_usage_csv(homedir_csv_path)
    username = homedir_csv_path.parent.name
    usage_data_for_user = get_usage_data_for_specific_user(parsed_homedir_csv, username)
    if not usage_data_for_user:
        return
    homedir_totals_rows = make_totals_rows("Total:", usage_data_for_user)
    homedir_path = homedir_csv_path.parent
    home_report = UsageReportTable(
        header = homedir_path,
        rows = homedir_totals_rows
        )
    print_usage_table(home_report)

def find_gscratch_csvs() -> list[pathlib.Path]:
    gscratch_path = pathlib.Path("/mmfs1/gscratch")
    gscratch_csvs = []
    gscratch_csvs += gscratch_path.glob('*/' + CSV_FILENAME)

    gscratch_directory_ignorelist = ["scrubbed", "flash", "lolo-test"]

    for gscratch_subdirectory in gscratch_directory_ignorelist:
        csv_to_ignore = gscratch_path / gscratch_subdirectory / CSV_FILENAME
        if csv_to_ignore in gscratch_csvs:
            gscratch_csvs.remove(csv_to_ignore)

    return gscratch_csvs

def make_row_with_title_only(title: str) -> UsageReportRow:
    return UsageReportRow(title=title, disk_usage="", file_usage="")

def make_filtered_rows(parsed_usage_csv: dict, args: argparse.Namespace, filter_type: str) -> list[UsageReportRow]:
    filtered_rows = []
    fileset_disk_usage = parsed_usage_csv["fileset"].disk_used
    fileset_files_usage = parsed_usage_csv["fileset"].files_used
    for csv_value_field, csv_data_fields in parsed_usage_csv[filter_type].items():
        row_title = csv_value_field
        row_disk_usage = csv_data_fields.disk_used
        row_disk_percentage = format_percentage(row_disk_usage, fileset_disk_usage)
        row_files_usage = csv_data_fields.files_used
        row_files_percentage = format_percentage(row_files_usage, fileset_files_usage)
        disk_usage_string = f"{row_disk_usage}GB ({row_disk_percentage})"
        files_usage_string = f"{row_files_usage} files ({row_files_percentage})"
        filtered_rows.append(UsageReportRow(row_title, disk_usage_string, files_usage_string))
        if args.sort_by_disk:
            filtered_rows.sort(reverse=True, key=lambda row: parsed_usage_csv[filter_type][row.title].disk_used)
        elif args.sort_by_files:
            filtered_rows.sort(reverse=True, key=lambda row: parsed_usage_csv[filter_type][row.title].files_used)
    return filtered_rows

def make_fileset_table(parsed_csv, path_to_csv_dir) -> UsageReportTable:
    table_rows = []
    fileset_totals_rows = make_totals_rows("Total:", parsed_csv["fileset"])
    table_rows.extend(fileset_totals_rows)
    my_username = getpass.getuser()
    my_usage = get_usage_data_for_specific_user(parsed_csv, my_username)
    if my_usage:
        my_totals_row = UsageReportRow("My usage:", f"{my_usage.disk_used}GB", f"{my_usage.files_used} files",)
        table_rows.append(my_totals_row)
    return UsageReportTable(path_to_csv_dir, table_rows)


def make_report_tables_from_csv(csv_path: pathlib.Path, user_args: argparse.Namespace) -> list[UsageReportTable]:
    parsed_usage_csv = parse_usage_csv(csv_path)
    report_tables = []
    fileset_table = make_fileset_table(parsed_usage_csv, csv_path.parent)
    report_tables.append(fileset_table)

    if user_args.show_usage_by_group:
        group_rows = make_filtered_rows(parsed_usage_csv, user_args, filter_type="group")
        if group_rows:
            group_subtable = UsageReportTable("Usage by group:", group_rows)
            report_tables.append(group_subtable)
    if user_args.show_usage_by_user:
        user_rows = make_filtered_rows(parsed_usage_csv, user_args, filter_type="user")
        if user_rows:
            user_subtable = UsageReportTable("Usage by user:", user_rows)
            report_tables.append(user_subtable)

    return report_tables

def print_usage_table(table_to_print: UsageReportTable) -> None:
    if isinstance(table_to_print.header, pathlib.Path):
        table_title = f"Usage report for {table_to_print.header}"
        rich_table = rich.table.Table(title=table_title, box=rich.box.ROUNDED)
    else:
        table_title = table_to_print.header
        rich_table = rich.table.Table(title=table_title, box=rich.box.ROUNDED,
            title_style="bg_color=None", show_header=False)

    rich_table.add_column("", width=20)
    rich_table.add_column("Disk Usage", width=30)
    rich_table.add_column("Files Usage", width=30)
    for row in table_to_print.rows:
        rich_table.add_row(*row)
    if rich_table.rows:
        console = rich.console.Console()
        console.print(rich_table)

def print_my_gscratch_dirs_reports(user_arguments: argparse.Namespace) -> None:
    accessible_gscratch_csvs = find_gscratch_csvs()
    usage_report_tables = []
    for path_to_csv in accessible_gscratch_csvs:
        usage_report_tables.extend(make_report_tables_from_csv(path_to_csv, user_arguments))
    for table in usage_report_tables:
        print_usage_table(table)

def get_my_groups() -> list[str]:
    my_username = getpass.getuser()
    return [g.gr_name for g in grp.getgrall() if my_username in g.gr_mem]

def find_my_contrib_csvs() -> list[pathlib.Path]:
    contrib_path = pathlib.Path("/mmfs1/sw/contrib")
    contrib_csvs = []
    my_groups = get_my_groups()
    for group_name in my_groups:
        src_directory = group_name + "-src"
        possible_contrib_csv = contrib_path / src_directory / CSV_FILENAME
        if possible_contrib_csv.exists():
            contrib_csvs.append(possible_contrib_csv)
    return contrib_csvs

def print_my_contrib_dirs_reports(user_arguments: argparse.Namespace) -> None:
    my_group_contrib_csvs = find_my_contrib_csvs()
    usage_report_tables = []
    for contrib_csv in my_group_contrib_csvs:
        usage_report_tables.extend(make_report_tables_from_csv(contrib_csv, user_arguments))
    for table in usage_report_tables:
        print_usage_table(table)

def check_if_linux_group(possible_group: str) -> bool:
    return possible_group in [g.gr_name for g in grp.getgrall()]

def check_if_linux_username(possible_user: str) -> bool:
    try:
        pwd.getpwnam(possible_user)
        return True
    except KeyError:
        return False


def parse_search_term_and_print_report(user_arguments: argparse.Namespace) -> None:
    search_term = user_arguments.search_for

    # figure out if it's a contrib dir, linux group/user, or potentially a path
    gpfs_home_path = pathlib.Path("/mmfs1/home")
    contrib_path = pathlib.Path("/mmfs1/sw/contrib")
    gscratch_path = pathlib.Path("/mmfs1/gscratch")
    if "-src" in search_term:
        possible_dir = contrib_path / search_term
    elif check_if_linux_group(search_term):
        possible_dir = gscratch_path / search_term
    elif check_if_linux_username(search_term):
        possible_dir = gpfs_home_path / search_term
    else:
        possible_dir = pathlib.Path(search_term)

    # now that we have a possible directory to check, add csv filename if needed
    if possible_dir.name == CSV_FILENAME:
        possible_csv = possible_dir
    else:
        possible_csv = possible_dir / CSV_FILENAME

    # if it's a homedir, print homedir report
    # otherwise, try to read a usage csv and print it
    try:
        if possible_csv.parent.parent == gpfs_home_path:
            print_homedir_report(possible_csv)
        else:
            if possible_csv.exists():
                found_csv_tables = make_report_tables_from_csv(possible_csv, user_arguments)
                for table in found_csv_tables:
                    print_usage_table(table)
            else:
                print(f"error: couldn't find a storage report for '{possible_csv.parent}'")
    except PermissionError:
        print(f"error: can't open directory '{possible_csv.parent}'")

def main():
    arguments = parse_arguments()

    if arguments.search_for:
        parse_search_term_and_print_report(arguments)
    else:
        if arguments.print_my_homedir:
            print_homedir_report()
        if arguments.print_my_gscratch_dirs:
            print_my_gscratch_dirs_reports(arguments)
        if arguments.print_my_contrib_dirs:
            print_my_contrib_dirs_reports(arguments)

if __name__ == "__main__":
    main()
