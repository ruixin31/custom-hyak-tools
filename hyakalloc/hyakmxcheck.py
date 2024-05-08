"""
Report on upcoming maintenances
"""
import re
import subprocess
from datetime import datetime

class HyakMxCheck:
    """
    Generate a list of dictionaries, representing Slurm reservations from
    `scontrol show res -ov`. Sort the list by start time, and set two datetime
    objects: self.next_mx_start_date and self.next_mx_end_date for the start
    and end times of the next maintenance.

    Provides two public methods:

    1. is_upcoming(timeframe) which returns True/False
    depending on if the next mx is within the provided timeframe (7 days, if blank).

    2. notice(), which returns a string for printing with readable dates:
        'Notice: Klone will be down for maintenance between\n %s and %s'
    """
    def __init__(self) -> None:
        self.reservation_list = []
        self.__generate_reservation_list()
        self.__sort_reservation_list()

        next_mx = self.reservation_list[0]
        if next_mx:
            slurm_timeformat = '%Y-%m-%dT%H:%M:%S'
            self.next_mx_start_date = datetime.strptime(next_mx['StartTime'], slurm_timeformat)

    def __sort_reservation_list(self):
        slurm_date_key = lambda x: datetime.strptime(x['StartTime'], '%Y-%m-%dT%H:%M:%S')
        self.reservation_list.sort(key = slurm_date_key)

    def __parse_scontrol(self, scontrol_output):
        reservation_line_pattern = re.compile(r"ReservationName.*")
        reservation_data_pattern = re.compile(r"(\w*)=(\S*)")

        reservation_lines = re.findall(reservation_line_pattern, scontrol_output)
        for line in reservation_lines:
            # 1. Make a dictionary of the reservation data
            reservation_data = dict(re.findall(reservation_data_pattern, line))
            # 2. Append the reservation list with that dictionary.
            if 'ALL_NODES' in reservation_data['Flags']:
                self.reservation_list.append(reservation_data)

    def __scontrol_run(self):
        scontrol_flags = ["scontrol", "show", "res", "-ov"]
        scontrol_output = subprocess.run(scontrol_flags, capture_output=True,
            encoding='utf-8', check=False).stdout
        return scontrol_output

    def __generate_reservation_list(self):
        self.__parse_scontrol(self.__scontrol_run())

    def is_upcoming(self, timeframe=7):
        """
            Check if the next maintenance is within the timeframe
            Input: timeframe integer in days, default is 7 days
        """
        now = datetime.now()
        next_mx = self.next_mx_start_date
        days_until_next_mx = abs((next_mx - now).days)

        if  days_until_next_mx <= timeframe:
            return True
        else:
            return False

    def notice (self):
        """
        Returns a string describing the next maintenance.
        """
        date_print_format = '%I:%M%p %A, %b %d'
        notice_line = 'Notice: Klone will be down for maintenance starting at'
        next_mx_start = self.next_mx_start_date.strftime(date_print_format)
        mailing_line = 'Subscribe to the hyak-users mailing list for more details.'

        return '%s %s.\n %s' % (notice_line, next_mx_start, mailing_line)
