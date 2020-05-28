import argparse
import os
import csv
import sqlite3
from datetime import datetime
import time

import pytz
from icalendar import Calendar, Event


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('dbpath')
    parser.add_argument('outdir')
    parser.add_argument('--limit', type=int, default=500)
    return parser.parse_args()


def main():
    args = parse_args()

    conn = sqlite3.connect(args.dbpath)

    us_west_start = datetime(2014, 6, 28)
    us_west_end = datetime(2014, 9, 24)
    us_east_start = datetime(2018, 8, 1)

    tz_tw = pytz.timezone('Asia/Taipei')
    tz_us_west = pytz.timezone('US/Pacific')
    tz_us_east = pytz.timezone('US/Eastern')

    cmd = '''SELECT activities.name, categories.name, start_time, end_time
    FROM activities, categories, facts
    WHERE facts.activity_id = activities.id
    AND activities.category_id = categories.id
    ORDER BY facts.start_time'''

    cals = []
    cal = Calendar()
    curr_cnt = 0
    cnt_us_west = cnt_us_east = cnt_tw = 0
    for row in conn.execute(cmd):
        start_time = datetime.strptime(row[-2], '%Y-%m-%d %H:%M:%S')
        if us_west_end >= start_time >= us_west_start:
            tz = tz_us_west
            cnt_us_west += 1
        elif start_time >= us_east_start:
            tz = tz_us_east
            cnt_us_east += 1
        else:
            tz = tz_tw
            cnt_tw += 1

        end_time = datetime(*time.strptime(row[-1], '%Y-%m-%d %H:%M:%S')[:6],
                            tzinfo=tz)
        start_time = datetime(*time.strptime(row[-2], '%Y-%m-%d %H:%M:%S')[:6],
                              tzinfo=tz)
        name = row[0]
        category = row[1]
        event = Event()
        event.add('summary', '%s (%s)' % (name, category))
        event.add('dtstart', start_time)
        event.add('dtend', end_time)
        cal.add_component(event)

        # limit calendar size
        curr_cnt += 1
        if curr_cnt >= args.limit:
            cals.append(cal)
            cal = Calendar()
            curr_cnt = 0
    os.makedirs(args.outdir, exist_ok=True)
    for idx, cal in enumerate(cals):
        with open(os.path.join(args.outdir, 'cal-%04d.ics' % idx),
                  'w') as outfile:
            outfile.write(cal.to_ical().decode('utf8').replace('\r\n',
                                                               '\n').strip())

    conn.close()


if __name__ == '__main__':
    main()
