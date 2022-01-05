import datetime


def datetime_to_iso(dt):
    return str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)

def iso_to_datetime(iso):
    return datetime.datetime(int(iso.split("-")[0]), int(iso.split("-")[1]), int(iso.split("-")[2]))

def iso_to_date(iso):
    return datetime.date(int(iso.split("-")[0]), int(iso.split("-")[1]), int(iso.split("-")[2]))


