def timedelta_total_seconds(td):
    """ Needed for python 2.6 compat """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10. ** 6) / 10. ** 6
