def timedelta_total_ms(time):
    return (time.microseconds + (time.seconds + time.days * 24 * 3600) *\
            10**6) / 10**3
