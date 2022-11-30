import datetime


class ConvertDatetime:
    @staticmethod
    def convert_unix_to_date(unixtime: int):
        x = datetime.datetime.fromtimestamp(unixtime)
        date = x.strftime('%Y-%m-%d')
        return date

    @staticmethod
    def convert_unix_to_time(unixtime: int):
        x = datetime.datetime.fromtimestamp(unixtime)
        time = x.strftime('%H:%M:%S')
        return time
