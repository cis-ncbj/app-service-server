#!/usr/bin/env python

import os
from datetime import datetime, timedelta

forecast_dir = '/mnt/home/appmeteo/NWP/FORECASTS'
start_date = '@@{start_case}'
end_date = '@@{end_case}'
out_file_name = 'AVAILABLE'


def get_in_data_file(forecast):
    return "wrfout_d02_" + forecast.strftime('%Y-%m-%d_%H')


def link_in_data_file(forecast):
    if not os.path.isfile(get_in_data_file(forecast) + ".nc"):
        os.symlink(os.path.join(forecast_dir,
                                (forecast + timedelta(hours=12)).strftime('CIS.%Y%m%d%H'),
                                'wrfprd',
                                "wrfout_d02_" + forecast.strftime('%Y-%m-%d_%H:%M:%S')),
                   get_in_data_file(forecast) + ".nc")


def main():
    av_forecasts = []
    s_date = datetime.strptime(start_date, '%Y%m%d %H0000')
    e_date = datetime.strptime(end_date, '%Y%m%d %H0000')

    for dir_ in os.listdir(forecast_dir):
        av_forecasts.append(datetime.strptime(dir_, 'CIS.%Y%m%d%H'))
    av_forecasts = sorted(av_forecasts)
    forecast_index = None

    for i, date in enumerate(av_forecasts):
        if date <= s_date <= (av_forecasts[i + 1]):
            forecast_index = i + 1
            break

    step = timedelta(hours=1)
    tmpdate = s_date

    in_data = get_in_data_file(av_forecasts[forecast_index])
    link_in_data_file(av_forecasts[forecast_index])

    out_file = open(out_file_name, 'w')
    header = '''XXXXXX EMPTY LINES XXXXXXXXX
XXXXXX EMPTY LINES XXXXXXXX
YYYYMMDD HHMMSS   name of the file(up to 80 characters)
'''
    out_file.write(header)

    while tmpdate < e_date:
        if tmpdate >= (av_forecasts[forecast_index + 1]):
            forecast_index += 1
            in_data = get_in_data_file(av_forecasts[forecast_index])
            link_in_data_file(av_forecasts[forecast_index])

        out_file.write(tmpdate.strftime('%Y%m%d %H0000') + "      '" + in_data + ".nc'      ' '\n")
        tmpdate += step
    out_file.write(e_date.strftime('%Y%m%d %H0000') + "      '" + in_data + ".nc'      ' '\n")


if __name__ == '__main__':
    main()
