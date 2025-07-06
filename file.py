import requests
import pandas as pd

def get_api_data():
    json_data = requests.get('https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,' \
        'daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,' \
        'wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,' \
        'soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch' \
        '&start_date=2025-05-16&end_date=2025-05-30').json()
    
    return json_data


def get_avg_hourly_data(data, data_name, data_type): # avg данные за каждые 24 часа
    converters = {
        'celsius': lambda x: (x-32)*5/9,
        'm_per_s': lambda x: x/1.944,
        'percent': lambda x: x,
        'm': lambda x: x/3.281,
    }
    transform = converters.get(data_type, lambda x: x)

    section = data['hourly'][data_name]
    avg = 0
    list_avg_data = []
    counter = 0
    for i in section:
        avg += transform(i)
        counter += 1
        if counter == 24:
            list_avg_data.append(round(avg/24, 4)) # округление до 3 знаков после запятой
            counter = 0
            avg = 0

    if avg != 0: # проверка на случай, если данные это не блоки по 24
        list_avg_data.append(round(avg/counter, 4))
    
    return list_avg_data


def get_total_hourly_data(data, data_name): # сумма данных за каждые 24 часа
    section = data['hourly'][data_name]
    summ = 0
    list_total_data = []
    counter = 0
    for i in section: 
        summ += i * 25.4 # происходит умножение чтобы перевести из дюймов в миллиметры
        counter += 1
        if counter == 24:
            list_total_data.append(round(summ, 4)) # округление до 4 знаков после запятой
            counter = 0
            summ = 0

    if summ != 0: # проверка на случай, если данные это не блоки по 24
        list_total_data.append(round(summ, 4))

    return list_total_data


def get_avg_daylight_data(data, data_name, data_type):
    converters = {
        'celsius': lambda x: (x-32)*5/9,
        'm_per_s': lambda x: x/1.944,
        'percent': lambda x: x,
        'm': lambda x: x/3.281,
        'mm': lambda x: x*25.4
    }
    transform = converters.get(data_type, lambda x: x)

    section = data['hourly'][data_name]
    summ_daylight = 0
    counter_daylight = 0
    list_avg_data = []
    is_total_need = data_name in ['rain', 'showers', 'snowfall']
    for i in range(len(section)):
        if data['daily']['sunrise'][i//24] <= data['hourly']['time'][i] <= data['daily']['sunset'][i//24]: # если время замера больше веремени рассвета и меньше заката
            summ_daylight += transform(section[i])
            counter_daylight += 1

        if (i+1) % 24 == 0:
            if counter_daylight > 0: # проверка деления на ноль
                if not is_total_need:
                    list_avg_data.append(round(summ_daylight/counter_daylight, 4))
                else:
                    list_avg_data.append(round(summ_daylight, 4))
            summ_daylight = 0
            counter_daylight = 0

    if counter_daylight>0 and not is_total_need: # проверка на случай, если данные это не блоки по 24
        list_avg_data.append(round(summ_daylight/counter_daylight, 4))
    elif counter_daylight>0:
        list_avg_data.append(round(summ_daylight, 4))
    
    return list_avg_data


def get_data_to_metric_system(data, data_name, data_type):
    converters = {
        'celsius': lambda x: (x-32)*5/9,
        'm_per_s': lambda x: x/1.944,
        'mm': lambda x: x*25.4
    }
    transform = converters.get(data_type, lambda x: x)

    section = data['hourly'][data_name]
    list_data = []
    for i in section:
        list_data.append(round(transform(i), 4))
    
    return list_data


def get_daylight_hours(data):
    sunset = data['daily']['sunset']
    sunrise = data['daily']['sunrise']
    list_daylight_hours = []
    for i, j in zip(sunset, sunrise):
        list_daylight_hours.append(round((i-j)/3600, 4))
    
    return list_daylight_hours


def get_iso_sun_set_rise_data(data):
    sunset = data['daily']['sunset']
    sunrise = data['daily']['sunrise']
    iso_sunset = [pd.to_datetime(t, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ") for t in sunset]
    iso_sunrise = [pd.to_datetime(t, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ") for t in sunrise]
    return iso_sunset, iso_sunrise


def data_to_csv(): # подготовка данных и загрузка их в csv файл
    data = get_api_data()
    load_data = []
    avg_table_sections = [('temperature_2m', 'celsius'), ('relative_humidity_2m', 'percent'),('dew_point_2m','celsius'), ('apparent_temperature', 'celsius'), ('temperature_80m', 'celsius'),
                   ('temperature_120m', 'celsius'), ('wind_speed_10m', 'm_per_s'), ('wind_speed_80m', 'm_per_s'), ('visibility', 'm')]
    
    total_table_sections = ['rain', 'showers', 'snowfall']

    avg_daylight_table_sections = avg_table_sections + [('rain', 'mm'), ('showers', 'mm'), ('snowfall', 'mm')]

    metric_system_data = [('wind_speed_10m', 'm_per_s'), ('wind_speed_80m', 'm_per_s'), ('temperature_2m', 'celsius'), ('apparent_temperature', 'celsius'), ('temperature_80m', 'celsius'),
                      ('temperature_120m', 'celsius'), ('soil_temperature_0cm', 'celsius'), ('soil_temperature_6cm', 'celsius'), ('rain', 'mm'), ('showers', 'mm'), ('snowfall', 'mm')]


    for i in avg_table_sections:
        name = 'avg_' + i[0]+ '_24h'
        data_segment = get_avg_hourly_data(data, i[0], i[1])
        load_data.append([name, data_segment])

    for i in total_table_sections:
        name = 'total_'+i+'_24h'
        data_segment = get_total_hourly_data(data, i)
        load_data.append([name, data_segment])

    for i in avg_daylight_table_sections:
        if i[0] not in ['rain', 'showers', 'snowfall']:
            name = 'avg_'+i[0]+'_daylight'
        else:
            name = 'total_'+i[0]+'_daylight'
        data_segment = get_avg_daylight_data(data, i[0], i[1])
        load_data.append([name, data_segment])

    for i in metric_system_data:
        name = i[0]+'_'+i[1]
        data_segment = get_data_to_metric_system(data, i[0], i[1])
        load_data.append([name, data_segment])

    load_data.append(['daylight_hours', get_daylight_hours(data)])

    iso_sunset, iso_sunrise = get_iso_sun_set_rise_data(data)
    load_data.append(['sunset_iso', iso_sunset])
    load_data.append(['sunrise_iso', iso_sunrise])

    with open('final_table.csv', 'w', encoding='utf-8') as file:
        for item in load_data:
                numbers_str = ",".join(map(str, item[1]))
                file.write(f"{item[0]},{numbers_str}\n")


data_to_csv()