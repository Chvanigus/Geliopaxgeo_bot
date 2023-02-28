""" Работа с базой данных"""
# -*- coding: utf-8 -*-

__author__ = 'Maxim Ilmenskiy'
__date__ = 'July 2021'

import logging
import sqlite3
import sqlite3 as lite
import sys
from datetime import datetime
from datetime import time, timedelta
from os.path import join
from typing import Any

import psycopg2
import psycopg2.extras
import pythonping

import settings

logger = logging.getLogger('__name__')
db_config = settings.DB_CONFIG


class MyPsycopg2Error(psycopg2.Error):
    """ Кастомный обработчик ошибок Psycopg2"""
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return f"Ошибка: {self.error.pgcode} {self.error.pgerror}"


class DBConnector:
    """ Диспетчер контекста для подключения к базе данных.
        Параметры подключения передаются через словарь
    """

    def __init__(self, config_dict: dict) -> None:
        self.configuration = config_dict

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(**self.configuration)
            self.cursor = self.conn.cursor()
            return self.cursor
        except psycopg2.Error as e:
            raise MyPsycopg2Error(e)

    def __exit__(self, exc_type, exc_value, exc_trace) -> None:
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


def check_user(telegram_id: int) -> bool:
    """ Проверка наличия записи о данном пользователе в базе данных

    :param telegram_id:
        Идентификатор пользователя в telegram
    :return:
        Если пользователь уже существует в системе - возвращается True
    """
    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT * FROM public."TelegramBot" WHERE telegram_id in (%s)'
            cur.execute(sql, (telegram_id,))
            user_data = cur.fetchall()
            if user_data:
                return True
            else:
                return False
    except psycopg2.Error as e:
        logger.critical(f'Невозможно проверить пользователя. Ошибка: {e}')


def registration_users(user_data: dict) -> None:
    """ Регистрация пользователя в системе. Все данные заносятся в таблицу TelegramBot

    :param user_data:
        Словарь фиксированной длины с данными о пользователе
    """
    try:
        with DBConnector(db_config) as cur:
            sql = 'INSERT INTO public."TelegramBot"(name, surname, regisdate, ' \
                  'telegram_id, regcheck, role) ' \
                  'VALUES (%s, %s, %s, %s, %s, %s);'

            cur.execute(sql, (user_data['name'],
                              user_data['surname'],
                              user_data['regisdate'],
                              user_data['telegram_id'],
                              False,
                              user_data['role']))
    except psycopg2.Error as e:
        logger.critical(f'Невозможно добавить пользователя. Ошибка: {e}')


def confirm_reg(telegram_id: int, check: str) -> None:
    """ Подтверждение или удаление учётной записи пользователя в базе данных
    :param telegram_id:
        Идентификатор пользователя в telegram
    :param check:
        Параметр, по которому делается запрос в базу данных. В строковом формате, если 'true', то подтверждает
        регистрацию пользователя, если 'delete' - удаляет запись о пользователе из базы данных
    """
    if check == 'delete':
        sql = 'DELETE FROM public."TelegramBot" WHERE "telegram_id" = %s'
    elif check == 'true':
        sql = 'UPDATE public."TelegramBot" SET regcheck = True WHERE telegram_id = %s'
    else:
        sql = None
    try:
        if sql:
            with DBConnector(db_config) as cur:
                cur.execute(sql, (telegram_id, ))
    except psycopg2.Error as e:
        logger.critical(f'Невозможно проверить/удалить пользователя. Ошибка: {e}')


def check_reg_status(telegram_id: int) -> bool:
    """ Проверка статуса подтверждения регистрации пользователя

    :param telegram_id:
        Идентификатор пользователя в telegram
    :return:
        Если у пользователя есть подтверждение регистрации - возвращается True
    """
    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT regcheck FROM public."TelegramBot" WHERE telegram_id in (%s)'
            cur.execute(sql, (telegram_id,))
            status = cur.fetchall()
            return status[0][0]
    except IndexError as e:
        logger.critical(
            f'Невозможно проверить статус регистрации пользователя. Ошибка: {e}')


def get_role(telegram_id: int) -> int:
    """ Получение информации о роли и месте работы пользователя

    :param telegram_id:
        Идентификатор пользователя в telegram
    :return:
        Список, содержащий в себе данные номер роли и места работы
    """

    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT role FROM public."TelegramBot" WHERE telegram_id in (%s)'
            cur.execute(sql, (telegram_id,))
            data = cur.fetchall()
            return data[0][0]
    except Exception as e:
        logger.critical(f'Невозможно получить данные по роли и месту работы. Ошибка: {e}')


def get_weather_station_id_from_agro(agro_id: int) -> list:
    """ Получение id метеостанций

    :param agro_id:
        Номер Агро
    :return:
        id всех метеостанций, которые находятся в данном Агро
    """
    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT weathergroupid FROM public."WeatherGroupAgro" WHERE agroid in (%s)'
            cur.execute(sql, (agro_id,))
            weather_station_id = cur.fetchall()
            return weather_station_id
    except psycopg2.Error as e:
        logger.critical(
            f'Невозможно получить id метеостанций в Агро {agro_id}. Ошибка: {e}')


def get_weather_data_from_agro(agro_id: int) -> list:
    """ Функция получения данных о текущей погоде в хозяйстве по номеру Агро

    :param agro_id:
        Предприятие, по которому производится запрос
    :return:
        Список кортежей данных по погоде в каждом агро
    """

    try:
        with DBConnector(db_config) as cur:
            weather_station_id = get_weather_station_id_from_agro(agro_id=agro_id)
            output_data = []
            for station_id in weather_station_id:
                sql = 'SELECT datetime, temperature, humidity, barometer, rain, windspeed, windgust, winddegrees, ' \
                      'winddirection, weatherstationid, consbatteryvoltage FROM public."WeatherData" ' \
                      'WHERE weatherstationid in (%s) ORDER BY id DESC LIMIT 1;'
                cur.execute(sql, (station_id[0],))
                weather_data = cur.fetchall()
                output_data.append(weather_data[0])
            return output_data
    except psycopg2.Error as e:
        logger.critical(f'Невозможно получить данные по текущей погоде для Агро {agro_id}. Ошибка: {e}')


def get_weather_station_name(weather_station_id: int) -> str:
    """ Получение названия метеостанции исходя из id метео

    :param weather_station_id:
        id метеостанции
    :return:
        Название станции
    """

    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT shortname FROM public."WeatherGroup" WHERE id in (%s)'
            cur.execute(sql, (weather_station_id,))
            weather_station_name = cur.fetchall()[0][0]
            return weather_station_name
    except Exception as e:
        logger.critical(
            f'Невозможно получить информацию по названию. Ошибка: {e}')


def get_max_id_from_layer(agro_id: int) -> int:
    """ Получение максимального id из базы данных для проверки наличия нового снимка

    :return:
        Возвращает максимальный id снимка из базы данных для конкретного Агро
    """
    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT MAX(id) FROM public."Layer" WHERE agroid in (%s) and set in (%s)'
            cur.execute(sql, (agro_id, 'visual'))
            current_id = cur.fetchall()[0][0]
            return current_id
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно извлечь данные о пользователях. Ошибка: {e}')


def get_amount_of_precipitation_for_the_last_day(weather_station_id: int) -> float:
    """ Получение суммы осадков за прошедшие сутки с каждой метеостанции

    :param weather_station_id:
        Номер метеостанции
    :return:
        Сумма осадков за последние сутки
    """
    try:
        with DBConnector(db_config) as cur:
            time_ = time(8, 00)
            date_end = datetime.combine(datetime.now().date(), time_)
            date_start = datetime.combine(
                datetime.now().date() - timedelta(days=1), time_)
            sql = 'SELECT SUM(rain) FROM public."WeatherData" where datetime ' \
                  'BETWEEN (%s) AND (%s) AND weatherstationid in (%s)'
            cur.execute(sql, (date_start, date_end, weather_station_id))
            sum_rain = cur.fetchall()[0][0]
            return sum_rain
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно извлечь данные о пользователях. Ошибка: {e}')


def get_zone_id_from_agro(agro_id: int) -> list:
    """ Получение id микрозон для конкретного Агро

    :param agro_id:
        Номер агро
    :return:
        Список номеров микрозон по хозяйству
    """
    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT id, forecastareaname FROM public."ForecastZoneArea" where agroid like (%s)'
            param = f'%{agro_id}%'
            cur.execute(sql, (param,))
            zones_id = cur.fetchall()
            return zones_id
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить данные о микрозонах. Ошибка: {e}')


def get_forecast_data(zone_id: int) -> list:
    """ Получение данных по прогнозу для выбранной микрозоны и даты

    :param zone_id:
        Номер микрозоны
    :return:
        Данные о прогнозе погоды по микрозоне
    """

    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT "time", summary, precipintensity, precipintensitymax, dewpoint, humidity, pressure, ' \
                  'temperaturemin, temperaturemax, temperaturemintime, temperaturemaxtime ' \
                  f'FROM public."ForecastDaily" where forecastzoneid in (%s) '
            cur.execute(sql, (zone_id,))
            forecast_data = cur.fetchall()
            print(forecast_data)
            return forecast_data
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить данные о погоде в микрозонах. Ошибка: {e}')


def get_forecast_name(zone_id: int) -> str:
    """ Получение названия микрозоны по id

    :param zone_id:
        Номер микрозоны
    :return:
        Строка названия микрозоны
    """

    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT forecastareaname FROM public."ForecastZoneArea" where id in (%s)'
            cur.execute(sql, (zone_id,))
            name = cur.fetchall()[0][0]
            return name
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить название микрозоны. Ошибка: {e}')


def get_forecast_dates(zone_id: int) -> list:
    """ Получение даты микрозоны по id

    :param zone_id:
        Номер микрозоны
    :return:
        Список кортежей с датами прогноза
    """

    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT "time" FROM public."ForecastDaily" where forecastzoneid in (%s)'
            cur.execute(sql, (zone_id,))
            dates = cur.fetchall()
            return dates
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить название микрозоны. Ошибка: {e}')


def get_forecast_data_with_date(zone_id: int, forecast_date: datetime.date = None) -> tuple[Any, ...]:
    """ Получение данных по прогнозу для выбранной микрозоны и даты
        Эта функция вынесена отдельно, чтобы точно проверять время прогноза

    :param zone_id:
        Номер микрозоны
    :param forecast_date:
        Дата прогноза погоды
    :return:
        Данные о прогнозе погоды по микрозоне по заданной дате
    """

    try:
        with DBConnector(db_config) as cur:
            sql = f'SELECT "time" FROM public."ForecastDaily" where forecastzoneid in (%s) '
            cur.execute(sql, (zone_id,))
            forecast_date_now = cur.fetchall()
            for dates in forecast_date_now:
                if str(forecast_date) == str(dates[0].date()):
                    sql = 'SELECT "time", summary, precipintensity, ' \
                          'precipintensitymax, dewpoint, humidity, pressure, ' \
                          'temperaturemin, temperaturemax, temperaturemintime, temperaturemaxtime ' \
                          'FROM public."ForecastDaily" where forecastzoneid in (%s) and "time" in (%s)'
                    cur.execute(sql, (zone_id, forecast_date_now[0]))
                    forecast_data = cur.fetchall()[0]
                    return forecast_data

    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить данные о погоде в микрозонах. Ошибка: {e}')


def check_cameras() -> list:
    """ Проверка статуса работы камер
    :return:
        Список камер, которые не пингуются
    """

    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT * FROM public."SecurityCam"'
            cur.execute(sql)
            cameras_data = cur.fetchall()
            output_data = []
            for camera in cameras_data:
                if not pythonping.ping(camera[2], count=1).success():
                    output_data.append(camera)
            return output_data
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить данные о статусе видеокамер. Ошибка: {e}')


def get_weather_archive(station_id: int, date: datetime.date) -> list:
    """ Получение архива данных о погоде (температуре и осадках) за указанный период

    :param station_id:
        id метеостанции
    :param date:
        Дата, по которой извлекаются данные
    """

    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT MAX(temperature), MIN(temperature), AVG(temperature), SUM(rain) ' \
                  'FROM public."WeatherData" WHERE weatherstationid in (%s) AND datetime BETWEEN (%s) and (%s)'
            cur.execute(sql, (station_id, date, datetime.combine(date, time(hour=23, minute=30))))
            weather_data = cur.fetchall()
            if weather_data:
                return weather_data
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить архивные данные о погоде. Ошибка запроса БД: {e}')


def check_weatherstations() -> list:
    """ Проверка статуса работы метеостанций
    :return:
        Список метеостанций, которые не пингуются
    """
    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT * FROM public."WeatherStation"'
            cur.execute(sql)
            weatherstations_data = cur.fetchall()
            output_data = []
            for weatherstation in weatherstations_data:
                if not pythonping.ping(weatherstation[7], count=2).success():
                    output_data.append(weatherstation)
            return output_data
    except psycopg2.Error as e:
        logging.critical(
            f'Невозможно получить данные о статусе метеостанций. Ошибка: {e}')


def get_list_weather_stations_id() -> list:
    """ Получение списка id работающих метеостанций из базы данных
    :return:
        Список с id метеостанций
    """
    try:
        with DBConnector(db_config) as cur:
            try:
                sql = 'SELECT id FROM public."WeatherStation"'
                cur.execute(sql)
                list_weather_stations_id = cur.fetchall()
                list_stations_id = []
                for tuple_station_id in list_weather_stations_id:
                    list_stations_id.append(*tuple_station_id)
                list_stations_id.sort()
                return list_stations_id
            except psycopg2.Error as e:
                logger.critical(
                    f'Невозможно получить список метеостанций из базы данных. Пропуск. Ошибка: {e}')
                sys.exit(-1)
    except psycopg2.Error as e:
        logging.critical(f'Невозможно получить данные о статусе. Ошибка: {e}')


def get_weather_data(weather_station_id: int = None) -> list or None:
    """ Извлечение данных из локальной базы метеостанции и обработка этих данных
        :param weather_station_id:
            Номер метеостанции
        :return:
         Список списков данных о погоде с метеостанций
    """
    weather_data = []
    con = lite.connect(
        join(
            '/var/lib/weewx',
            f'meteo_{weather_station_id}.sdb'))
    with con:
        cur = con.cursor()
        try:
            cur.execute(
                "SELECT * FROM archive ORDER BY dateTime DESC LIMIT 1;")
            weather_data = cur.fetchall()
        except sqlite3.OperationalError:
            pass
    none_list = list()
    if weather_data:
        for data in weather_data:
            line_weather = list()

            if data[68] is None:
                line_weather.append("Температура")
            if data[19] is None:
                line_weather.append('Точка росы')
            if data[67] is None:
                line_weather.append('Влажность')
            if data[76] is None:
                line_weather.append('Осадки')
            if data[6] is None:
                line_weather.append('Давление')
            if data[113] is None:
                line_weather.append('Скорость ветра')
            if data[110] is None:
                line_weather.append('Порывы ветра')

            none_list.append(line_weather)
        return none_list
    else:
        return None


def get_list_users() -> list:
    """ Получает список всех зарегистрированных пользователей"""
    try:
        with DBConnector(db_config) as cur:
            sql = 'SELECT * FROM public."TelegramBot"'
            cur.execute(sql, )
            list_users_without_reg = cur.fetchall()
            return list_users_without_reg
    except psycopg2.Error as e:
        logger.critical(f'Невозможно получить список пользователей без регистрации. Ошибка: {e}')
