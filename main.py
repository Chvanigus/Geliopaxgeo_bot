""" TelegramBot Geliopaxgeo"""

__author__ = 'Maxim Ilmenskiy, Evgeniy Klucherov'
__date__ = 'July 2021'

import logging
import platform
import threading
from datetime import datetime, time, timedelta
from time import sleep

import requests
import telebot
from decouple import config

import dboperator as db
import settings
from utils import RepeatedTimer, check_permission, check_registration, create_button, delete_message, \
    get_agro_from_user, get_agro_from_user_classmethod, mult_threading, parse_query, send_bot_location, send_bot_message


# Управляющий токен для бота
bot = telebot.TeleBot(config('TOKEN', default=''))
logger = logging.getLogger('__name__')

""" Существует система распределения информации в зависимости от роли

    role:
    1 - view - может смотреть информацию по своему хозяйству. Нет доступа к камерам и прочим важным вещам.
    Обычно это сотрудники Агро
    2 - programmer - может смотреть всё
    3 - view all - может смотреть информацию по всем хозяйствам. Нет доступа к камерам и прочим важным вещам.
    Обычно это сотрудники офиса
    4 - security - может смотреть информацию по камерам
    9999 - отдельный вид пользователей, которые не используют большую часть системы. В основном это для рассылок

"""


def timer(start: str, func_name: str, *args):
    """ Таймер, по которому происходит запуск функции
    :param start:
        Время, в которое мы хотим запустить функцию (в строковом формате)
    :param func_name:
        Имя функции (в строковом формате)
    :param args:
        Аргументы для функции
    """
    # Получаем текущее время для сравнения
    now = datetime.today().strftime("%H:%M:%S")
    if str(now) == start:
        if args:
            # Функция, которая должна быть запущена в определенное время (с аргументами)
            globals()[func_name](args)
        else:
            # Функция, которая должна быть запущена в определенное время (без аргументов)
            globals()[func_name]()


@get_agro_from_user
def answer_about_weather(query: telebot.types.CallbackQuery) -> None:
    """ Ответ на запрос о погоде в выбранном Агро"""

    def parse_weather_data(weather_data: list) -> str:
        """ Парсит данные о погоде в строковый формат
        :param weather_data:
            Список данных с погодой из базы данных
        :return:
            Строковый формат представления данных по всем заданным метеостанциям
        """
        text_ = ''
        if weather_data:
            text_ = '*Текущая погода*\n'
            for i in range(0, len(weather_data)):
                weather_date = datetime.strftime(weather_data[i][0], '%d-%m-%Y %H:%M')
                weather_station_name = db.get_weather_station_name(weather_station_id=weather_data[i][9])
                if weather_data[i][5]:
                    wind_speed = str(weather_data[i][5]) + ' м/с'
                else:
                    wind_speed = 'Ветра нет'
                if weather_data[i][6]:
                    gusts_speed = str(weather_data[i][6]) + ' м/с'
                else:
                    gusts_speed = 'Порывов нет'
                if weather_data[i][7]:
                    wind_direction = str(weather_data[i][7]) + '°'
                else:
                    wind_direction = 'Ветра нет'
                text_ += f'\nМетеостанция: _{weather_station_name}_\n' \
                         f'Дата и время: {weather_date}\n' \
                         f'Температура: {weather_data[i][1]}°\n' \
                         f'Влажность: {weather_data[i][2]}%\n' \
                         f'Давление (по барометру): {weather_data[i][3]} мм\n' \
                         f'Текущие осадки: {weather_data[i][4] if weather_data[i][4] else 0} мм\n' \
                         f'Скорость ветра: {wind_speed}\n' \
                         f'Порывы ветра: {gusts_speed}\n' \
                         f'Направление ветра: {wind_direction}\n'
        return text_

    data = parse_query(query=query)
    text = parse_weather_data(weather_data=db.get_weather_data_from_agro(agro_id=int(data.get('agro'))))

    if not text:
        bot.send_chat_action(chat_id=query.message.chat.id, action='typing')
        text = 'Невозможно получить доступ к текущей погоде. Потеряно соединение с базой данных.\n ' \
               'Пожалуйста, повторите попытку позже'

    keyboard = create_button('back_to_weather_agro_menu', 'back_to_menu')
    send_bot_message(users=query.message.chat.id, text=text, keyboard=keyboard)


class WeatherArchive:
    """ Класс создания меню архива погоды"""

    def __init__(self, query: telebot.types.CallbackQuery):
        self.query = query
        self.data = parse_query(query=query)

    @get_agro_from_user_classmethod
    def get_weather_archive_stations(self) -> None:
        """Меню выбора метеостанции для показа архива"""
        keyboard = create_button('archive_stations', 'back_to_archive_agro_menu', 'back_to_menu',
                                 agro_id=self.data.get('agro'))
        send_bot_message(users=self.query.message.chat.id, text='Выберите необходимую метеостанцию:',
                         keyboard=keyboard)

    def get_archive_stations_date(self):
        """ Меню выбора даты архива"""
        data = parse_query(query=self.query)

        # Создание кнопок меню
        args = ['archive_stations_date', 'back_to_archive_stations', 'back_to_archive_agro_menu', 'back_to_menu']
        kwargs = {
            'station_id': int(data.get('station')),
            'agro_id': int(data.get('agro'))
        }

        keyboard = create_button(*args, **kwargs)
        bot.send_message(chat_id=self.query.message.chat.id,
                         text='Выберите необходимую неделю в архиве:',
                         reply_markup=keyboard)

    def answer_about_archive_weather(self) -> None:
        """ Ответ на запрос пользователя по архиву погоды"""

        def get_range(week: int) -> datetime.date:
            """Получение диапазона для работы с архивом"""
            if week == 1:
                date_range_1_ = datetime.now().date()
                date_range_2_ = datetime.now().date() - timedelta(days=7)
            elif week == 2:
                date_range_1_ = datetime.now().date() - timedelta(days=7)
                date_range_2_ = datetime.now().date() - timedelta(days=14)
            elif week == 3:
                date_range_1_ = datetime.now().date() - timedelta(days=14)
                date_range_2_ = datetime.now().date() - timedelta(days=21)
            else:
                date_range_1_ = datetime.now().date() - timedelta(days=21)
                date_range_2_ = datetime.now().date() - timedelta(days=28)
            return date_range_1_, date_range_2_

        data = parse_query(query=self.query)
        date_range_1, date_range_2 = get_range(week=int(data.get('week')))
        search_date = date_range_2

        station_name = db.get_weather_station_name(weather_station_id=int(data.get('station')))
        text = f'*Архив погоды* метеостанции {station_name}:\n\n'

        while search_date <= date_range_1:
            weather_data = db.get_weather_archive(station_id=int(data.get('station')), date=search_date)
            if weather_data:
                text += f'Дата: {search_date}\n' \
                        f'Макс.темп: {str(round(weather_data[0][0], 1)) + "°" if weather_data[0][0] else "Нет данных"}\n' \
                        f'Мин.темп: {str(round(weather_data[0][1], 1)) + "°" if weather_data[0][1] else "Нет данных"}\n' \
                        f'Ср.темп: {str(round(weather_data[0][2], 1)) + "°" if weather_data[0][2] else "Нет данных"}\n' \
                        f'Осадки: {str(round(weather_data[0][3], 1)) if weather_data[0][3] else 0} мм\n\n'
            search_date += timedelta(days=1)

        args = ['back_to_archive_stations', 'back_to_archive_station_week_menu', 'back_to_archive_agro_menu',
                'back_to_menu']
        kwargs = {
            'station_id': int(data.get('station')),
            'agro_id': int(data.get('agro'))
        }

        keyboard = create_button(*args, **kwargs)
        bot.send_chat_action(chat_id=self.query.message.chat.id, action='typing')
        send_bot_message(users=self.query.message.chat.id, text=text, keyboard=keyboard)


class Forecast:
    """ Класс создания меню прогноза погоды"""

    def __init__(self, query: telebot.types.CallbackQuery):
        self.query = query
        self.data = parse_query(query=query)

    @get_agro_from_user_classmethod
    def get_forecast_zone(self):
        """ Получение id микрозоны по выбранному хозяйству"""
        args = ['forecast_zones', 'back_to_forecast_agro_menu', 'back_to_menu']
        kwargs = {
            'agro_id': int(self.data.get('agro'))
        }
        keyboard = create_button(*args, **kwargs)

        if platform.system() == 'Linux':
            file = open(f"/home/sysop/telegrambot/zones_{int(self.data.get('agro'))}.png", 'rb')
        elif platform.system() == 'Windows':
            file = open(f"media/zones_{int(self.data.get('agro'))}.png", 'rb')
        else:
            file = None
        text = 'Выберите необходимую микрозону:'
        if file:
            bot.send_photo(chat_id=self.query.message.chat.id, caption=text, reply_markup=keyboard, photo=file)
        else:
            send_bot_message(users=self.query.message.chat.id, text=text, keyboard=keyboard)

    def get_forecast_zone_date(self):
        """ Строит меню выбора даты прогноза погоды для указанной микрозоны"""
        args = ['forecast_zones_date', 'back_to_forecast_zones', 'back_to_forecast_agro_menu', 'back_to_menu']
        kwargs = {
            'zone_id': self.data.get('zone'),
            'agro_id': self.data.get('agro')
        }
        keyboard = create_button(*args, **kwargs)
        send_bot_message(users=self.query.message.chat.id, text='Выберите дату прогноза:', keyboard=keyboard)

    def answer_about_forecast(self) -> None:
        """ Ответ на запрос о прогнозе погоды в выбранном Агро и заданной микрозоне"""

        def parse_forecast_data(zone: tuple or None) -> str:
            """Парсит данные по прогнозу погоды на выбранную дату для заданной микрозоны"""
            text_ = ''
            if zone:
                zone_name = db.get_forecast_name(zone_id=self.data.get('zone'))
                text_ = f'*Прогноз погоды* на {zone[0].date()} по микрозоне: _{zone_name}_\n\n' \
                        f'Общий прогноз: {zone[1]}\n' \
                        f'Средние осадки: {round(zone[2], 1)} мм/ч\n' \
                        f'Максимальные осадки: {round(zone[3], 1)} мм/ч\n' \
                        f'Точка росы: {round(zone[4], 1)}°\n' \
                        f'Влажность: {round(zone[5], 1)}%\n' \
                        f'Давление: {round(zone[6] / 1.333, 1)} мм\n' \
                        f'Мин.темп: {round(zone[7], 1)}° в {zone[9].time()}\n' \
                        f'Макс.темп: {round(zone[8], 1)}° в {zone[10].time()}\n'
            return text_

        zone_id = self.data.get('zone')
        forecast_date = self.data.get('date')
        text = parse_forecast_data(zone=db.get_forecast_data_with_date(zone_id=zone_id, forecast_date=forecast_date))
        if not text:
            text = 'Невозможно получить доступ к данным. Потеряно соединение с базой данных.\n' \
                   'Пожалуйста, повторите попытку позже'
        args = ['back_to_forecast_zones_date', 'back_to_forecast_zones', 'back_to_forecast_agro_menu', 'back_to_menu']
        kwargs = {
            'zone_id': zone_id,
            'agro_id': self.data.get('agro')
        }
        keyboard = create_button(*args, **kwargs)
        send_bot_message(users=self.query.message.chat.id, text=text, keyboard=keyboard)


def answer_about_cameras(query: telebot.types.CallbackQuery) -> None:
    """ Ответ на запрос о состоянии камер по всем хозяйствам"""
    bot.send_chat_action(chat_id=query.message.chat.id,
                         action='typing')
    cameras = db.check_cameras()
    bot.send_chat_action(chat_id=query.message.chat.id,
                         action='typing')

    if cameras:
        msg = ''
        for camera in cameras:
            msg += f'\nНазвание камеры: {camera[1]}\n' \
                   f'Номер Агро: {camera[0]}\n' \
                   f'Ip камеры: {camera[2]}\n\n'
        text = 'Список камер, которые не работают на данный момент:\n' + msg
    else:
        text = 'Все камеры в рабочем состоянии'

    keyboard = create_button('back_to_menu')
    send_bot_message(users=query.message.chat.id, text=text, keyboard=keyboard)


def answer_about_weather_stations(query: telebot.types.CallbackQuery) -> None:
    """ Ответ на запрос о состоянии метеостанций"""

    bot.send_chat_action(chat_id=query.message.chat.id,
                         action='typing')
    weather_stations = db.check_weatherstations()
    bot.send_chat_action(chat_id=query.message.chat.id,
                         action='typing')

    if weather_stations:
        msg = ''
        for station in weather_stations:
            msg = f'\nМетеостанция: {station[2]}' \
                  f'\nID метеостанции: {station[0]}' \
                  f'\nIP-адрес: {station[7]}\n'
        text = 'Список метеостанций, которые не работают на данный момент:\n' + msg
    else:
        text = 'Все метеостанции в рабочем состоянии'

    keyboard = create_button('back_to_menu')
    send_bot_message(users=query.message.chat.id, text=text, keyboard=keyboard)


@get_agro_from_user
def answer_about_weather_battery(query: telebot.types.CallbackQuery) -> None:
    """ Ответ на запрос о состоянии батареек в выбранном Агро"""

    def parse_weather_battery_data(weather_data_battery: list) -> str:
        """ Парсит данные по напряжению батареек на метеостанции"""
        text_ = ''
        for i in range(0, len(weather_data_battery)):
            weather_station_name = db.get_weather_station_name(weather_station_id=weather_data_battery[i][9])

            if weather_data_battery[i][10]:
                voltage = str(weather_data_battery[i][10]) + ' В'
            else:
                voltage = 'Нет данных'

            text_ += f'\n*Статус батареи* на _{weather_station_name}_\n' \
                     f'Дата и время: {weather_data_battery[i][0]}\n' \
                     f'Напряжение батареи: {voltage}\n'
        return text_

    data = parse_query(query=query)
    text = parse_weather_battery_data(db.get_weather_data_from_agro(agro_id=data.get('agro')))
    keyboard = create_button('back_to_battery_agro_menu', 'back_to_menu')
    if not text:
        text = 'Невозможно получить доступ к данным. Потеряно соединение с базой данных.\n' \
               'Пожалуйста, повторите попытку позже'
    send_bot_message(users=query.message.chat.id, text=text, keyboard=keyboard)


# @TODO доделать меню Wialon
def answer_about_wialon(query: telebot.types.CallbackQuery) -> None:
    """ Ответ на запрос по Wialon"""
    keyboard = create_button('wialon_menu', 'help', 'back_to_menu')
    bot.send_message(chat_id=query.message.chat.id,
                     text='Данное меню является пустышкой, пока оно не работает\n\n'
                          'Основное меню _Wialon_:',
                     reply_markup=keyboard,
                     parse_mode='Markdown')


def insert_user_in_db(message: telebot.types.Message) -> None:
    """ Регистрация пользователя в системе бота"""

    def mention_user(user_id):
        """ Возвращает ссылку на пользователя в виде слова"""
        return f"[{'пользователь'}](tg://user?id={user_id})"

    user_data = {'name': message.chat.first_name,
                 'surname': message.chat.last_name,
                 'regisdate': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                 'telegram_id': message.chat.id,
                 'role': 1}

    db.registration_users(user_data=user_data)
    text_admin = '*[Автоматическое уведомление*]:\n' \
                 f'Новый {mention_user(user_data["telegram_id"])} ' \
                 'подал заявку для работы с ботом. Сделайте проверку учетной записи:'
    keyboard_admin = create_button('check_reg', user_data=user_data)

    text_user = 'Ваша заявка отправлена на рассмотрение. Пожалуйста, ожидайте подтверждения от администратора.\n' \
                'По окончанию проверки вам придёт уведомление.'

    send_bot_message(users=settings.ADMIN_ID, text=text_admin, keyboard=keyboard_admin)
    send_bot_message(users=message.chat.id, text=text_user)


def reg_user(query: telebot.types.CallbackQuery) -> None:
    """ Регистрирует или удаляет пользователя, в зависимости от переданных данных"""
    data = parse_query(query=query)
    db.confirm_reg(telegram_id=int(data.get('user')), check=data.get('check'))
    if data.get('check') == 'true':
        keyboard = create_button('menu', 'help', 'contact')
        send_bot_message(users=int(data.get('user')), text='*[Автоматическое уведомление]*\n'
                                                           'Ваша заявка одобрена. Вы можете начать работы с ботом, '
                                                           'для этого нажмите на одному из кнопок ниже:',
                         keyboard=keyboard)

    elif data.get('check') == 'delete':
        send_bot_message(users=int(data.get('user')), text='*[Автоматическое уведомление]*\n'
                                                           'Ваша заявка была отклонена. Вы не можете продолжить работу')


# @TODO необходимо доделать отправку сообщений в главной функции
@mult_threading
def main() -> None:
    """ Основная функция. Здесь содержатся все функции обработки входящих сообщений и запросов от пользователей."""

    @bot.message_handler(commands=['start'])
    def start_command(message: telebot.types.Message) -> None:
        """ Обработчик команды /start

        :param message:
            Входное сообщение от пользователя, которое обрабатывается декоратором message_handler.
            У данного параметра имеются атрибуты по типу: chat, from_user и др.
            На основе этих атрибутов можно работать с конкретным пользователем.
            Например: message.chat.id = это id пользователя Telegram.
        """
        keyboard = create_button('reg', 'contact', 'menu')
        bot.send_message(chat_id=message.chat.id,
                         text=f'Приветствуем, _{message.chat.first_name}_!\n'
                              'Это информационный бот _Geliopaxgeo_, для взаимодействия с ним, '
                              'вам необходимо выбрать одну из команд ниже:',
                         reply_markup=keyboard,
                         parse_mode='Markdown')

    @bot.message_handler(commands=['help'])
    @check_registration
    def help_command(message: telebot.types.Message) -> None:
        """ Обработчики команды /help

        :param message:
            Входное сообщение от пользователя, которое обрабатывается декоратором message_handler.
            У данного параметра имеются атрибуты по типу: chat, from_user и др.
            На основе этих атрибутов можно работать с конкретным пользователем.
            Например: message.chat.id = это id пользователя Telegram.
        """
        keyboard = create_button('help_menu', 'contact', 'back_to_menu')
        bot.send_message(chat_id=message.chat.id,
                         text='*Меню помощи*:\n'
                              'Выберите необходимый пункт меню, для получения дополнительной информации',
                         reply_markup=keyboard,
                         parse_mode='Markdown')

    @bot.message_handler(commands=['reg'])
    @check_registration
    def reg_command(message: telebot.types.Message):
        """ Обработчик команды /reg

        :param message:
            Входное сообщение от пользователя, которое обрабатывается декоратором message_handler.
            У данного параметра имеются атрибуты по типу: chat, from_user и др.
            На основе этих атрибутов можно работать с конкретным пользователем.
            Например: message.chat.id = это id пользователя Telegram.
        """
        if not db.check_user(telegram_id=message.chat.id):
            msg = bot.send_message(chat_id=message.chat.id,
                                   text='Отлично! Вы подали заявку для подключения бота...')
            bot.send_chat_action(chat_id=message.chat.id,
                                 action='typing')
            sleep(3)
            bot.delete_message(chat_id=message.chat.id,
                               message_id=msg.message_id)
            insert_user_in_db(message=message)

    @bot.message_handler(commands=['contact'])
    def contact_command(message: telebot.types.Message) -> None:
        """ Обработчики команды /contact

        :param message:
            Входное сообщение от пользователя, которое обрабатывается декоратором message_handler.
            У данного параметра имеются атрибуты по типу: chat, from_user и др.
            На основе этих атрибутов можно работать с конкретным пользователем.
            Например: message.chat.id = это id пользователя Telegram.
        """
        if db.check_reg_status(telegram_id=message.chat.id):
            keyboard = create_button('admin', 'menu')
        else:
            keyboard = create_button('admin')
        bot.send_message(chat_id=message.chat.id,
                         text='Контактные данные администратора: Ильменский Максим\n'
                              'Мобильный телефон: +7(904)433-44-20\n'
                              'Email: geo@geliopax.ru\n'
                              'Для связи с администратором через сообщения telegram нажмите кнопку ниже',
                         reply_markup=keyboard)

    @bot.message_handler(commands=['menu'])
    @check_registration
    def menu_command(message: telebot.types.Message) -> None:
        """ Основное меню. Здесь происходят вся магия.
        :param message:
            Входное сообщение от пользователя, которое обрабатывается декоратором message_handler.
            У данного параметра имеются атрибуты по типу: chat, from_user и др.
            На основе этих атрибутов можно работать с конкретным пользователем.
            Например: message.chat.id = это id пользователя Telegram.
        """
        # @TODO подумать над системой ролей
        role = db.get_role(telegram_id=message.chat.id)
        if role == 2:
            keyboard = create_button('weather', 'archive', 'forecast', 'cameras', 'weather_stations', 'battery',
                                     'wialon', 'admin_menu', 'help')
        elif role == 3:
            keyboard = create_button('weather', 'archive', 'forecast', 'cameras', 'weather_stations', 'battery',
                                     'wialon', 'help')
        elif role == 4:
            keyboard = create_button('weather', 'archive', 'forecast', 'cameras', 'wialon', 'help')
        elif role == 9999:
            keyboard = None
            bot.send_message(chat_id=message.chat.id, text='Основное меню недоступно. У вас нет доступа')
        else:
            keyboard = create_button('weather', 'archive', 'forecast', 'wialon', 'help')

        bot.send_message(chat_id=message.chat.id,
                         text='Основное меню _Geliopaxgeo_:',
                         reply_markup=keyboard,
                         parse_mode='Markdown')

    @bot.message_handler(commands=['admin'])
    @check_registration
    @check_permission
    def admin_command(message: telebot.types.Message) -> None:
        """ Обработчик команды /admin"""
        pass
        # args = ['users_list', 'users_list_without_reg']

    @bot.message_handler(content_types=['text', 'photo', 'audio'])
    @check_registration
    def other_messages(message: telebot.types.Message) -> None:
        """ Обработчик всех левых сообщений

        :param message:
            Входное сообщение от пользователя, которое обрабатывается декоратором message_handler.
            У данного параметра имеются атрибуты по типу: chat, from_user и др.
            На основе этих атрибутов можно работать с конкретным пользователем.
            Например: message.chat.id = это id пользователя Telegram.
        """
        keyboard = create_button('reg', 'help', 'contact', 'menu')
        bot.send_message(chat_id=message.chat.id,
                         text='Я вас не понимаю. Для взаимодействия нажмите на одну из кнопок ниже:',
                         reply_markup=keyboard)

    @bot.callback_query_handler(func=lambda call: True)
    def button_handler(query: telebot.types.CallbackQuery) -> None:
        """ Обработчик нажатия клавиш в меню пользователя

        :param query:
            Переменная, отвечающая за нажатие кнопки. На основе данных
            атрибутов query можно выполнить необходимый запрос от пользователя
        """
        print(query.data)
        data = parse_query(query=query)
        bot.answer_callback_query(callback_query_id=query.id)
        delete_message(query=query)

        # Кнопка основное меню
        if data.get('button') == 'menu':
            menu_command(message=query.message)

        # Кнопка заявки для работы с ботом
        elif data.get('button') == 'reg':
            reg_command(message=query.message)

        # Кнопка подтверждения или удаления пользователя при регистрации
        elif data.get('button') == 'check_reg':
            reg_user(query=query)

        # Кнопка меню помощи
        elif data.get('button') == 'help':
            help_command(message=query.message)

        # Кнопка меню контакт
        elif data.get('button') == 'contact':
            contact_command(message=query.message)

        # Блок обработки меню с текущей погодой
        elif data.get('button') == 'weather':
            answer_about_weather(query)

        # Блок обработки меню с архивом погоды
        elif data.get('button') == 'archive':
            WeatherArchive(query=query).get_weather_archive_stations()

        elif data.get('button') == 'archive_stations':
            WeatherArchive(query=query).get_archive_stations_date()

        elif data.get('button') == 'archive_stations_date':
            WeatherArchive(query=query).answer_about_archive_weather()

        elif data.get('button') == 'forecast':
            Forecast(query=query).get_forecast_zone()

        elif data.get('button') == 'forecast_zones':
            Forecast(query=query).get_forecast_zone_date()

        elif data.get('button') == 'forecast_zones_date':
            Forecast(query=query).answer_about_forecast()

        # Обработка запроса статуса камер видеонаблюдения
        elif data.get('button') == 'cameras':
            answer_about_cameras(query=query)

        # Обработка запроса статуса метеостанций
        elif data.get('button') == 'weather_stations':
            answer_about_weather_stations(query=query)

        # Запрос о статусе батареек метеостанции
        elif data.get('button') == 'battery':
            answer_about_weather_battery(query=query)

        # Вызов меню Wialon
        elif data.get('button') == 'wialon':
            answer_about_wialon(query=query)

        # @TODO доделать обработчик оставшихся кнопок
        elif data.get('button') == 'help_weather':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button('contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи о текущей погоде*:\n'
                                  'Данное меню предназначено для снятия показаний '
                                  'с метеостанции на текущий момент.\n\n'
                                  '_Если вы сотрудник Агро_:\n'
                                  'После нажатия кнопки, вам придёт сообщение о '
                                  'текущей погоде по всем метеостанциям,'
                                  ' которые находятся в вашем хозяйстве.\n\n'
                                  '_Если вы сотрудник "ВГП"_:\n'
                                  'Для начала вам необходимо выбрать нужное хозяйство. После нажатия кнопки, '
                                  'вам придёт сообщение о текущей погоде по всем метеостанциям, '
                                  'которые находятся в выбранном вами хозяйстве.\n\n'
                                  '*ВОЗМОЖНЫЕ ПРОБЛЕМЫ*:\n'
                                  '1) Нет данных\n'
                                  '*Решение*: необходимо подождать восстановления соединения с ботом '
                                  'или сообщить администратору о сбое.\n'
                                  '2) Данные неактуальны. \n'
                                  'Например, данные за 9 утра, а текущее время 15 часов дня.\n'
                                  '*Решение*: возможно в хозяйстве нет электричества или интернета. '
                                  'В данном случае, '
                                  'данные будут подгружены автоматически, как только появится свет и сеть.',
                             reply_markup=keyboard,
                             parse_mode='Markdown')

        elif data.get('button') == 'help_forecast':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button(
                    'contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи о прогнозе погоды*:\n'
                                  'Данное меню предназначено для получения прогноза '
                                  'погоды по выбранной микрозоне.\n\n'
                                  '_Если вы сотрудник Агро_:\n'
                                  'После нажатия кнопки, вам потребуется выбрать '
                                  'нужную вам микрозону. Для удобства, '
                                  'к сообщению прикреплена карта с обозначениями микрозон.\n'
                                  'Выберите необходимую микрозону и дату, для получения прогноза погоды\n\n'
                                  '_Если вы сотрудник "ВГП"_:\n'
                                  'Для начала вам необходимо выбрать нужное хозяйство. Для удобства, '
                                  'к сообщению прикреплена карта с обозначениями микрозон.\n'
                                  'После нажатия кнопки выберите необходимую микрозону и '
                                  'дату, для получения прогноза погоды\n\n'
                                  '*ВОЗМОЖНЫЕ ПРОБЛЕМЫ*:\n'
                                  '1) Нет данных\n'
                                  '*Решение*: необходимо подождать восстановления соединения с ботом '
                                  'или сообщить администратору о сбое.\n'
                                  '2) Данные неактуальны\n'
                                  '*Решение*: Данные о прогнозе погоды обновляются каждый день в 23-24 часа. '
                                  'Поэтому, если данные неактуальны - то требуется подождать указанное время.\n'
                                  '3) Данные неточные\n'
                                  '*Ответ*: К сожалению, это лишь прогноз а не 100% верные данные, '
                                  'поэтому могут быть неточности',
                             reply_markup=keyboard,
                             parse_mode='Markdown')

        elif data.get('button') == 'help_cameras':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button('contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи о статусе камер видеонаблюдения*:\n'
                                  'Данное меню предназначено для уведомления о работе камер.\n'
                                  '\n*ВНИМАНИЕ*:\n'
                                  'Данное меню вам будет доступно, только если вы сотрудник '
                                  'охраны или сотрудник, обслуживающий камеры!\n\n'
                                  'После нажатия кнопки, произойдет один из двух вариантов сообщений:\n'
                                  'Если все камеры в порядке - придёт соответствующее уведомление.\n'
                                  'Если хотя бы одна из камер в хозяйстве не работает - '
                                  'придёт уведомление, с указанием необходимых данных, по каждой камере.\n\n'
                                  'Так же, существует автоматическая система уведомлений, '
                                  'но данное меню требуется для ручной проверки статуса камер\n\n'
                                  '*ВОЗМОЖНЫЕ ПРОБЛЕМЫ*:\n'
                                  '1) Нет данных\n'
                                  '*Решение*: необходимо подождать восстановления соединения с ботом '
                                  'или сообщить администратору о сбое.',
                             reply_markup=keyboard,
                             parse_mode='Markdown')

        elif data.get('button') == 'help_alert':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button(
                    'contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи об автоматических уведомлениях*:\n'
                                  'Данные сообщения приходят автоматически в необходимое время\n '
                                  'или по мере поступления данных. Отвечать на данные сообщения не требуется',
                             reply_markup=keyboard,
                             parse_mode='Markdown')

        elif data.get('button') == 'help_battery':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button(
                    'contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи о статусе батарей на метеостанциях*:\n'
                                  'Данный раздел меню присылает актуальные данные '
                                  'по напряжению батареи в каждой метеостанции по выбранном хозяйству. '
                                  'Так же, существует автоматическое уведомление о '
                                  'достижении нижнего порога напряжения\n\n'
                                  '*ВОЗМОЖНЫЕ ПРОБЛЕМЫ*:\n'
                                  '1) Нет данных\n'
                                  '*Решение*: необходимо подождать восстановления соединения с ботом '
                                  'или сообщить администратору о сбое.',
                             reply_markup=keyboard,
                             parse_mode='Markdown')

        elif data.get('button') == 'help_sentinel':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button('contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи о спутниковых снимках*:\n'
                                  'Спутниковые снимки являются важной составляющей, для анализа состояния полей.'
                                  'Данный бот автоматически информирует о появлении нового спутниково снимка\n\n'
                                  '*ВОЗМОЖНЫЕ ПРОБЛЕМЫ*:\n'
                                  '1) На снимке слишком большая облачность\n'
                                  '*Решение*: Необходимо сообщить об этом Администратору\n'
                                  '2) Индекс NDVI имеет слишком резкие отклонения, не связанные с аномалиями на полях\n'
                                  '*Решение*: Возможно, в зональную статистику NDVI попали значения облачности.'
                                  'Если, на полях не замечены патологии или иные признаки, которые могли бы вызвать '
                                  'резкое отклонение индекса от нормы - значит облачность попала в статистику',
                             reply_markup=keyboard,
                             parse_mode='Markdown')

        elif data.get('button') == 'help_wialon':
            bot.answer_callback_query(callback_query_id=query.id)
            delete_message(query=query)
            keyboard = create_button(
                    'contact', 'back_to_help_menu', 'back_to_menu')
            bot.send_message(chat_id=query.message.chat.id,
                             text='*Раздел помощи о меню Wialon*:\n'
                                  'Данный раздел пока ещё не имеет никакого функционала. Ждите обновлений! ',
                             reply_markup=keyboard,
                             parse_mode='Markdown')
        # @TODO добавить кнопку обратки для метеостанций в меню help
        elif data == 'help_weather_stations':
            send_bot_message(users=query.message.chat.id, text='Данное меню недоработано')

        elif data.get('button') == 'admin_menu':
            admin_command(message=query.message)


@mult_threading
def alert_messages_about_sentinel(agro_id: int) -> None:
    """ Автоматическое уведомление пользователей о публикации нового спутникого снимка по каждому хозяйству.
        О снимках не оповещаются сотрудники офиса, но идёт отдельное сообщение Администратору

    :param agro_id:
        Номер агро
    """
    while True:
        current_id = db.get_max_id_from_layer(agro_id=agro_id) + 1
        while current_id > db.get_max_id_from_layer(agro_id=agro_id):
            sleep(5)

        text = '*[Автоматическое уведомление]*:\n' \
               'Опубликован новый спутниковый снимок по хозяйству Гелио-Пакс Агро {workplace}.\n' \
               'Вы можете просмотреть его на сайте _Geliopaxgeo_.'
        send_bot_message(users=settings.ALERTS_SENTINEL, text=text, back=True)


@mult_threading
def alerts_rain() -> None:
    """ Автоматическое уведомление о выпавших осадках за последние сутки каждое утро.
        Уведомления для сотрудников Агро присылаются только по их хозяйству
    """

    def get_rain_data_from_weather_stations() -> list:
        """ Получает данные по сумме осадков за прошедшие сутки
        :return:
            Список списков
        :example:
            >>> get_rain_data_from_weather_stations()
            [['0.3', 'Новокиевка'], ['1.7', 'Красноармейский']]
        """
        all_rain_data = []
        for agro_id in range(1, 7):
            if agro_id == 2:
                continue
            weather_station_id = db.get_weather_station_id_from_agro(agro_id=agro_id)

            for station_id in weather_station_id:
                sum_rains = db.get_amount_of_precipitation_for_the_last_day(weather_station_id=station_id[0])
                name_station = db.get_weather_station_name(weather_station_id=station_id[0])
                if sum_rains is not None:
                    all_rain_data.append([sum_rains, name_station])
        return all_rain_data

    # @TODO Доделать рассылку для работников агро
    def send_message(data_rains: list) -> None:
        """ Отправляет сообщению пользователю о сумме осадков"""
        output_str_office = ''
        for data in data_rains:
            if data[0] != 0:
                output_str_office += f'\n\nМетеостанция: {data[1]}\nОсадки: {data[0]} мм'

        if output_str_office:
            text = '*[Автоматическое уведомление]*:\n ' \
                   'Выпавшие осадки за период \n' \
                   f'с {date_start.date()} по {date_end.date()}:\n' \
                   'По всем хозяйствам Гелио-Пакс Агро:' + output_str_office
        else:
            text = '*[Автоматическое уведомление]*:\n' \
                   f'За период с {date_start.date()} по {date_end.date()} осадков ' \
                   'во всех хозяйствах Гелио-Пакс Агро не было.\n'

        send_bot_message(users=settings.ALERTS_RAIN, text=text, back=True)

    time_ = time(8, 00)
    date_end = datetime.combine(datetime.now().date(), time_)
    date_start = datetime.combine(datetime.now().date() - timedelta(days=1), time_)

    all_agro_rains = get_rain_data_from_weather_stations()
    if all_agro_rains:
        send_message(all_agro_rains)


@mult_threading
def alert_forecast_volgograd() -> None:
    """ Автоматическое уведомление о погоде на текущий и завтрашний день в Волгограде"""

    def take_data_from_api_weather() -> list or None:
        """ Запрашивает данные из API о погоде в Волгограде
        :return:
            Список данных о погоде
        """
        try:
            res = requests.get("http://api.openweathermap.org/data/2.5/forecast",
                               params={'q': 'Volgograd, Ru',
                                       'units': 'metric',
                                       'APPID': settings.FORECAST_API_ID,
                                       'lang': 'ru'})
            weather_data = res.json()
            return weather_data
        except requests.exceptions.RequestException:
            return None

    def parse_weather_data(weather_data: dict = None) -> tuple or None:
        """ Парсит данные о погоде и переводит их в сообщение

        :param weather_data:
            Список данных о погоде
        :return:
            4 строки с данными о погоде или None
        """

        def get_weather_data_str(a: dict, weather_str: str) -> str:
            """ Превращает данные по

            :param weather_str:
                Строка с данными о погоде
            :param a:
                Список данных с погодой
            :return:
                Сформированную строку с данными
            """
            weather_str += f'\n*Время: {datetime.fromtimestamp(i["dt"]).time().strftime("%H:%M")}*\n' \
                           f'Погодные условия: {a["weather"][0]["description"]}\n' \
                           f'Температура воздуха: {a["main"]["temp"]}°\n' \
                           f'Ощущается как: {a["main"]["feels_like"]}°\n' \
                           f'Ветер: {a["wind"]["speed"]} м/с\n' \
                           f'Давление: {round(a["main"]["pressure"] * 0.75006375541921)} мм рт. ст.\n'
            return weather_str

        if weather_data:
            today = datetime.now().date()
            tomorrow = (datetime.now() + timedelta(days=1)).date()

            # Шапка сообщений на завтра и сегодня
            header = '*[Автоматическое уведомление]*\n ' \
                     f'Прогноз погоды по *Волгограду* на '

            header_weather_today = header + f'{today} (*сегодня*):\n'
            header_weather_tomorrow = header + f'{tomorrow} (*завтра*):\n'

            weather_str_today = ''
            weather_str_tomorrow = ''

            for i in weather_data['list']:
                date_weather = datetime.fromtimestamp(i["dt"]).date()
                if date_weather == today:
                    weather_str_today = get_weather_data_str(
                            i, weather_str_today)
                elif date_weather == tomorrow:
                    weather_str_tomorrow = get_weather_data_str(
                            i, weather_str_tomorrow)
            return header_weather_today, weather_str_today, header_weather_tomorrow, weather_str_tomorrow
        return None

    # Получаем данные о погоде на сегодня и завтра
    header_today, weather_today, header_tomorrow, weather_tomorrow = parse_weather_data(take_data_from_api_weather())
    # Текущее время для сравнения
    time_ = datetime.now().time()
    # Прогноз погоды на сегодня
    if time(5, 59) < time_ < time(18, 2):
        if weather_today and header_today:
            send_bot_message(users=settings.ALERTS_FORECAST_VLG, text=header_today + weather_today, back=True)

    # Прогноз погоды на завтра
    if time(20, 59) < time_ < time(21, 2):
        if weather_tomorrow and header_tomorrow:
            send_bot_message(users=settings.ALERTS_FORECAST_VLG, text=header_tomorrow + weather_tomorrow, back=True)


@mult_threading
def alert_about_weather_stations() -> None:
    """ Автоматическая проверка статуса метеостанций"""
    while True:
        sleep(1)
        if time(8, 00) <= datetime.now().time() <= time(17, 00) and (datetime.now().isoweekday() != 6 or
                                                                     datetime.now().isoweekday() != 7):
            flag_1, flag_2 = False, False

            weatherstations_1 = db.check_weatherstations()
            if weatherstations_1:
                flag_1 = True
            sleep(60)

            weatherstations_2 = db.check_weatherstations()
            if weatherstations_2:
                flag_2 = True

            if flag_1 and flag_2:
                # Шапка сообщения
                header = '*[Автоматическое уведомление]*:\n' \
                         f'Список метеостанций, которые не работают в данный момент:'
                msg = ''

                for station in weatherstations_2:
                    # @TODO Необходимо убрать эту проверку, когда подключат 9-ую метеостанцию
                    if station[0] == 9:
                        continue
                    msg = f'\n\nМетеостанция: {station[2]}' \
                          f'\nID метеостанции: {station[0]}' \
                          f'\nIP-адрес: {station[7]}'

                if msg:
                    send_bot_message(users=settings.ALERTS_WEATHERSTATIONS, text=header + msg, back=True)
                sleep(7200)


@mult_threading
def alert_about_cameras() -> None:
    """ Автоматическая проверка статуса видеокамер"""
    while True:
        sleep(1)
        if time(8, 00) <= datetime.now().time() <= time(17, 00) and (datetime.now().isoweekday() != 6 or
                                                                     datetime.now().isoweekday() != 7):
            flag_1, flag_2 = False, False

            cameras_1 = db.check_cameras()
            if cameras_1:
                flag_1 = True
            sleep(60)

            cameras_2 = db.check_cameras()
            if cameras_2:
                flag_2 = True

            if flag_1 and flag_2:
                for cam in cameras_2:
                    # @TODO убрать эту проверку, когда камера заработает
                    if cam[1] == 'ГПА-5 | МТМ | КПП -> ворота':
                        continue
                    msg = f'*[Автоматическое уведомление]*:\n' \
                          f'Нет ответа от камеры {cam[-1]}\n' \
                          f'*Название камеры*: \n{cam[1]}\n' \
                          f'IP-адрес: {cam[2]}\n' \
                          f'\nМестоположение камеры: (см. ниже)'
                    lat = cam[3]
                    lon = cam[4]

                    if msg and lat and lon:
                        send_bot_message(users=settings.ALERTS_CAMERAS, text=msg)
                        send_bot_location(users=settings.ALERTS_CAMERAS, lon=lon, lat=lat, back=True)
                sleep(7200)


@mult_threading
def check_weather_data() -> None:
    """ Автоматическая проверка уведомления о поступлении нулевых данных"""
    while True:
        sleep(1)
        if time(8, 00) <= datetime.now().time() <= time(17, 00) and (datetime.now().isoweekday() != 6 or
                                                                     datetime.now().isoweekday() != 7):
            msg = ''
            header = ''
            weather_stations = db.get_list_weather_stations_id()
            for station_id in weather_stations:
                # @TODO убрать эту проверку, когда починят метеостанции
                if station_id == 13 or 12:
                    continue
                weather_data = db.get_weather_data(
                        weather_station_id=station_id)
                if weather_data:
                    for weather_line in weather_data:
                        if weather_line:
                            header = '*[Автоматическое уведомление]*\n ' \
                                     f'Meteo_{station_id} отправляет нулевые данные'
                            msg = f'\n\n{weather_line}'

                    if msg:
                        send_bot_message(users=settings.ALERTS_WEATHERSTATIONS, text=header + msg, back=True)
            sleep(7200)


def starts_threads() -> None:
    """ Запускает указанные потоки"""

    # Основные функции бота
    main()

    # Список агро
    agro_ = [1, 3, 4, 5, 6]
    # Уведомления о спутниковых снимках, каждый поток для своего Агро
    for agro in agro_:
        alert_messages_about_sentinel(agro)

    # Уведомления о нерабочих метеостанциях
    alert_about_weather_stations()

    # Уведомления о нерабочих камерах видеонаблюдения
    alert_about_cameras()

    # Уведомления о погоде в Волгограде
    for time_ in settings.TIMES_FORECAST_VLG:
        RepeatedTimer((1., 1), timer, time_, 'alert_forecast_volgograd')

    # Уведомления о выпавших осадках по всем хозяйствам Гелио-Пакс Агро
    RepeatedTimer((1., 1), timer, '08:00:00', 'alerts_rain')


if __name__ == '__main__':
    # Запускаем все потоки
    starts_threads()
    # Включаем бота в режим бесконечной работы с перехватом ошибок и вылетов
    logging.critical(f'Количество потоков, работающие в данный момент: {threading.active_count()}')
    bot.infinity_polling()
