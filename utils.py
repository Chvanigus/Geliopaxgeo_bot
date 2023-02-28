""" Утилиты для бота"""
import logging
from datetime import datetime
from functools import wraps
from threading import Thread, Timer

import telebot
from decouple import config
from telebot import types

import dboperator as db

logger = logging.getLogger('__name__')
bot = telebot.TeleBot(config('TOKEN', default=''))


class RepeatedTimer(object):
    """ Класс, запускающий и перезапускающий функцию в указанное время"""
    nruns = 0

    def __init__(self, times, function, *args, **kwargs):
        self._timer = None
        self.function = function
        self.times = times
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        """ Запуск функции"""
        if self.times[1] != -1 and type(self).nruns >= self.times[1]:
            self.stop()
            return

        if not self.is_running:
            self._timer = Timer(self.times[0], self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        """ Останавливает функцию"""
        self._timer.cancel()
        self.is_running = False


def parse_query(query) -> dict:
    """ Обрабатывает запрос query и разбивает данные на ключ/значение если есть разделитель"""
    parts = query.data.split(",")
    data = {}

    for part in parts:
        key, value = part.split(":")
        data[key] = value
    return data


def handle_input(func):
    """ Декоратор для проверки типа переданного аргумента"""

    @wraps(func)
    def wrapper(users, *args, **kwargs):
        """ Проверяет тип переменной"""
        # Если список - то запуск функции в цикле
        if isinstance(users, list):
            for item in users:
                func(item, *args, **kwargs)
        # Если число - то запуск один раз
        elif isinstance(users, int):
            func(users, *args, **kwargs)

    return wrapper


def mult_threading(func):
    """Декоратор для запуска функции в отдельном потоке"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        """ Запускает функцию в отдельном потоке и сообщает её название и переданные аргументы"""
        func_thread = Thread(target=func, daemon=True, args=tuple(args), kwargs=kwargs)
        func_thread.start()
        if args:
            logger.critical(f'Запущен поток: {func.__name__}. Используемые аргументы для запуска: {args}')
        else:
            logger.critical(f'Запущен поток: {func.__name__}. Аргументов нет')
        return func_thread
    return wrapper


def retry_send_msg(func):
    """ Декоратор отправки сообщений с бесконечным количеством попыток"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        """ Пытается отправить сообщение в бесконечном цикле"""
        while True:
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.critical(f'Невозможно отправить сообщение. Ошибка: {e}')
    return wrapper


@handle_input
@retry_send_msg
def send_bot_message(users: int or list, text: str, keyboard: telebot.types.InlineKeyboardMarkup = None,
                     back: bool = False) -> None:
    """ Отправляет сообщение с заданными параметрами"""
    bot.send_message(chat_id=users, text=text, parse_mode='Markdown', reply_markup=keyboard)
    # Если был передан флаг back - True, бот дополнительно присылает сообщение о возврате в главное меню
    if back:
        back_message(users=users)


@handle_input
@retry_send_msg
def send_bot_location(users: int or list, lon: float, lat: float, back: bool = False) -> None:
    """ Отправляет местоположение с заданными параметрами"""
    bot.send_location(chat_id=users, longitude=lon, latitude=lat)
    # Если был передан флаг back - True, бот дополнительно присылает сообщение о возврате в главное меню
    if back:
        back_message(users=users)


@handle_input
@retry_send_msg
def back_message(users: int or list) -> None:
    """ Отправляет сообщение, позволяющее вернуться в основное меню (кроме пользователей с ролью 9999)
    :param users:
        ID пользователя telegram
    """
    role = db.get_role(telegram_id=users)
    keyboard = create_button('menu')
    if role != 9999:
        bot.send_message(chat_id=users,
                         text='Возврат в _основное меню_:',
                         reply_markup=keyboard,
                         parse_mode='Markdown')


def delete_message(query) -> None:
    """ Попытка удаления сообщения. В случае ошибки - сообщение будет оставлено"""
    try:
        bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.id)
    except Exception as e:
        logger.critical(f'Ошибка при удалении сообщения: {e}')
        pass


@retry_send_msg
def check_registration(func):
    """ Декоратор проверки статуса регистрации пользователя"""

    @wraps(func)
    def wrapper(message, *args, **kwargs):
        """ Проверяет регистрацию пользователя и делает соответствующее предложение зарегистрироваться"""
        user = message.chat.id

        if not db.check_user(user):
            bot.send_chat_action(chat_id=user, action='typing')
            keyboard = create_button('reg')
            send_bot_message(users=user, text="Вы ещё не подавали заявку на регистрацию, для работы с ботом. "
                                              "Используйте команду /reg для подачи заявки на регистрацию "
                                              "или нажмите на кнопку ниже:",
                             keyboard=keyboard)
            return

        elif not db.check_reg_status(user):
            keyboard = create_button('contact')
            send_bot_message(users=user, text="Вы уже отправили запрос на регистрацию. Ожидайте подтверждения. "
                                              "Вам придёт автоматическое уведомление, "
                                              "с сообщением о статусе вашей заявки. Или напишите администратору, "
                                              "воспользовавшись кнопкой ниже:",
                             keyboard=keyboard)
            return
        else:
            return func(message, *args, **kwargs)

    return wrapper


@retry_send_msg
def check_permission(func):
    """ Декоратор проверки доступа к функции"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        """ Проверяет, что у пользователя имеется доступ (пока только для админки)"""
        if kwargs['query']:
            if db.get_role(telegram_id=kwargs['query'].message.chat.id) == 2:
                return func(*args, **kwargs)
            else:
                send_bot_message(users=kwargs['query'].message.chat.id,
                                 text='У вас нет доступа к этой команде',
                                 back=True)
                return
        elif kwargs['message']:
            if db.get_role(telegram_id=kwargs['message'].chat.id) == 2:
                return func(*args, **kwargs)
            else:
                send_bot_message(users=kwargs['message'].chat.id,
                                 text='У вас нет доступа к этой команде',
                                 back=True)
                return
    return wrapper


def get_agro_from_user_classmethod(func):
    """ Декоратор для методов класса"""

    def wrapper(self, *args, **kwargs):
        """ Обёртка для методов класса"""
        if 'agro' not in self.data:
            keyboard = create_button('agro', 'back_to_menu', flag=self.data.get('button'))
            send_bot_message(users=self.query.message.chat.id, text='Выберите нужный Агро...', keyboard=keyboard)
            return None
        return func(self, *args, **kwargs)

    return wrapper


def get_agro_from_user(func):
    """ Декоратор создания меню выбора Агро"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """ Строит меню выбора предприятия в том случае, если предприятие не было выбрано"""
        if len(args) > 0:
            query = args[0]
        else:
            query = kwargs['query']

        user_id = query.message.chat.id
        data = parse_query(query=query)

        if data.get('agro'):
            return func(*args, **kwargs)
        else:
            keyboard = create_button('agro', 'back_to_menu', flag=data.get('button'))
            send_bot_message(users=user_id, text='Выберите нужный Агро...', keyboard=keyboard)
            return None

    return wrapper


def create_button(*args: str,
                  agro_id: int = None,
                  zone_id: int = None,
                  station_id: int = None,
                  flag: str = None,
                  user_data: dict = None) -> telebot.types.InlineKeyboardMarkup:
    """ Создание клавиатуры на основе переданных аргументов.
    Порядок записи кнопок в аргументе функции определяет их порядок появления на клавиатуре.

    :param args:
        Это названия кнопок в строковом формате, которые должны быть созданы при вызове функции.
        Основные виды кнопок:
        'menu', 'reg', 'contact', 'help', 'back_to_menu' и др.
    :param agro_id:
        Параметр для создания меню для конкретного агро. По умолчанию - None
    :param zone_id:
        Параметр создания дат прогноза погоды по выбранной микрозоне. По умолчанию - None
    :param station_id:
        Параметр создания меню для выбранной метеостанции. По умолчанию - None
    :param flag:
        Параметр, определяющий меню выбора Агро
    :param user_data:
        Словарь с данными о пользователе, необходим для создания кнопки с регистрацией
    :return:
        Возвращает выходную клавиатуру, которую можно использовать при формировании сообщений от бота.
        Если она пустая - то ничего создано не будет
    """

    keyboard = types.InlineKeyboardMarkup()
    for button in args:
        # Кнопка основного меню
        if button == 'menu':
            key_menu = types.InlineKeyboardButton(text='Основное меню', callback_data='button:menu')
            keyboard.add(key_menu)

        # Кнопка возврата в основное меню
        elif button == 'back_to_menu':
            key_menu = types.InlineKeyboardButton(text='« Назад к основному меню', callback_data='button:menu')
            keyboard.add(key_menu)

        # Кнопка для заявки на работу с ботом
        elif button == 'reg':
            key_reg = types.InlineKeyboardButton(text='Подать заявку', callback_data='button:reg')
            keyboard.add(key_reg)

        elif button == 'check_reg' and user_data:
            key_true = types.InlineKeyboardButton(text='Одобрить', callback_data='button:check_reg,'
                                                                                 f'user:{user_data["telegram_id"]},'
                                                                                 f'check:true')
            key_false = types.InlineKeyboardButton(text='Проверить позже',
                                                   callback_data='button:check_reg,'
                                                                 f'user:{user_data["telegram_id"]},'
                                                                 f'check:false')
            key_delete = types.InlineKeyboardButton(text='Отклонить',
                                                    callback_data='button:check_reg,'
                                                                  f'user:{user_data["telegram_id"]},'
                                                                  f'check:delete')
            keyboard.add(key_true, key_delete)
            keyboard.add(key_false)

        # Кнопка для связи с администратором
        elif button == 'contact':
            key_contact = types.InlineKeyboardButton(text='Связаться с администратором', callback_data='button:contact')
            keyboard.add(key_contact)

        # Кнопка меню помощи
        elif button == 'help':
            key_help = types.InlineKeyboardButton(text='Помощь', callback_data='button:help')
            keyboard.add(key_help)

        # Кнопки для меню текущей погоды
        elif button == 'weather':
            key_weather = types.InlineKeyboardButton(text='Текущая погода',
                                                     callback_data='button:weather')
            keyboard.add(key_weather)

        # Кнопка возврата для выбора Агро в меню текущей погоды
        elif button == 'back_to_weather_agro_menu':
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору Агро',
                                                  callback_data='button:weather')
            keyboard.add(key_menu)

        # Кнопки для меню архива с погодой
        elif button == 'archive':
            key_archive = types.InlineKeyboardButton(text='Архив погоды', callback_data='button:archive')
            keyboard.add(key_archive)

        # Создаёт кнопку выбора метеостанции в меню архива
        elif button == 'archive_stations' and agro_id:
            weather_station_list = db.get_weather_station_id_from_agro(agro_id=agro_id)
            for station in weather_station_list:
                station_name = db.get_weather_station_name(weather_station_id=station[0])
                key_station = types.InlineKeyboardButton(text=f'{station_name}',
                                                         callback_data=f'button:archive_stations,'
                                                                       f'station:{station[0]},'
                                                                       f'agro:{agro_id}')
                keyboard.add(key_station)

        # Создаёт кнопку выбора даты для конкретной метеостанции в меню архива
        elif button == 'archive_stations_date' and station_id and agro_id:
            key_date = types.InlineKeyboardButton(text=f'Архив за последнюю неделю',
                                                  callback_data=f'button:archive_stations_date,'
                                                                f'week:1,'
                                                                f'station:{station_id},'
                                                                f'agro:{agro_id}')
            keyboard.add(key_date)
            for i in range(2, 5):
                key_date = types.InlineKeyboardButton(text=f'Архив {i} недели назад',
                                                      callback_data=f'button:archive_stations_date,'
                                                                    f'week:{i},'
                                                                    f'station:{station_id},'
                                                                    f'agro:{agro_id}')
                keyboard.add(key_date)

        # Возвращает пользователя к выбору метеостанции в меню архива погоды
        elif button == 'back_to_archive_stations' and agro_id:
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору метеостанции',
                                                  callback_data=f'button:archive,'
                                                                f'agro:{agro_id}')
            keyboard.add(key_menu)

        # Возвращает пользователя к выбору недели в меню архива погоды
        elif button == 'back_to_archive_station_week_menu' and agro_id and station_id:
            # Возвращает пользователя к выбору недели в меню Архив
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору недели',
                                                  callback_data=f'button:archive_stations,'
                                                                f'station:{station_id},'
                                                                f'agro:{agro_id}')
            keyboard.add(key_menu)

        # Кнопка возврата для выбора Агро в меню архива погоды
        elif button == 'back_to_archive_agro_menu':
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору Агро',
                                                  callback_data='button:archive')
            keyboard.add(key_menu)

        # Кнопки для меню прогноза погоды по микрозонам
        elif button == 'forecast':
            key_forecast = types.InlineKeyboardButton(text='Прогноз погоды по микрозонам',
                                                      callback_data='button:forecast')
            keyboard.add(key_forecast)

        # Кнопка выбора микрозоны для прогноза погоды по конкретному Агро
        elif button == 'forecast_zones' and agro_id:
            zones = db.get_zone_id_from_agro(agro_id=agro_id)
            for zone in zones:
                key_forecast = types.InlineKeyboardButton(text=f'{zone[1]}',
                                                          callback_data=f'button:forecast_zones,'
                                                                        f'zone:{zone[0]},'
                                                                        f'agro:{agro_id}')
                keyboard.add(key_forecast)

        # Кнопка выбора даты прогноза погоды по конкретной микрозоне
        elif button == 'forecast_zones_date' and zone_id and agro_id:
            dates = db.get_forecast_dates(zone_id=zone_id)
            for date in dates:
                forecast_date = datetime.strftime(date[0].date(), '%d-%m-%Y')
                key_forecast_date = types.InlineKeyboardButton(text=f'{forecast_date}',
                                                               callback_data=f'button:forecast_zones_date,'
                                                                             f'date:{date[0].date()},'
                                                                             f'zone:{zone_id},'
                                                                             f'agro:{agro_id}')
                keyboard.add(key_forecast_date)

        # Кнопка возврата для выбора микрозоны по конкретному Агро
        elif button == 'back_to_forecast_zones' and agro_id:
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору микрозоны',
                                                  callback_data=f'button:forecast,'
                                                                f'agro:{agro_id}')
            keyboard.add(key_menu)

        # Кнопка возврата для выбора даты прогноза погоды для конкретной микрозоны
        elif button == 'back_to_forecast_zones_date' and zone_id and agro_id:
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору даты',
                                                  callback_data=f'button:forecast_zones,'
                                                                f'zone:{zone_id},'
                                                                f'agro:{agro_id}')
            keyboard.add(key_menu)

        # Кнопка возврата для выбора Агро в меню прогноза погоды
        elif button == 'back_to_forecast_agro_menu':
            key_menu = types.InlineKeyboardButton(text='« Назад к выбору Агро',
                                                  callback_data='button:forecast')
            keyboard.add(key_menu)

        # Кнопка для проверки статуса камер видеонаблюдения
        elif button == 'cameras':
            key_cameras = types.InlineKeyboardButton(text='Статус камер видеонаблюдения',
                                                     callback_data='button:cameras')
            keyboard.add(key_cameras)

        # Кнопка для проверки статуса метеостанций
        elif button == 'weather_stations':
            key_cameras = types.InlineKeyboardButton(text='Статус метеостанций',
                                                     callback_data='button:weather_stations')
            keyboard.add(key_cameras)

        # Кнопка для проверки статуса батареек метеостанций
        elif button == 'battery':
            key_weather_battery = types.InlineKeyboardButton(text='Батарейки метеостанций',
                                                             callback_data='button:battery')
            keyboard.add(key_weather_battery)

        # Кнопка возврата для выбора Агро меню батареек метеостанций
        elif button == 'back_to_battery_agro_menu':
            key_weather_battery = types.InlineKeyboardButton(text='« Назад к выбору Агро',
                                                             callback_data='button:battery')
            keyboard.add(key_weather_battery)

        # Кнопка ссылки на telegram администратора
        elif button == 'admin':
            key_contact = types.InlineKeyboardButton(text='Написать администратору',
                                                     url=config.ADMIN_URL)
            keyboard.add(key_contact)

        # Кнопка на ссылку к меню Wialon
        elif button == 'wialon':
            key_wialon = types.InlineKeyboardButton(text='Меню "Виалона"',
                                                    callback_data='button:wialon')
            keyboard.add(key_wialon)

        # Меню Wialon
        elif button == 'wialon_menu':

            key_wialon = types.InlineKeyboardButton(text='Wialon в AppStore | Iphone',
                                                    url='https://apps.apple.com/ru/app/wialon-local/id1011136393')
            keyboard.add(key_wialon)

            key_wialon = types.InlineKeyboardButton(text='Wialon в PlayMarket | Android',
                                                    url='https://play.google.com/store/apps/details'
                                                        '?id=com.gurtam.wialon_local_1504&hl=ru&gl=US')
            keyboard.add(key_wialon)

            key_wialon = types.InlineKeyboardButton(text='Открыть Wialon в браузере:',
                                                    url='http://wialon.geliopax.ru/')
            keyboard.add(key_wialon)

        # @TODO добавить в меню кнопку по поводу метеостанций
        elif button == 'help_menu':
            key_help_1 = types.InlineKeyboardButton(text='Текущая погода',
                                                    callback_data='help_weather')
            key_help_2 = types.InlineKeyboardButton(text='Прогноз погоды',
                                                    callback_data='help_forecast')
            keyboard.add(key_help_1, key_help_2)

            key_help_1 = types.InlineKeyboardButton(text='Видеокамеры',
                                                    callback_data='help_cameras')
            key_help_2 = types.InlineKeyboardButton(text='Уведомления',
                                                    callback_data='help_alert')
            keyboard.add(key_help_1, key_help_2)

            key_help_1 = types.InlineKeyboardButton(text='Батарейки метео',
                                                    callback_data='help_battery')

            key_help_2 = types.InlineKeyboardButton(text='Спутниковые снимки',
                                                    callback_data='help_sentinel')
            keyboard.add(key_help_1, key_help_2)

            key_help = types.InlineKeyboardButton(text='Виалон',
                                                  callback_data='help_wialon')
            keyboard.add(key_help)

        elif button == 'back_to_help_menu':
            key_menu = types.InlineKeyboardButton(text='« Назад к меню помощи',
                                                  callback_data='help')
            keyboard.add(key_menu)

        # Кнопку agro обрабатываем в цикле (она создаёт ссылки на указанное хозяйство для нужного меню)
        elif button == 'agro':
            for agro_id in range(1, 7, 2):
                if flag == 'weather':
                    callback_key_1 = f'button:weather,agro:{agro_id}'
                    callback_key_2 = f'button:weather,agro:{agro_id + 1}'
                elif flag == 'archive':
                    callback_key_1 = f'button:archive,agro:{agro_id}'
                    callback_key_2 = f'button:archive,agro:{agro_id + 1}'
                elif flag == 'battery':
                    callback_key_1 = f'button:battery,agro:{agro_id}'
                    callback_key_2 = f'button:battery,agro:{agro_id + 1}'
                elif flag == 'forecast':
                    callback_key_1 = f'button:forecast,agro:{agro_id}'
                    callback_key_2 = f'button:forecast,agro:{agro_id + 1}'
                else:
                    callback_key_1 = f'button:workplace,agro:{agro_id}'
                    callback_key_2 = f'button:workplace,agro:{agro_id + 1}'

                if agro_id == 1:
                    key_1 = types.InlineKeyboardButton(text=f'ГПА {agro_id}',
                                                       callback_data=callback_key_1)
                    keyboard.add(key_1)
                else:
                    key_1 = types.InlineKeyboardButton(text=f'ГПА {agro_id}',
                                                       callback_data=callback_key_1)
                    key_2 = types.InlineKeyboardButton(text=f'ГПА {agro_id + 1}',
                                                       callback_data=callback_key_2)
                    keyboard.add(key_1, key_2)

        elif button == 'admin_menu':
            key_users = types.InlineKeyboardButton(text='Администрирование',
                                                   callback_data='button:admin_menu')
            keyboard.add(key_users)

    return keyboard
