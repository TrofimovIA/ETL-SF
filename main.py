import logging
import re
from datetime import date, datetime, timedelta
import requests
import psycopg2
from psycopg2 import Error
import os
import secure_data
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
import ssl
from email.message import EmailMessage


class FromApiToDatabase:
    def __init__(self, data, database, user, password, host, port):
        self.data = data
        # данные для подключения к базе
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        # данные для выгрузки в Google таблицы
        self.summary = {}

    # получение данных из api
    @staticmethod
    def collect_api_data(api_url, params):
        try:
            logging.info(f'Request to receive data')
            response = requests.get(api_url, params=params)
            logging.info(f'Request completed. Status code: {response.status_code}')
            return response.json()
        except Exception as error:
            logging.error(f'Exception {error}')

    def get_data(self):
        try:
            return self.data
        except Exception as error:
            logging.error(f'Exception {error}')

    def get_summary(self):
        return self.summary

    @staticmethod
    def get_user_id(resp_dict):
        try:
            if resp_dict['lti_user_id']:
                return resp_dict['lti_user_id']
            else:
                logging.warning('Value user_id is missing')
        except Exception as error:
            logging.error(f'Exception {error}')

    # далее идут функции для извлечения необходимых полей из полученного реквеста
    @staticmethod
    def get_oauth_consumer_key(resp_dict):
        try:
            passback_params = eval(resp_dict['passback_params'])
            oauth_consumer_key = passback_params['oauth_consumer_key']
            if oauth_consumer_key:
                return oauth_consumer_key
            else:
                logging.warning('Value oauth_consumer_key is missing')
        except Exception as error:
            logging.error(f'Exception {error}')

    @staticmethod
    def get_lis_result_sourcedid(resp_dict):
        try:
            passback_params = eval(resp_dict['passback_params'])
            lis_result_sourcedid = passback_params['lis_result_sourcedid']
            if lis_result_sourcedid:
                return lis_result_sourcedid
            else:
                logging.warning('Value lis_result_sourcedid is missing')
        except Exception as error:
            logging.error(f'Exception {error}')

    @staticmethod
    def get_lis_outcome_service_url(resp_dict):
        try:
            passback_params = eval(resp_dict['passback_params'])
            if 'lis_outcome_service_url' in passback_params.keys():
                return passback_params['lis_outcome_service_url']
            else:
                logging.warning('Value lis_outcome_service_url is missing')

        except Exception as error:
            logging.error(f'Exception {error}')

    @staticmethod
    def get_is_correct(resp_dict):
        try:
            if resp_dict['is_correct'] in [0, 1] and resp_dict['attempt_type'] == 'run':
                raise ValueError
            elif resp_dict['is_correct'] == 0:
                return 0
            elif resp_dict['is_correct']:
                return resp_dict['is_correct']
        except Exception as error:
            logging.error(f'Exception {error}')

    @staticmethod
    def get_attempt_type(resp_dict):
        try:
            attempt_type = resp_dict['attempt_type']
            if attempt_type in ['run', 'submit']:
                return f'{attempt_type}'
            else:
                raise ValueError("Value must be 'run' or 'submit'")
        except Exception as error:
            logging.error(f'Exception {error}')

    @staticmethod
    def get_created_at(resp_dict):
        try:
            created_at = resp_dict['created_at']
            pattern = r'(^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d*)$'
            if re.match(pattern, created_at):
                return created_at
            else:
                raise ValueError(f"Unknown date format: {created_at}")
        except Exception as error:
            logging.error(f'Exception {error}')

    def post_norm_data(self):
        try:
            # Подключение к сбазе данных
            logging.info('Connecting to the database')
            connection = psycopg2.connect(database=self.database,
                                          user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port)

            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()

            # записываем данные построчно
            logging.info('Writing values to the database')
            # словарь да агрегированных данных за день
            summary = {'date': date.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f'),
                       'attempts': 0,
                       'successful_attempts': 0,
                       'unique_users': 0}
            unique_users = set()
            for row in self.data:
                # достаём данные для загрузки в базу
                user_id = FromApiToDatabase.get_user_id(row)
                oauth_consumer_key = FromApiToDatabase.get_oauth_consumer_key(row)
                lis_result_sourcedid = FromApiToDatabase.get_lis_result_sourcedid(row)
                lis_outcome_service_url = FromApiToDatabase.get_lis_outcome_service_url(row)
                is_correct = FromApiToDatabase.get_is_correct(row)
                attempt_type = FromApiToDatabase.get_attempt_type(row)
                created_at = FromApiToDatabase.get_created_at(row)

                values_dict = {'user_id': user_id,
                               'oauth_consumer_key': oauth_consumer_key,
                               'lis_result_sourcedid': lis_result_sourcedid,
                               'lis_outcome_service_url': lis_outcome_service_url,
                               'is_correct': is_correct,
                               'attempt_type': attempt_type,
                               'created_at': created_at}
                # агрегируем данные для summary
                unique_users.add(user_id)
                if attempt_type:
                    summary['attempts'] += 1
                if is_correct == 1:
                    summary['successful_attempts'] += 1
                # Составляем запрос
                values = ''
                keys = ''
                for k, v in values_dict.items():
                    if v is not None:
                        values += f"'{v}', "
                        keys += f"{k}, "
                sql_request = "insert into simulative_api (" + keys[:-2] + ") values (" + values[:-2] + ")"
                # выполняем запрос
                cursor.execute(sql_request)
                connection.commit()

        except (Exception, Error) as error:
            logging.error(f'Error when working with PostgreSQL {error}')

        finally:
            summary['unique_users'] += len(unique_users)
            self.summary = summary
            if connection:
                cursor.close()
                connection.close()
                logging.info("PostgreSQL connection closed")

    def summary_to_gsheets(self):
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/spreadsheets',
                 'https://www.googleapis.com/auth/drive.file',
                 'https://www.googleapis.com/auth/drive']

        # Загружаем ключи аутентификации из файла json
        credentials = ServiceAccountCredentials.from_json_keyfile_name('simulativeapiproject-a17c11de6dce.json',
                                                                       scope)
        # Авторизуемся в Google Sheets API
        gc = gspread.authorize(credentials)

        # Записываем данные в Google таблицы
        sheet = gc.open('SimulativeApi_daily_summary').sheet1
        try:
            sheet.append_row([self.summary['date'],
                              self.summary['attempts'],
                              self.summary['successful_attempts'],
                              self.summary['unique_users']])
        except Exception as error:
            logging.error(error)

    @staticmethod
    def del_old_logs():
        files = os.listdir('logs')
        actual_files = [date.strftime(date.today(), '%Y-%m-%d') + '_log.log',
                        date.strftime(date.today() - timedelta(days=1), '%Y-%m-%d') + '_log.log',
                        date.strftime(date.today() - timedelta(days=2), '%Y-%m-%d') + '_log.log']
        logging.info('Deleting old logs')
        try:
            for file in files:
                if file not in actual_files:
                    os.remove(f'logs/{file}')
            logging.info('Operation was completed successfully')
        except Exception as err:
            logging.error(f'Error when working with PostgreSQL {err}')

    @staticmethod
    def send_email(subject, message, from_email, to_email):
        context = ssl.create_default_context()
        msg = EmailMessage()
        subject = subject
        message = message

        msg.set_content(message)
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = to_email
        try:
            with smtplib.SMTP_SSL('smtp.mail.ru', 465, context=context) as server:
                server.login(from_email, secure_data.pass_mail)
                server.send_message(msg=msg)
        except Exception as error:
            logging.error(error)


# настраиваем логи
logging.basicConfig(level=logging.INFO,
                    filename=f"logs/{date.today()}_log.log",
                    filemode="w",
                    format='%(asctime)s %(name)s %(levelname)s: %(message)s')

# Данные для подключения к API (secure_data - отдельный файл для приватных данных)
api_url = "https://b2b.itresume.ru/api/statistics"
params = {'client': secure_data.client,
          'client_key': secure_data.client_key,
          'start': datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d %H:%M:%S.%f'),
          'end': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f')}

# данные для подключения к базе (secure_data - отдельный файл для приватных данных)
database = secure_data.database
user = secure_data.user
password = secure_data.password
host = secure_data.host
port = secure_data.port

# получаем данные
data_from_api = FromApiToDatabase.collect_api_data(api_url, params)

# создаем объект, в который передаем полученные из api данные для последующей обработки и загрузки в базу
today_data = FromApiToDatabase(data_from_api, database, user, password, host, port)
today_data.post_norm_data()

# удаляем старые логи
FromApiToDatabase.del_old_logs()

# записываем агрегированные данные за день в Google таблицы
today_data.summary_to_gsheets()

# Отправляем письмо об успешно выполненной работе
message = ('Данные успешно получены и загружены в базу данных. Итоговые агрегированные данные можно посмотреть здесь: '
           '\nhttps://docs.google.com/spreadsheets/d/1soi3IotpwBLZdtcBexfD1Hx3IZ3Fwt6UtRVNFGWAUUw/edit#gid=0')
subject = 'Отчёт о выполненной работе'

FromApiToDatabase.send_email(subject=subject,
                             message=message,
                             from_email=secure_data.from_email,
                             to_email=secure_data.to_email)
