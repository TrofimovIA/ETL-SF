# ETL-пайплайн для обучающей платформы
## Описание задачи

Задача - написать скрипт, в котором:
- Будет происходить обращение к API для получения данных
- Данные будут обрабатываться и готовиться к загрузке в базу данных
- Обработанные данные будут загружаться в локальную базу PostgreSQL
- Во время обработки будет сохраняться лог работы скрипта с отлавливанием всех ошибок и выводом промежуточных стадий (например, скачивание началось / скачивание завершилось / заполнение базы началось и т.д., с трекингом времени). Лог нужно сохранять в текстовый файл. Файл нужно именовать в соответствии с текущей датой. Если в папке с логами уже есть другие логи - их необходимо удалять, оставляем только логи за последние 3 дня.

Для взаимодействия с API и получения данных, используем библиотеку requests.

## Пример ответа от API:
```
[
    {
        "lti_user_id": "3583bf109f8b458e13ae1ac9d85c396a",
        "passback_params": "{'oauth_consumer_key': '', 'lis_result_sourcedid': 'course-v1:SF+DST-3.0+28FEB2021:lms.skillfactory.ru-ca3ecf8e5f284c329eb7bd529e1a9f7e:3583bf109f8b458e13ae1ac9d85c396a', 'lis_outcome_service_url': 'https://lms.sf.ru/courses/course-v1:sf+DST-3.0+28FEB2021/xblock/block-v1:SkillFactory+DST-3.0+28FEB2021+type@lti+block@ca3ecf8e5f284c329eb7bd529e1a9f7e/handler_noauth/grade_handler'}",
        "is_correct": null,
        "attempt_type": "run",
        "created_at": "2023-05-31 09:16:11.313646"
    },
    {
        "lti_user_id": "ab6ddeb7654ab35d44434d8db629bd01",
        "passback_params": "{'oauth_consumer_key': '', 'lis_result_sourcedid': 'course-v1:SkillFactory+DSPR-2.0+14JULY2021:lms.skillfactory.ru-0cf38fe58c764865bae254da886e119d:ab6ddeb7654ab35d44434d8db629bd01', 'lis_outcome_service_url': 'https://lms.sf.ru/courses/course-v1:sf+DSPR-2.0+14JULY2021/xblock/block-v1:sf+DSPR-2.0+14JULY2021+type@lti+block@0cf38fe58c764865bae254da886e119d/handler_noauth/grade_handler'}",
        "is_correct": null,
        "attempt_type": "run",
        "created_at": "2023-05-31 09:16:30.117858"
    }
]
```
## Данные для выгрузки:

Данные необходимо сохранить в базу - определим её структуру:

- user_id - строковый айди пользователя
- oauth_consumer_key - уникальный токен клиента
- lis_result_sourcedid - ссылка на блок, в котором находится задача в ЛМС
- lis_outcome_service_url - URL адрес в ЛМС, куда мы шлем оценку
- is_correct - была ли попытка верной (null, если это run)
- attempt_type - ран или сабмит
- created_at - дата и время попытки

## Дополнительный функционал:
1. Необходимо добавить в скрипт код, который в конце будет агрегировать данные за день:

- сколько попыток было совершено
- сколько успешных попыток было из всех совершенных
- количество уникальных юзеров

  Затем необходимо загрузить это все в табличку в Google Sheets.

2. Необходимо настроить e-mail оповещения об успешной (или нет) выполненной выгрузке данных.

## Результат:
Код  рабочего скрипта [здесь]([https://github.com/TrofimovIA/Monetization-Analytics/blob/main/rolling%20retention.sql](https://github.com/TrofimovIA/ETL-SF/blob/main/main.py)).
   

  
