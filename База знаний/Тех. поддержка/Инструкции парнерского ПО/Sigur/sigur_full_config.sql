
USE `TC-DB-MAIN`;

-- URL сервиса по приему фактов прохода. Пустая строка, если факты прохода не требуется передавать на веб-сервис:
UPDATE PARAMB set PARAMVALUE='http://test.sigur.com/logs.php' where NAME='SS_LOG_URL';

-- URL сервиса по синхронизации сотрудников:
UPDATE PARAMB set PARAMVALUE='http://test.sigur.com/emps.php' where NAME='SS_EMP_URL';

-- URL сервиса по синхронизации фотографий. Пустая строка, если фотографии не синхронизируются по веб-сервису:
UPDATE PARAMB set PARAMVALUE='http://test.sigur.com/photo.php' where NAME='SS_PHOTO_URL';

-- URL сервиса по синхронизации платежных меню. Пустая строка, если синхронизация меню не используется:
UPDATE PARAMB set PARAMVALUE='http://test.sigur.com/menu.php' where NAME='SS_PAYMENU_URL';

-- URL сервиса по приему фактов изменения платежного баланса. Пустая строка, если факты изменения платежных балансов не требуется передавать на веб-сервис:
UPDATE PARAMB set PARAMVALUE='http://test.sigur.com/paylog.php' where NAME='SS_PAYLOG_URL';

-- URL сервиса по приему кадров с IP камер. Пустая строка, если кадры не требуется передавать на веб-сервис.
UPDATE PARAMB set PARAMVALUE='http://test.sigur.com/frames.php' where NAME='SS_FRAME_URL';

-- Текст SMS сообщения о проходе, который устанавливается создаваемым сотрудникам по-умолчанию:
UPDATE PARAMB set PARAMVALUE='test' where NAME='SS_EMP_DEFSMSTEXT';
 
-- Токен авторизации для доступа с сервису. Можно оставить пустым или включить туда логин+пароль:
UPDATE PARAMB set PARAMVALUE='user:password' where NAME='SS_LOGIN';


-- Признак включения функции:
UPDATE PARAMI set PARAMVALUE=1 where NAME='SS_ENABLED';

-- Период синхронизации, в секундах:
UPDATE PARAMI set PARAMVALUE=900 where NAME='SS_EMP_PERIOD'; 

-- Передавать ли на сервис события совершенных проходов:
UPDATE PARAMI set PARAMVALUE=1 where NAME='SS_LOG_USE_PASS';

-- Передавать ли на сервис события запрещенных проходов. 
UPDATE PARAMI set PARAMVALUE=1 where NAME='SS_LOG_USE_DENY';

-- Передавать ли на сервис события, не связанные ни с каким из сотрудников, ранее полученных от сервиса. 
UPDATE PARAMI set PARAMVALUE=1 where NAME='SS_LOG_USE_NOID';


