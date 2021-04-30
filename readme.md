ods_payment

Table
1) expect_table_columns_to_match_ordered_list(column_list=['user_id', 'pay_doc_type', 'pay_doc_num', 'account', 'phone', 'billing_period', 'pay_date', 'sum']) - Проверка на месте ли все колонки
2) expect_table_row_count_to_be_between(max_value=10000, min_value=10000) -Проверка все ли строки подгрузились.

User_id

1) expect_column_values_to_not_be_null(column='user_id') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='user_id', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.
4) expect_column_proportion_of_unique_values_to_be_between(column='user_id', min_value=0.01) - Пропорция уникальных значений не менее 0,01. Так как значения в столбце могут повторяться используем пропорцию. Проверка от ошибок системы источника от которого могут поступить одинаковые значения.

Pay_doc_type

1) expect_column_values_to_not_be_null(column='pay_doc_type') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='pay_doc_type', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_be_in_set(column='pay_doc_type', value_set=['MASTER', 'MIR', 'VISA']) - Проверка значений. Наименования платёжных систем должны входить в список. Если появится новая будет варнинг. Обсуждали на занятии.

Pay_doc_num

1) expect_column_values_to_not_be_null(column='pay_doc_num') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='pay_doc_num', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.

Account

1) expect_column_values_to_not_be_null(column='account') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_match_regex(column='account', regex='FL-\d+') - Проверка корректности значений поля. Аккаунт представляет из себя набор цифр идущих после префикса "FL-". Проверяется соответствие этому паттерну.
3) expect_column_values_to_be_in_type_list(column='account', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.

Phone

1) expect_column_values_to_not_be_null(column='phone') - Проверка на пустые значения. В поле не должно быть пустых значений. Теоретически поле может быть пустым но в нашем датасете это не так, поэтому предполагаем что пре регистрации телефон обязателен.
2) expect_column_proportion_of_unique_values_to_be_between(column='phone', max_value=0.9994, min_value=0.9994) - Показывает что долю уникальных значений в поле должна быть в заданных пределах. Телефоны могут повторяться, но зная наш датасет, что повторов всего 6 решил оставить тест.
3) expect_column_value_lengths_to_be_between(column='phone', min_value=10, max_value=12) = Проверка длинны строки. Телефон без префикса страны содержит 10 символов с префиксом 12.
4) batch.expect_column_values_to_be_in_type_list(column='phone', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.

Billing_period

1) batch.expect_column_values_to_not_be_null(column='billing_period') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) batch.expect_column_values_to_be_in_type_list(column='billing_period', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_match_regex(column='billing_period', regex='^\d{4}-\d{2}') - Проверка корректности значений поля. В поле содержится год(4 цифры)-месяц(2 цифры). Проверятеся соответствие значений этому паттерну.

Pay_date

1) expect_column_values_to_not_be_null(column='pay_date') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='pay_date', type_list=['DATE']) - Проверка типа данных. Поле должно быть типа DATE.
3) expect_column_values_to_be_between(column='pay_date', min_value= '2013-01-01', parse_strings_as_datetimes=True) - Проверка корректности значений. В нашем датасете нет дат ранее 2013 года.

Sum

1) expect_column_values_to_not_be_null(column='sum') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='sum', type_list=['REAL']) - Проверка типа данных. Поле должно быть типа REAL.
3) expect_column_values_to_be_between(column='sum', min_value=0, max_value = 999999) - Сумма платежа не может быть отрицательной. Проверяем что все значения больше 0, а также что мы не получили черзмерно большие суммы от источника. Максимум у нас в датасете 49 тысяч, устанавливаю планку в 999999 с запасом, учитывая что могут быть юр. лица которые платят сотни тысяч..


ods_issue

Table
1) expect_table_columns_to_match_ordered_list(column_list=['user_id', 'start_time', 'end_time', 'title', 'description', 'service']) - Проверка на месте ли все колонки
2) expect_table_row_count_to_be_between(max_value=10000, min_value=10000) -Проверка все ли строки подгрузились.

User_id

1) expect_column_values_to_not_be_null(column='user_id') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='user_id', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.
4) expect_column_proportion_of_unique_values_to_be_between(column='user_id', min_value=0.01) - Пропорция уникальных значений не менее 0,01. Так как значения в столбце могут повторяться используем пропорцию. Проверка от ошибок системы источника от которого могут поступить одинаковые значения.

start_time

1) expect_column_values_to_not_be_null(column='start_time') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='start_time', type_list=['DATE']) - Проверка типа данных. Поле должно быть типа DATE.
3) expect_column_values_to_be_between(column='start_time', min_value='2013-01-01', parse_strings_as_datetimes=True) -  Проверка корректности значений. В нашем датасете нет дат ранее 2013 года.

end_time
1) expect_column_values_to_not_be_null(column='end_time') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='end_time', type_list=['DATE']) - Проверка типа данных. Поле должно быть типа DATE.
3) expect_column_values_to_be_between(column='end_time', min_value='2013-01-01', parse_strings_as_datetimes=True) -  Проверка корректности значений. В нашем датасете нет дат ранее 2013 года.
4) expect_column_pair_values_A_to_be_greater_than_B(column_A = 'end_time', column_B='start_time', or_equal=True) - Проверка корректности данных. Время завершения события всегда больше или равно времени начала. Почему то не заработало на нашем датасете.

title

1) expect_column_values_to_not_be_null(column='title') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='title', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.

description

1) expect_column_values_to_not_be_null(column='description') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='description', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.

service

1) expect_column_values_to_not_be_null(column='service') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='service', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_be_in_set(column='service', value_set=['Connect', 'Disconnect', 'Setup Environment']) - Проверка корректности значения. Значение должно входить в ограниченный набор сервисов.


ods_traffic

Table

1) expect_table_columns_to_match_ordered_list(column_list=['user_id', 'event_ts', 'device_id', 'device_ip_addr', 'bytes_sent', 'bytes_received']) - Проверка на месте ли все колонки
2) expect_table_row_count_to_be_between(max_value=10000, min_value=1) - Проверка все ли строки подгрузились.

User_id

1) expect_column_values_to_not_be_null(column='user_id') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='user_id', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.
4) expect_column_proportion_of_unique_values_to_be_between(column='user_id', min_value=0.01) - Пропорция уникальных значений не менее 0,01. Так как значения в столбце могут повторяться используем пропорцию. Проверка от ошибок системы источника от которого могут поступить одинаковые значения.

event_ts

1) expect_column_values_to_not_be_null(column='event_ts') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='event_ts', type_list=['TIMESTAMP']) - Проверка типа данных. Поле должно быть типа TIMESTAMP.
3) expect_column_values_to_be_between(column='event_ts', min_value='2013-01-01', parse_strings_as_datetimes=True) -  Проверка корректности значений. В нашем датасете нет дат ранее 2013 года. Ghjdthztv xnj dct lfns ,jktt bkb hfdys 01.01.2013/

device_id
1) expect_column_values_to_not_be_null(column='device_id') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='device_id', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_proportion_of_unique_values_to_be_between(column='device_id', max_value=0.0009, min_value=0.0009) - Сгенерированное поле. Проверка на уникальность данных. Если исходить из того что у каждого устройства свой уникальный id, то если будет встречаться большое кол-во одинаковых записей это может говорить о проблемах в системе источнике.

device_ip_addr

1) expect_column_values_to_not_be_null(column='device_ip_addr') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) batch.expect_column_values_to_be_in_type_list(column='device_ip_addr', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_match_regex(column='device_ip_addr', regex="^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$") - Проверка корректности данных. IP состоит из 4 наборов цифр(от 1 до 3) разделённых точками.

bytes_sent
1) expect_column_values_to_not_be_null(column='bytes_sent') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='bytes_sent', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='bytes_sent', min_value=0, max_value=100000) - Проверка на корректность данных. Количество должно быть больше или равно нулю. Максимум в нашем датасете 49996. Если учесть что это байты то в целом это довольно мало, но незная из какой системы источника данные сложно оценить теоретический максимум, установил максимальную планку в 2 раза выше, если значения будут больше это причина обратить на это внимание.

bytes_recieved

1) expect_column_values_to_not_be_null(column='bytes_recieved') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='bytes_recieved', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='bytes_recieved', min_value=0, max_value=100000) - Проверка на корректность данных. Количество должно быть больше или равно нулю. Максимум в нашем датасете 49999. Если учесть что это байты то в целом это довольно мало, но незная из какой системы источника данные сложно оценить теоретический максимум, установил максимальную планку в 2 раза выше, если значения будут больше это причина обратить на это внимание..

ods_billing

Table

1) expect_table_columns_to_match_ordered_list(column_list=['user_id', 'billing_period', 'service', 'tariff', 'sum', 'created_at']) - Проверка на месте ли все колонки
2) expect_table_row_count_to_be_between(max_value=10000, min_value=1) - Проверка все ли строки подгрузились.

User_id

1) expect_column_values_to_not_be_null(column='user_id') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='user_id', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.
4) expect_column_proportion_of_unique_values_to_be_between(column='user_id', min_value=0.01) - Пропорция уникальных значений не менее 0,01. Так как значения в столбце могут повторяться используем пропорцию. Проверка от ошибок системы источника от которого могут поступить одинаковые значения.

billing_period

1) expect_column_values_to_not_be_null(column='billing_period') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='billing_period', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_match_regex(column='billing_period', regex='^\\d{4}-\\d{2}') - Проверка корректности значений поля. В поле содержится год(4 цифры)-месяц(2 цифры). Проверятеся соответствие значений этому паттерну.
4) batch.expect_column_values_to_match_strftime_format(column='billing_period', strftime_format = "%Y-%d" ) - Проверка на приводимость к типу даты. Почему то не заработала. NotImplementedError.

service

1) expect_column_values_to_not_be_null(column='service') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='service', type_list=['TEXT'])  - Проверка типа данных. Поле должно быть типа TEXT.

tariff

1) expect_column_values_to_not_be_null(column='tariff') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='tariff', type_list=['TEXT']) Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_be_in_set(column='tariff', value_set=['Gigabyte', 'Maxi', 'Megabyte', 'Mini']) - Проверка корректности значений. Значения должны входить в определённый перечень тарифов.

sum

1) expect_column_values_to_not_be_null(column='sum') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='sum', type_list=['REAL']) - Проверка типа данных. Поле должно быть типа REAL.
3) expect_column_values_to_be_between(column='sum', min_value=0, max_value = 999999) - Сумма платежа не может быть отрицательной. Проверяем что все значения больше 0, а также что мы не получили черзмерно большие суммы от источника. Максимум у нас в датасете 49 тысяч, устанавливаю планку в 999999 с запасом, учитывая что могут быть юр. лица которые платят сотни тысяч.


created_at

1) expect_column_values_to_not_be_null(column='created_at') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='created_at', type_list=['DATE']) - Проверка типа данных. Поле должно быть типа DATE.
3) expect_column_values_to_be_between(column='created_at', min_value='2013-01-01', parse_strings_as_datetimes=True)  Проверка корректности значений. В нашем датасете нет дат ранее 2013 года.
