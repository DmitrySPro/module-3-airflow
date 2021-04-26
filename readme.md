ods_payment

Table
1) expect_table_columns_to_match_ordered_list(column_list=['user_id', 'pay_doc_type', 'pay_doc_num', 'account', 'phone', 'billing_period', 'pay_date', 'sum']) - Проверка на месте ли все колонки
2) expect_table_row_count_to_be_between(max_value=10000, min_value=10000) -Проверка все ли строки подгрузились.

User_id

1) expect_column_values_to_not_be_null(column='user_id') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='user_id', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.

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

1) expect_column_values_to_not_be_null(column='phone') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_proportion_of_unique_values_to_be_between(column='phone', max_value=0.9994, min_value=0.9994) - Тест предложен автогенерацией. Показывает что долю уникальных значений в поле должна быть в заданных пределах. Телефоны могут повторяться, но зная наш датасет, что повторов всего 6 решил оставить тест.
3) expect_column_value_lengths_to_be_between(column='phone', min_value=10, max_value=12) = Проверка длинны строки. Телефон без префикса страны содержит 10 символов с префиксом 12.
4) batch.expect_column_values_to_be_in_type_list(column='phone', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.

Billing_period

1) batch.expect_column_values_to_not_be_null(column='billing_period') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) batch.expect_column_values_to_be_in_type_list(column='billing_period', type_list=['TEXT']) - Проверка типа данных. Поле должно быть типа TEXT.
3) expect_column_values_to_match_regex(column='billing_period', regex='^\d{4}-\d{2}') - Проверка корректности значений поля. В поле содержится год(4 цифры)-месяц(2 цифры). Проверятеся соответствие значений этому паттерну.

Pay_date

1) expect_column_values_to_not_be_null(column='pay_date') -  Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='pay_date', type_list=['DATE']) - Проверка типа данных. Поле должно быть типа DATE.
3) expect_column_values_to_be_between(column='pay_date', min_value= '2013-01-01', parse_strings_as_datetimes=True) - Проверка корректности значений. В нашем датасете нет дат ранее 2013 года. Это и проверяем.

Sum

1) expect_column_values_to_not_be_null(column='sum') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='sum', type_list=['REAL']) - Проверка типа данных. Поле должно быть типа REAL.
3) expect_column_values_to_be_between(column='sum', min_value=0) - Сумма платежа не может быть отрицательной. Проверяем что все значения больше 0.


ods_issue

Table
1) expect_table_columns_to_match_ordered_list(column_list=['user_id', 'start_time', 'end_time', 'title', 'description', 'service']) - Проверка на месте ли все колонки
2) expect_table_row_count_to_be_between(max_value=10000, min_value=10000) -Проверка все ли строки подгрузились.

User_id

1) expect_column_values_to_not_be_null(column='user_id') - Проверка на пустые значения. В поле не должно быть пустых значений.
2) expect_column_values_to_be_in_type_list(column='user_id', type_list=['INTEGER']) - Проверка типа данных. Поле должно быть типа integer.
3) expect_column_values_to_be_between(column='user_id', min_value=0) - Значения поля не должны быть отрицательными. Проверка значений что они больше нуля.








ods_traffic






ods_billing





