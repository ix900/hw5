# hw5
Home work 5

**ods_billing** (user_id int, billing_period int, service text, tariff text, sum decimal(20,2), created_at timestamp)
**Table Expectation(s)**
проверка таблицы на соответствие структуре (наличию полей). Проверка необходима для понимания, что в структуре присутствуют
данные поля и работа скриптов в следующих слоях не будет нарушена
batch.expect_table_columns_to_match_set(column_set=['user_id', 'billing_period', 'service', 'tariff', 'sum', 'created_at'])

Контроль кол-ва полей в таблице - необходим для сигнализации если  структура таблицы изменилась 
batch.expect_table_column_count_to_equal(value=6)

проверка на отсутсвие записей в таблице, нужен для информирования что данная таблица не содержит записей
batch.expect_table_row_count_to_be_between(0,20000)

**Column Expectation(s)**
user_id
идентификатор пользователя (клиента) не может быть пустым
batch.expect_column_values_to_not_be_null(column='user_id')

проверка на тип
batch.expect_column_values_to_be_of_type(column='user_id', type_='INTEGER')

billing_period
идентификатор периода, в data_lake в ods был приведен к числовому формату
данная проверка позволит контролировать корректность значения в данном поле
batch.expect_column_values_to_be_in_set(column='billing_period',value_set = [x for x in range(201301, int(datetime.datetime.now().strftime("%Y%m"))+1) if x%100 in [d for d in range(1,13)]])

tariff
контроль за значениями в поле tariff, а именно при появлении нового или при отсутствии одного из них сработает warning  
batch.expect_column_distinct_values_to_be_in_set(column='tariff', value_set=['Gigabyte', 'Maxi', 'Megabyte', 'Mini'])

контроль за распределением записей по столбцу tariff 
batch.expect_column_kl_divergence_to_be_less_than(column='tariff', partition_object={'values': ['Gigabyte', 'Maxi', 'Megabyte', 'Mini'], 'weights': [0.242, 0.241, 0.272, 0.245]}, threshold=0.6)

sum
проверка по допустимые значения в интервале от 0 до 1000 ( почему 1000 ? взял как заведомо больший порог чем максимальное значение в данном поле )
batch.expect_column_values_to_be_between(column='sum', max_value=1000, min_value=0)
проверка на числовой тип
batch.expect_column_values_to_be_of_type(column='sum', type_='NUMERIC')

created_at
проверка на корректное содержание даты/времени
batch.expect_column_values_to_be_of_type(column='created_at', type_='TIMESTAMP') 

**ods_issue** (user_id::int, start_time::timestamp, end_time::timestamp, title::text, descriptions::text, service::text)
**Table Expectation(s)**
проверка таблицы на соответствие структуре (наличию полей). Проверка необходима для понимания, что в структуре присутствуют
данные поля и работа скриптов в следующих слоях не будет нарушена
batch.expect_table_columns_to_match_set(column_set=['user_id', 'start_time', 'end_time', 'title', 'description', 'service'])

Контроль кол-ва полей в таблице - необходим для сигнализации если  структура таблицы изменилась 
batch.expect_table_column_count_to_equal(value=6)

проверка на отсутствие записей в таблице. Проверка нужена для контроля, что в данной таблице есть записей
batch.expect_table_row_count_to_be_between(0,10000)

**Column Expectation(s)**
user_id
идентификатор пользователя (клиента) не может быть пустым
batch.expect_column_values_to_not_be_null(column='user_id')
 
проверка на тип
batch.expect_column_values_to_be_of_type(column='user_id', type_='INTEGER')

start_time, end_time
проверка на тип
batch.expect_column_values_to_be_of_type(column='start_time', type_='TIMESTAMP')
batch.expect_column_values_to_be_of_type(column='end_time', type_='TIMESTAMP')

даты начала и завершения задачи(обращения) не могут быть пустыми
batch.expect_column_values_to_not_be_null(column='start_time')
batch.expect_column_values_to_not_be_null(column='end_time')

title
заголовок не может быть пустым
batch.expect_column_values_to_not_be_null(column='title')
 
service
в этом поле должны быть только значения Connect, Disconnect, Setup Environment
batch.expect_column_distinct_values_to_be_in_set(column='service', value_set=['Connect','Disconnect','Setup Environment'])

поле Service не может быть пустым, т.к. обращение принимается по услуге
batch.expect_column_values_to_not_be_null(column='service')

batch.expect_column_kl_divergence_to_be_less_than(column='service', partition_object={'values': ['Connect', 'Disconnect', 'Setup Environment'], 'weights': [0.34, 0.31, 0.35]}, threshold=0.6)

**ods_payment** (user_id::int, pay_doc_type::text, pay_doc_num::int, account::text, phone::text, billing_period::int, pay_date::timestamp, sum::numeric)
**Table Expectation(s)**
batch.expect_table_columns_to_match_ordered_list(column_list=['user_id', 'pay_doc_type', 'pay_doc_num', 'account', 'phone', 'billing_period', 'pay_date', 'sum'])

Контроль кол-ва полей в таблице - необходим для сигнализации если  структура таблицы изменилась 
batch.expect_table_column_count_to_equal(value=8)

проверка на отсутствие записей в таблице. Проверка нужена для контроля, что в данной таблице есть записей
batch.expect_table_row_count_to_be_between(0,10000)

**Column Expectation(s)** 
user_id
идентификатор пользователя (клиента) не может быть пустым
batch.expect_column_values_to_not_be_null(column='user_id')
проверка на тип
batch.expect_column_values_to_be_of_type(column='user_id', type_='INTEGER')

pay_doc_type
идентификатор платежного документа не может быть пустым
batch.expect_column_values_to_not_be_null(column='pay_doc_type')
контроль содержимого в данном поле  
batch.expect_column_distinct_values_to_be_in_set(column='pay_doc_type', value_set=['MASTER', 'VISA', 'MIR'])

pay_doc_num
номер платежного документа не может быть пустым
batch.expect_column_values_to_not_be_null(column='pay_doc_num')
проверка на тип
batch.expect_column_values_to_be_of_type(column='pay_doc_num', type_='INTEGER')

account
номер лицевого счета не может быть пустым
batch.expect_column_values_to_not_be_null(column='account')
проверка на соответствию формата записи лицевого счета
batch.expect_column_values_to_match_regex(column='account', regex = r'^\D{2,}-\d{1,}$')

phone
проверка соответствия номера телефона формату записи
batch.expect_column_values_to_match_regex(column='phone', regex = r'^\+\d{11,11}$')

billing_period
контроль корректности значений в данном поле
batch.expect_column_values_to_be_in_set(column='billing_period',value_set = [x for x in range(201301, int(datetime.datetime.now().strftime("%Y%m"))+1) if x%100 in [d for d in range(1,13)]])
Идентификатор периода не может быть пустой
batch.expect_column_values_to_not_be_null(column='pay_date')

pay_date
Дата платежа не может быть пустой
batch.expect_column_values_to_not_be_null(column='pay_date')
проверка на тип
batch.expect_column_values_to_be_of_type(column='pay_date', type_='TIMESTAMP') 

sum
Сумма платежа не может быть пустой
batch.expect_column_values_to_not_be_null(column='sum')
проверка по допустимые значения в интервале от 0 до 100000 ( почему 100000 ? взял как заведомо больший порог чем максимальное значение в данном поле )
batch.expect_column_values_to_be_between(column='sum', max_value=100000, min_value=0)
проверка на числовой тип
batch.expect_column_values_to_be_of_type(column='sum', type_='NUMERIC')



**ods_traffic** (user_id::int, event_ts::timestamp, device_id:: text, device_ip_addr text, bytes_sent INT, bytes_received INT)
**Table Expectation(s)**
batch.expect_table_columns_to_match_ordered_list(column_list=['user_id', 'event_ts', 'device_id', 'device_ip_addr', 'bytes_sent', 'bytes_received'])

Контроль кол-ва полей в таблице - необходим для сигнализации если  структура таблицы изменилась 
batch.expect_table_column_count_to_equal(value=6)

проверка на отсутствие записей в таблице. Проверка нужена для контроля, что в данной таблице есть записей
batch.expect_table_row_count_to_be_between(0,10000)

**Column Expectation(s)** 
user_id
идентификатор пользователя (клиента) не может быть пустым
batch.expect_column_values_to_not_be_null(column='user_id')
проверка на тип
batch.expect_column_values_to_be_of_type(column='user_id', type_='INTEGER')

event_ts
Дата соьбытия не может быть пустой
batch.expect_column_values_to_not_be_null(column='event_ts')
проверка на тип
batch.expect_column_values_to_be_of_type(column='event_ts', type_='TIMESTAMP')

device_id
идентификатор устройства не может быть пустым
batch.expect_column_values_to_not_be_null(column='device_id')
проверка соответствия идентификатора устройства формату записи
batch.expect_column_values_to_match_regex(column='device_id', regex = r'^\d\d{3,}$')

device_ip_addr
ip адрес не может быть пустым
batch.expect_column_values_to_not_be_null(column='device_ip_addr')
проверка соответствия значения ip адресу
batch.expect_column_values_to_match_regex(column='device_ip_addr', regex = r'^((?:1?\d{0,2}|2[0-4]\d{1}|25[0-5])\.){3}(?:1?\d{0,2}|2[0-4]\d{1}|25[0-5])$')
 
bytes_sent, bytes_received
не могут быть пустыми
batch.expect_column_values_to_not_be_null(column='bytes_sent')
batch.expect_column_values_to_not_be_null(column='bytes_received')

проверка на тип
batch.expect_column_values_to_be_of_type(column='bytes_sent', type_='INTEGER')
batch.expect_column_values_to_be_of_type(column='bytes_received', type_='INTEGER')

