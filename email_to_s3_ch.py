from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

import os 

# Инициализация SparkSession с ClickHouse JDBC драйвером
spark = SparkSession.builder \
    .appName("pyspark-clickhouse") \
    .config("spark.jars", "./clickhouse-jdbc-0.5.0-shaded.jar") \
    .getOrCreate()
  
spark.sparkContext.setLogLevel('ERROR')


input_path = os.environ.get('S3_INPUT_PATH')
output_path_correct = os.environ.get('S3_OUTPUT_CORRECT_PATH')
output_path_incorrect = os.environ.get('S3_OUTPUT_INCORRECT_PATH')
clickhouse_table = os.environ.get('CH_OUTPUT_CORRECT_PATH')

# Загрузка данных из S3

schema = StructType([
    StructField("row", StringType(), True),
    StructField("email", StringType(), True),
    # Добавьте здесь другие поля в соответствии с вашими данными
])

df = spark.read.schema(schema).csv(input_path)

print("Сэмпл данных из S3 с правильными и неправильными email")
df.show(10)

# Регулярное выражение для проверки email
email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

# Фильтрация правильных и неправильных email-адресов
print("Сэмпл данных из S3 с правильными email")
correct_emails = df.filter(col("email").rlike(email_regex))
correct_emails.show(10)

print("Сэмпл данных из S3 с неправильными email")
incorrect_emails = df.filter(~col("email").rlike(email_regex))
incorrect_emails.show(10)

# Подсчет и вывод количества правильных и неправильных email-адресов
count_correct = correct_emails.count()
count_incorrect = incorrect_emails.count()

print(f"Количество правильных email-адресов: {count_correct}")
print(f"Количество неправильных email-адресов: {count_incorrect}")


# Параметры подключения к ClickHouse
#REPLACE WITH YOUR DB URL "jdbc:clickhouse://213.219.214.98:8123/s3_export_db"
url = "jdbc:clickhouse://213.219.214.98:8123/s3_export_db"
properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "REPLACE_WITH_YOUR_CH_USER",
    "password": "REPLACE_WITH_YOUR_CH_PASSWORD"
}

print("Записываем данные с правильными email в ClickHouse")

# Запись DataFrame в ClickHouse
correct_emails.write.format("jdbc") \
.mode("overwrite") \
.option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
.option("url", url) \
.option("dbtable", clickhouse_table) \
.option("createTableOptions", "ENGINE = MergeTree() ORDER BY email") \
.option("user","user") \
.option("password","o1F2KhM670J-q41d9") \
.save()

print("Запись в ClickHouse прошла успешно")

print("Читаем таблицу с правильными email из ClickHouse ")
# Чтение данных из таблицы ClickHouse
df = spark.read.jdbc(url=url, table=clickhouse_table, properties=properties)

print("Сэмпл данных из ClickHouse")
df.show()


print("Сохраняем данные в S3")
# Сохранение результатов обратно в S3
correct_emails.write.mode("overwrite").csv(output_path_correct)
incorrect_emails.write.mode("overwrite").csv(output_path_incorrect)
print("Данные успешно сохранены в S3")

print("Завершение работы")
# Завершение сессии Spark
spark.stop()
