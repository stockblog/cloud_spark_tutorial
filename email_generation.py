from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import random
import string

# Инициализация SparkSession
spark = SparkSession.builder.appName("emailDataset").getOrCreate()

# Функция для генерации случайной строки
def random_string(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# Функция для генерации email-адресов
def generate_email():
    user_name = random_string(10)
    domain = random_string(5) + ".com"
    email_type = random.randint(0, 4)

    if email_type == 0:
        return f"{user_name}@{domain}"
    elif email_type == 1:
        return f"{user_name}{domain}"
    elif email_type == 2:
        return f"{user_name}@{domain[:-4]}"
    elif email_type == 3:
        return f"{user_name} @{domain}"
    else:
        return f"{user_name}!@{domain}"

# Регистрация UDF
email_udf = udf(generate_email, StringType())

# Создание DataFrame с 10000 строками
df = spark.range(10000).withColumn("email", email_udf())

# Показать пример данных
df.show(10, truncate=False)

# Сохранение DataFrame в S3 bucket
df.coalesce(1).write.mode("overwrite").csv(f"s3a://REPLACE_WITH_YOUR_BUCKET/datasets/email")

# Завершение сессии Spark
spark.stop()
