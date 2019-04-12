from pyspark.sql.types import *
import pyspark.sql.functions as f
import time

df = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5'),
    ('id_cliente-3',  'cat-6, cat-7'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9'),
    ('id_cliente-9',  'cat-1'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])

df1 = df.select(
        "id_cliente",
        f.split("categorias", ", ").alias("categorias"),
        f.posexplode(f.split("categorias", ", ")).alias("pos", "val"), f.lit("1").alias("cat_num"))

df3 = df1.groupBy(df1.id_cliente).pivot('val').agg(f.first("cat_num"))

df4 = df3.fillna("0")
df4.show()
