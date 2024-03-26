from pyspark.sql import SparkSession

def get_product_category_pairs(products_df, categories_df):
  return products_df.join(categories_df, "product_id", "left_outer")\
        .select("product_name", "category_name")


        
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("test") \
        .getOrCreate()

    products_data = [("p1", "Product 1"),
                     ("p2", "Product 2"), 
                     ("p3", "Product 3")]


    categories_data = [("p1", "Category A"),
                       ("p2", "Category B")]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["product_id", "category_name"])


    print("Pairs:")
    get_product_category_pairs(products_df, categories_df).show()
    spark.stop()


"""
Надеюсь я праельно понял этот текст 'Напишите метод на PySpark,
 который в одном датафрейме вернет все пары «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий.'

"""