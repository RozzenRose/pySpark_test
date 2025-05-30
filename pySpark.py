from pyspark.sql import DataFrame


def get_product_category_info_single_df(products: DataFrame, categories: DataFrame) -> DataFrame:
    """
    Возвращает:
    DataFrame со следующей структурой: ('product_name', 'category_name')
    Если category_name нет, там будет лежать None

    Точно не знаю, структуру датафрймов, исхожу из того, что они примерно такие:
    products('product_id', 'product_name, category_id')
    categories('category_id', 'category_name')
    """
    joined_dataframe = products.join(categories, on='category_id', how='left') \
        .select('product_name', 'category_name')

    return joined_dataframe