from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_df: DataFrame,
) -> DataFrame:
    joined_df = products_df.join(
        product_category_df,
        on="product_id",
        how="left",
    )
    joined_df = joined_df.join(
        categories_df,
        on="category_id",
        how="left",
    )
    result_df = joined_df.select(
        col("product_name"),
        col("category_name"),
    )

    return result_df


if __name__ == "__main__":
    pass
