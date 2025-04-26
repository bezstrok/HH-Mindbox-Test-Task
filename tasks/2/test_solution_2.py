import typing as tp

import pytest
from pyspark.sql import Row, SparkSession
from solution_2 import get_product_category_pairs


class TestGetProductCategoryPairs:
    @pytest.fixture(scope="session")
    def spark(self) -> tp.Generator[SparkSession, None, None]:
        spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
        yield spark
        spark.stop()

    def test_product_category_pairs_with_categories(
        self,
        spark: SparkSession,
    ) -> None:
        products = [
            Row(product_id=1, product_name="A"),
            Row(product_id=2, product_name="B"),
        ]
        categories = [
            Row(category_id=10, category_name="X"),
            Row(category_id=20, category_name="Y"),
        ]
        mapping = [
            Row(product_id=1, category_id=10),
            Row(product_id=2, category_id=20),
        ]

        products_df = spark.createDataFrame(
            products,
            schema="product_id INT, product_name STRING",
        )
        categories_df = spark.createDataFrame(
            categories,
            schema="category_id INT, category_name STRING",
        )
        pc_df = spark.createDataFrame(
            mapping,
            schema="product_id INT, category_id INT",
        )

        result = get_product_category_pairs(products_df, categories_df, pc_df)
        collected = {(r.product_name, r.category_name) for r in result.collect()}

        assert collected == {("A", "X"), ("B", "Y")}

    def test_product_category_pairs_without_categories(
        self,
        spark: SparkSession,
    ) -> None:
        products = [
            Row(product_id=1, product_name="A"),
            Row(product_id=2, product_name="B"),
        ]
        categories = []  # type: ignore[var-annotated]
        mapping = [Row(product_id=1, category_id=None)]

        products_df = spark.createDataFrame(
            products,
            schema="product_id INT, product_name STRING",
        )
        categories_df = spark.createDataFrame(
            categories,
            schema="category_id INT, category_name STRING",
        )
        pc_df = spark.createDataFrame(
            mapping,
            schema="product_id INT, category_id INT",
        )

        rows = get_product_category_pairs(products_df, categories_df, pc_df).collect()

        assert any(
            row.product_name == "B" and row.category_name is None for row in rows
        )
        assert any(row.product_name == "A" for row in rows)

    def test_empty_arguments(
        self,
        spark: SparkSession,
    ) -> None:
        products_df = spark.createDataFrame(
            [],
            schema="product_id INT, product_name STRING",
        )
        categories_df = spark.createDataFrame(
            [],
            schema="category_id INT, category_name STRING",
        )
        pc_df = spark.createDataFrame(
            [],
            schema="product_id INT, category_id INT",
        )

        result = get_product_category_pairs(products_df, categories_df, pc_df)
        assert result.count() == 0
