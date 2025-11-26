from main import reverse_string, is_palindrome, find_max, group_words_by_length, two_sum, group_anagrams, etl_sales_data
import pandas as pd


def test_reverse_string():
    assert reverse_string("hello") == "olleh"
    assert reverse_string("a") == "a"
    assert reverse_string("") == ""


def test_is_palindrome():
    assert is_palindrome("Madam") is True
    assert is_palindrome("test") is False
    assert is_palindrome("Racecar") is True
    assert is_palindrome("Python") is False


def test_find_max():
    assert find_max([3, 1, 5]) == 5
    assert find_max([1, 3, 7, 2]) == 7
    assert find_max([-5, -2, -10]) == -2
    assert find_max([]) is None


def test_group_words_by_length():
    assert group_words_by_length(["a", "to", "hi", "sun"]) == {
        1: ["a"],
        2: ["to", "hi"],
        3: ["sun"]
    }
    assert group_words_by_length(["hi", "hello", "to", "world", "a"]) == {
        2: ["hi", "to"],
        5: ["hello", "world"],
        1: ["a"]
    }


def test_two_sum():
    assert two_sum([2, 7, 11, 15], 9) == [0, 1]
    assert two_sum([3, 2, 4], 6) == [1, 2]
    assert two_sum([1, 5, 3, 2], 8) == [1, 2]


def test_group_anagrams_1():
    input_words = ["bat", "tab", "cat", "tac", "act"]
    result = group_anagrams(input_words)

    # Convert each group to a frozenset (ignore order inside group)
    result_sets = set(frozenset(group) for group in result)

    # Expected groups also as frozensets
    expected = set(
        [frozenset(["bat", "tab"]),
         frozenset(["cat", "tac", "act"])])

    assert result_sets == expected


def test_group_anagrams_2():
    input_words = ["bat", "tab", "cat", "tac", "act", "dog"]
    result = group_anagrams(input_words)

    # Convert each group to a frozenset (ignore order inside group)
    result_sets = set(frozenset(group) for group in result)

    # Expected groups also as frozensets
    expected = set([
        frozenset(["bat", "tab"]),
        frozenset(["cat", "tac", "act"]),
        frozenset(["dog"])
    ])

    assert result_sets == expected


def test_etl_sales_data_basic():
    """Test basic ETL functionality with simple dataset"""
    sales = [
        {"sale_id": 1, "product_id": 101, "region_id": 1, "quantity": 5, "sale_date": "2024-01-15", "amount": 150.0},
        {"sale_id": 2, "product_id": 102, "region_id": 1, "quantity": 3, "sale_date": "2024-01-16", "amount": 90.0},
        {"sale_id": 3, "product_id": 101, "region_id": 2, "quantity": 2, "sale_date": "2024-01-17", "amount": 60.0}
    ]

    products = [
        {"product_id": 101, "product_name": "Widget A", "category": "Electronics", "unit_price": 30.0},
        {"product_id": 102, "product_name": "Widget B", "category": "Home", "unit_price": 30.0}
    ]

    regions = [
        {"region_id": 1, "region_name": "North", "country": "USA"},
        {"region_id": 2, "region_name": "South", "country": "USA"}
    ]

    result = etl_sales_data(sales, products, regions)

    # Verify it's a DataFrame
    assert isinstance(result, pd.DataFrame)

    # Verify columns
    expected_columns = ["category", "country", "total_sales", "total_quantity", "avg_sale_amount"]
    assert list(result.columns) == expected_columns

    # Verify row count
    assert len(result) == 2

    # Verify first row (highest total_sales)
    assert result.iloc[0]["category"] == "Electronics"
    assert result.iloc[0]["country"] == "USA"
    assert result.iloc[0]["total_sales"] == 210.0
    assert result.iloc[0]["total_quantity"] == 7
    assert result.iloc[0]["avg_sale_amount"] == 105.0

    # Verify second row
    assert result.iloc[1]["category"] == "Home"
    assert result.iloc[1]["country"] == "USA"
    assert result.iloc[1]["total_sales"] == 90.0
    assert result.iloc[1]["total_quantity"] == 3
    assert result.iloc[1]["avg_sale_amount"] == 90.0


def test_etl_sales_data_multiple_countries():
    """Test ETL with multiple countries and categories"""
    sales = [
        {"sale_id": 1, "product_id": 101, "region_id": 1, "quantity": 10, "sale_date": "2024-01-15", "amount": 500.0},
        {"sale_id": 2, "product_id": 102, "region_id": 2, "quantity": 5, "sale_date": "2024-01-16", "amount": 250.0},
        {"sale_id": 3, "product_id": 103, "region_id": 1, "quantity": 8, "sale_date": "2024-01-17", "amount": 400.0},
        {"sale_id": 4, "product_id": 101, "region_id": 3, "quantity": 15, "sale_date": "2024-01-18", "amount": 750.0},
        {"sale_id": 5, "product_id": 102, "region_id": 1, "quantity": 3, "sale_date": "2024-01-19", "amount": 150.0}
    ]

    products = [
        {"product_id": 101, "product_name": "Laptop", "category": "Electronics", "unit_price": 50.0},
        {"product_id": 102, "product_name": "Chair", "category": "Furniture", "unit_price": 50.0},
        {"product_id": 103, "product_name": "Desk", "category": "Furniture", "unit_price": 50.0}
    ]

    regions = [
        {"region_id": 1, "region_name": "West", "country": "USA"},
        {"region_id": 2, "region_name": "East", "country": "Canada"},
        {"region_id": 3, "region_name": "Central", "country": "Mexico"}
    ]

    result = etl_sales_data(sales, products, regions)

    # Verify DataFrame type
    assert isinstance(result, pd.DataFrame)

    # Verify it's sorted by total_sales descending
    assert result["total_sales"].tolist() == sorted(result["total_sales"].tolist(), reverse=True)

    # Verify specific aggregations
    furniture_usa = result[(result["category"] == "Furniture") & (result["country"] == "USA")]
    assert len(furniture_usa) == 1
    assert furniture_usa.iloc[0]["total_sales"] == 550.0  # 400 + 150
    assert furniture_usa.iloc[0]["total_quantity"] == 11  # 8 + 3
    assert furniture_usa.iloc[0]["avg_sale_amount"] == 275.0  # (400 + 150) / 2

    electronics_mexico = result[(result["category"] == "Electronics") & (result["country"] == "Mexico")]
    assert len(electronics_mexico) == 1
    assert electronics_mexico.iloc[0]["total_sales"] == 750.0
    assert electronics_mexico.iloc[0]["total_quantity"] == 15


def test_etl_sales_data_with_missing_values():
    """Test ETL handles missing product or region references"""
    sales = [
        {"sale_id": 1, "product_id": 101, "region_id": 1, "quantity": 5, "sale_date": "2024-01-15", "amount": 150.0},
        {"sale_id": 2, "product_id": 999, "region_id": 1, "quantity": 3, "sale_date": "2024-01-16", "amount": 90.0},  # Missing product
        {"sale_id": 3, "product_id": 101, "region_id": 999, "quantity": 2, "sale_date": "2024-01-17", "amount": 60.0}  # Missing region
    ]

    products = [
        {"product_id": 101, "product_name": "Widget A", "category": "Electronics", "unit_price": 30.0}
    ]

    regions = [
        {"region_id": 1, "region_name": "North", "country": "USA"}
    ]

    result = etl_sales_data(sales, products, regions)

    # Should only include the valid sale
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["total_sales"] == 150.0
    assert result.iloc[0]["total_quantity"] == 5


def test_etl_sales_data_rounding():
    """Test that avg_sale_amount is properly rounded to 2 decimal places"""
    sales = [
        {"sale_id": 1, "product_id": 101, "region_id": 1, "quantity": 1, "sale_date": "2024-01-15", "amount": 100.0},
        {"sale_id": 2, "product_id": 101, "region_id": 1, "quantity": 1, "sale_date": "2024-01-16", "amount": 150.0},
        {"sale_id": 3, "product_id": 101, "region_id": 1, "quantity": 1, "sale_date": "2024-01-17", "amount": 133.33}
    ]

    products = [
        {"product_id": 101, "product_name": "Widget", "category": "Electronics", "unit_price": 30.0}
    ]

    regions = [
        {"region_id": 1, "region_name": "North", "country": "USA"}
    ]

    result = etl_sales_data(sales, products, regions)

    # Average should be (100 + 150 + 133.33) / 3 = 127.776666... rounded to 127.78
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["avg_sale_amount"] == 127.78
