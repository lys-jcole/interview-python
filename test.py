from main import reverse_string, is_palindrome, find_max, group_words_by_length, two_sum, group_anagrams, etl_sales_data, etl_network_performance
import pandas as pd


def test_reverse_string() -> None:
    assert reverse_string("hello") == "olleh"
    assert reverse_string("a") == "a"
    assert reverse_string("") == ""


def test_is_palindrome() -> None:
    assert is_palindrome("Madam") is True
    assert is_palindrome("test") is False
    assert is_palindrome("Racecar") is True
    assert is_palindrome("Python") is False


def test_find_max() -> None:
    assert find_max([3, 1, 5]) == 5
    assert find_max([1, 3, 7, 2]) == 7
    assert find_max([-5, -2, -10]) == -2
    assert find_max([]) is None


def test_group_words_by_length() -> None:
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


def test_two_sum() -> None:
    assert two_sum([2, 7, 11, 15], 9) == [0, 1]
    assert two_sum([3, 2, 4], 6) == [1, 2]
    assert two_sum([1, 5, 3, 2], 8) == [1, 2]


def test_group_anagrams_1() -> None:
    input_words = ["bat", "tab", "cat", "tac", "act"]
    result = group_anagrams(input_words)

    # Convert each group to a frozenset (ignore order inside group)
    result_sets = set(frozenset(group) for group in result)

    # Expected groups also as frozensets
    expected = set(
        [frozenset(["bat", "tab"]),
         frozenset(["cat", "tac", "act"])])

    assert result_sets == expected


def test_group_anagrams_2() -> None:
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


def test_etl_sales_data_basic() -> None:
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


def test_etl_sales_data_multiple_countries() -> None:
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


def test_etl_sales_data_with_missing_values() -> None:
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


def test_etl_sales_data_rounding() -> None:
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


def test_etl_network_performance_basic() -> None:
    """Test basic network performance ETL functionality"""
    site_metrics = [
        {"site_id": "SITE-001", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 15.5, "packet_loss_pct": 0.1, "throughput_mbps": 95.2, "active_connections": 120},
        {"site_id": "SITE-001", "timestamp": "2024-01-15 11:00:00",
         "latency_ms": 18.2, "packet_loss_pct": 0.2, "throughput_mbps": 92.8, "active_connections": 135},
        {"site_id": "SITE-002", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 45.0, "packet_loss_pct": 1.5, "throughput_mbps": 78.5, "active_connections": 89}
    ]

    site_info = [
        {"site_id": "SITE-001", "site_type": "Macro", "region": "North", "installation_date": "2023-05-10"},
        {"site_id": "SITE-002", "site_type": "Micro", "region": "South", "installation_date": "2022-11-20"}
    ]

    result = etl_network_performance(site_metrics, site_info)

    # Verify DataFrame structure
    assert isinstance(result, pd.DataFrame)
    expected_columns = ["site_id", "site_type", "region", "avg_latency_ms",
                       "avg_packet_loss_pct", "avg_throughput_mbps", "total_connections",
                       "measurement_count", "performance_grade"]
    assert list(result.columns) == expected_columns

    # Verify row count
    assert len(result) == 2

    # Verify first row (best latency - SITE-001)
    assert result.iloc[0]["site_id"] == "SITE-001"
    assert result.iloc[0]["site_type"] == "Macro"
    assert result.iloc[0]["region"] == "North"
    assert result.iloc[0]["avg_latency_ms"] == 16.85  # (15.5 + 18.2) / 2
    assert result.iloc[0]["avg_packet_loss_pct"] == 0.15  # (0.1 + 0.2) / 2
    assert result.iloc[0]["avg_throughput_mbps"] == 94.0  # (95.2 + 92.8) / 2
    assert result.iloc[0]["total_connections"] == 255  # 120 + 135
    assert result.iloc[0]["measurement_count"] == 2
    assert result.iloc[0]["performance_grade"] == "Excellent"

    # Verify second row (SITE-002)
    assert result.iloc[1]["site_id"] == "SITE-002"
    assert result.iloc[1]["avg_latency_ms"] == 45.0
    assert result.iloc[1]["performance_grade"] == "Good"


def test_etl_network_performance_grades() -> None:
    """Test performance grade categorization"""
    site_metrics = [
        {"site_id": "SITE-010", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 10.0, "packet_loss_pct": 0.0, "throughput_mbps": 100.0, "active_connections": 50},
        {"site_id": "SITE-020", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 35.0, "packet_loss_pct": 0.5, "throughput_mbps": 85.0, "active_connections": 40},
        {"site_id": "SITE-030", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 75.0, "packet_loss_pct": 2.0, "throughput_mbps": 60.0, "active_connections": 30},
        {"site_id": "SITE-040", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 150.0, "packet_loss_pct": 5.0, "throughput_mbps": 40.0, "active_connections": 20}
    ]

    site_info = [
        {"site_id": "SITE-010", "site_type": "Macro", "region": "East", "installation_date": "2023-01-01"},
        {"site_id": "SITE-020", "site_type": "Micro", "region": "West", "installation_date": "2023-02-01"},
        {"site_id": "SITE-030", "site_type": "Pico", "region": "North", "installation_date": "2023-03-01"},
        {"site_id": "SITE-040", "site_type": "DAS", "region": "South", "installation_date": "2023-04-01"}
    ]

    result = etl_network_performance(site_metrics, site_info)

    # Verify all grades are assigned correctly
    assert result.iloc[0]["performance_grade"] == "Excellent"  # 10ms
    assert result.iloc[1]["performance_grade"] == "Good"       # 35ms
    assert result.iloc[2]["performance_grade"] == "Fair"       # 75ms
    assert result.iloc[3]["performance_grade"] == "Poor"       # 150ms

    # Verify sorting by latency ascending
    latencies = result["avg_latency_ms"].tolist()
    assert latencies == sorted(latencies)


def test_etl_network_performance_aggregation() -> None:
    """Test aggregation with multiple measurements per site"""
    site_metrics = [
        {"site_id": "SITE-100", "timestamp": "2024-01-15 08:00:00",
         "latency_ms": 20.0, "packet_loss_pct": 0.5, "throughput_mbps": 90.0, "active_connections": 100},
        {"site_id": "SITE-100", "timestamp": "2024-01-15 09:00:00",
         "latency_ms": 25.0, "packet_loss_pct": 0.7, "throughput_mbps": 88.0, "active_connections": 110},
        {"site_id": "SITE-100", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 30.0, "packet_loss_pct": 0.8, "throughput_mbps": 85.0, "active_connections": 120},
        {"site_id": "SITE-100", "timestamp": "2024-01-15 11:00:00",
         "latency_ms": 25.0, "packet_loss_pct": 0.6, "throughput_mbps": 87.0, "active_connections": 105}
    ]

    site_info = [
        {"site_id": "SITE-100", "site_type": "Macro", "region": "Central", "installation_date": "2023-06-15"}
    ]

    result = etl_network_performance(site_metrics, site_info)

    assert len(result) == 1
    assert result.iloc[0]["avg_latency_ms"] == 25.0  # (20 + 25 + 30 + 25) / 4
    assert result.iloc[0]["avg_packet_loss_pct"] == 0.65  # (0.5 + 0.7 + 0.8 + 0.6) / 4
    assert result.iloc[0]["avg_throughput_mbps"] == 87.5  # (90 + 88 + 85 + 87) / 4
    assert result.iloc[0]["total_connections"] == 435  # 100 + 110 + 120 + 105
    assert result.iloc[0]["measurement_count"] == 4
    assert result.iloc[0]["performance_grade"] == "Good"


def test_etl_network_performance_missing_site() -> None:
    """Test handling of metrics for sites not in site_info"""
    site_metrics = [
        {"site_id": "SITE-050", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 15.0, "packet_loss_pct": 0.1, "throughput_mbps": 95.0, "active_connections": 120},
        {"site_id": "SITE-999", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 10.0, "packet_loss_pct": 0.0, "throughput_mbps": 100.0, "active_connections": 150},
        {"site_id": "SITE-060", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 50.0, "packet_loss_pct": 1.0, "throughput_mbps": 80.0, "active_connections": 90}
    ]

    site_info = [
        {"site_id": "SITE-050", "site_type": "Macro", "region": "North", "installation_date": "2023-05-10"},
        {"site_id": "SITE-060", "site_type": "Pico", "region": "South", "installation_date": "2022-11-20"}
    ]

    result = etl_network_performance(site_metrics, site_info)

    # Should only include the two valid sites
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert "SITE-999" not in result["site_id"].values


def test_etl_network_performance_boundary_grades() -> None:
    """Test boundary conditions for performance grades"""
    site_metrics = [
        {"site_id": "SITE-201", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 19.99, "packet_loss_pct": 0.1, "throughput_mbps": 95.0, "active_connections": 50},
        {"site_id": "SITE-202", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 20.0, "packet_loss_pct": 0.2, "throughput_mbps": 90.0, "active_connections": 50},
        {"site_id": "SITE-203", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 49.99, "packet_loss_pct": 0.5, "throughput_mbps": 85.0, "active_connections": 50},
        {"site_id": "SITE-204", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 50.0, "packet_loss_pct": 1.0, "throughput_mbps": 80.0, "active_connections": 50},
        {"site_id": "SITE-205", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 99.99, "packet_loss_pct": 2.0, "throughput_mbps": 70.0, "active_connections": 50},
        {"site_id": "SITE-206", "timestamp": "2024-01-15 10:00:00",
         "latency_ms": 100.0, "packet_loss_pct": 3.0, "throughput_mbps": 60.0, "active_connections": 50}
    ]

    site_info = [
        {"site_id": "SITE-201", "site_type": "Macro", "region": "A", "installation_date": "2023-01-01"},
        {"site_id": "SITE-202", "site_type": "Micro", "region": "B", "installation_date": "2023-01-01"},
        {"site_id": "SITE-203", "site_type": "Pico", "region": "C", "installation_date": "2023-01-01"},
        {"site_id": "SITE-204", "site_type": "DAS", "region": "D", "installation_date": "2023-01-01"},
        {"site_id": "SITE-205", "site_type": "Macro", "region": "E", "installation_date": "2023-01-01"},
        {"site_id": "SITE-206", "site_type": "Micro", "region": "F", "installation_date": "2023-01-01"}
    ]

    result = etl_network_performance(site_metrics, site_info)

    # Verify boundary conditions
    site_1 = result[result["site_id"] == "SITE-201"].iloc[0]
    site_2 = result[result["site_id"] == "SITE-202"].iloc[0]
    site_3 = result[result["site_id"] == "SITE-203"].iloc[0]
    site_4 = result[result["site_id"] == "SITE-204"].iloc[0]
    site_5 = result[result["site_id"] == "SITE-205"].iloc[0]
    site_6 = result[result["site_id"] == "SITE-206"].iloc[0]

    assert site_1["performance_grade"] == "Excellent"  # 19.99 < 20
    assert site_2["performance_grade"] == "Good"       # 20.0 >= 20
    assert site_3["performance_grade"] == "Good"       # 49.99 < 50
    assert site_4["performance_grade"] == "Fair"       # 50.0 >= 50
    assert site_5["performance_grade"] == "Fair"       # 99.99 < 100
    assert site_6["performance_grade"] == "Poor"       # 100.0 >= 100
