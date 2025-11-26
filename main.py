import pandas as pd
from typing import Any


def reverse_string(s: str) -> str | None:
    """
    Reverses the input string s.

    Example:
    reverse_string("hello") -> "olleh"
    """
    # Your code here
    return None


def is_palindrome(s: str) -> bool | None:
    """
    Returns True if the input string s is a palindrome (ignores case), False otherwise.

    Example:
    is_palindrome("Madam") -> True
    is_palindrome("hello") -> False
    """
    # Your code here
    return None


def find_max(nums: list[int]) -> int | None:
    """
    Returns the maximum number in the list nums.
    Returns None if the list is empty.

    Example:
    find_max([1, 4, 2, 9]) -> 9
    """
    # Your code here
    return None


def group_words_by_length(words: list[str]) -> dict[int, list[str]] | None:
    """
    Groups words in a list by their length.

    Returns a dictionary where the key is the word length,
    and the value is a list of words of that length.

    Example:
    group_words_by_length(["hi", "hello", "to", "world"]) -> {2: ["hi", "to"], 5: ["hello", "world"]}
    """
    # Your code here
    return None


def two_sum(nums: list[int], target: int) -> list[int] | None:
    """
    Given a list of integers and a target number,
    return the indices of the two numbers that add up to the target.

    Assume exactly one solution exists.

    Example:
    two_sum([2, 7, 11, 15], 9) -> [0, 1]
    """
    # Your code here
    return None


def group_anagrams(words: list[str]) -> list[list[str]] | None:
    """
    Groups a list of words into anagram groups.

    Example:
    group_anagrams(["bat", "tab", "cat", "act", "tac", "dog"]) ->
    [["bat", "tab"], ["cat", "act", "tac"], ["dog"]]
    """
    # Your code here
    return None


def etl_sales_data(sales_data: list[dict[str, Any]], product_data: list[dict[str, Any]], region_data: list[dict[str, Any]]) -> pd.DataFrame | None:
    """
    ETL Challenge: Transform and analyze sales data using pandas.

    This function simulates a typical ETL pipeline for a sales analytics system.
    You need to extract data from multiple sources, transform it, clean it,
    join datasets, and produce an aggregated report.

    Args:
        sales_data: List of dictionaries containing sales transactions
            Format: [{"sale_id": int, "product_id": int, "region_id": int,
                     "quantity": int, "sale_date": str (YYYY-MM-DD), "amount": float}]

        product_data: List of dictionaries containing product information
            Format: [{"product_id": int, "product_name": str, "category": str, "unit_price": float}]

        region_data: List of dictionaries containing region information
            Format: [{"region_id": int, "region_name": str, "country": str}]

    Returns:
        A pandas DataFrame with the following specifications:
        - Columns: ["category", "country", "total_sales", "total_quantity", "avg_sale_amount"]
        - Grouped by: category and country
        - total_sales: Sum of all sale amounts for that category/country
        - total_quantity: Sum of all quantities sold for that category/country
        - avg_sale_amount: Average sale amount per transaction (rounded to 2 decimal places)
        - Sorted by: total_sales in descending order
        - Index: Reset (no multi-index)
        - Handle any missing/null values appropriately (drop rows with missing critical data)

    ETL Steps to implement:
    1. EXTRACT: Convert input lists to pandas DataFrames
    2. TRANSFORM:
       - Join sales with products on product_id
       - Join result with regions on region_id
       - Handle any missing values
    3. LOAD/AGGREGATE:
       - Group by category and country
       - Calculate required aggregations
       - Sort and format the output

    Example:
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

        Result DataFrame:
           category  country  total_sales  total_quantity  avg_sale_amount
        0  Electronics  USA       210.0           7             105.00
        1  Home         USA        90.0           3              90.00
    """
    # Your code here
    return None


def etl_network_performance(site_metrics: list[dict[str, Any]], site_info: list[dict[str, Any]]) -> pd.DataFrame | None:
    """
    ETL Challenge: Analyze telecom network performance data.

    This function processes time-series performance metrics from cell sites,
    performing data pivoting, time-based calculations, and quality categorization.
    Tests different ETL skills than etl_sales_data: pivoting wide-to-long format,
    datetime operations, conditional transformations, and handling metrics data.

    Args:
        site_metrics: List of dictionaries containing performance metrics over time
            Format: [{"site_id": str, "timestamp": str (YYYY-MM-DD HH:MM:SS),
                     "latency_ms": float, "packet_loss_pct": float,
                     "throughput_mbps": float, "active_connections": int}]

        site_info: List of dictionaries containing site metadata
            Format: [{"site_id": str, "site_type": str ("Macro", "Micro", "Pico", or "DAS"),
                     "region": str, "installation_date": str (YYYY-MM-DD)}]

    Returns:
        A pandas DataFrame with the following specifications:
        - Columns: ["site_id", "site_type", "region", "avg_latency_ms",
                   "avg_packet_loss_pct", "avg_throughput_mbps", "total_connections",
                   "measurement_count", "performance_grade"]
        - Grouped by: site_id (with site metadata joined)
        - avg_latency_ms: Average latency across all measurements (rounded to 2 decimals)
        - avg_packet_loss_pct: Average packet loss percentage (rounded to 2 decimals)
        - avg_throughput_mbps: Average throughput in Mbps (rounded to 2 decimals)
        - total_connections: Sum of all active connections
        - measurement_count: Count of measurements per site
        - performance_grade: Categorical grade based on avg_latency_ms:
            * "Excellent" if avg_latency < 20ms
            * "Good" if 20 <= avg_latency < 50ms
            * "Fair" if 50 <= avg_latency < 100ms
            * "Poor" if avg_latency >= 100ms
        - Sorted by: avg_latency_ms ascending (best performance first)
        - Handle missing site_info gracefully (exclude metrics for unknown sites)

    ETL Steps to implement:
    1. EXTRACT: Load both datasets as DataFrames
    2. TRANSFORM:
       - Join metrics with site info on site_id
       - Parse timestamp strings to datetime objects (if needed for validation)
       - Calculate aggregations per site
       - Create performance_grade using conditional logic
    3. LOAD:
       - Format numeric columns with proper rounding
       - Sort by performance (latency)
       - Reset index

    Example:
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

        Result DataFrame:
          site_id site_type region  avg_latency_ms  avg_packet_loss_pct  avg_throughput_mbps  total_connections  measurement_count performance_grade
        0 SITE-001     Macro  North           16.85                 0.15                94.00                255                  2         Excellent
        1 SITE-002     Micro  South           45.00                 1.50                78.50                 89                  1              Good
    """
    # Your code here
    return None

def main() -> None:
    None

if __name__ == '__main__':
    main()