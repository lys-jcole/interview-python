import pandas as pd


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


def etl_sales_data(sales_data: list[dict[str, any]], product_data: list[dict[str, any]], region_data: list[dict[str, any]]) -> pd.DataFrame | None:
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

def main() -> None:
    None

if __name__ == '__main__':
    main()