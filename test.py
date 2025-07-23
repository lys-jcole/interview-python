# test_functions_pytest.py
from main import reverse_string, is_palindrome, find_max, group_words_by_length, two_sum, group_anagrams


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
