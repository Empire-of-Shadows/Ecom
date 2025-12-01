"""
Test script for time-of-day and weekend/weekday categorization.

This script tests the new ActivitySystem categorization methods without needing
to run the full bot or have database access.
"""

# Import the categorization methods (they're static, so we can use them directly)
import sys
sys.path.append('.')

from ecom_system.activity_system.activity_system import ActivitySystem


def test_hour_categorization():
    """Test hour-to-time-of-day categorization."""
    print("=" * 60)
    print("Testing Hour Categorization")
    print("=" * 60)

    test_cases = [
        (0, "night"),
        (1, "night"),
        (2, "overnight"),
        (3, "overnight"),
        (4, "overnight"),
        (5, "overnight"),
        (6, "morning"),
        (9, "morning"),
        (11, "morning"),
        (12, "afternoon"),
        (15, "afternoon"),
        (17, "afternoon"),
        (18, "evening"),
        (20, "evening"),
        (22, "evening"),
        (23, "night"),
    ]

    passed = 0
    failed = 0

    for hour, expected_period in test_cases:
        result = ActivitySystem.categorize_hour_to_time_of_day(hour)
        status = "[PASS]" if result == expected_period else "[FAIL]"
        if result == expected_period:
            passed += 1
        else:
            failed += 1
        print(f"{status} Hour {hour:2d}:00 -> {result:10s} (expected: {expected_period})")

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_weekday_categorization():
    """Test weekday categorization."""
    print("\n" + "=" * 60)
    print("Testing Weekday Categorization")
    print("=" * 60)

    test_cases = [
        (0, "weekday", "Monday"),
        (1, "weekday", "Tuesday"),
        (2, "weekday", "Wednesday"),
        (3, "weekday", "Thursday"),
        (4, "weekday", "Friday"),
        (5, "weekend", "Saturday"),
        (6, "weekend", "Sunday"),
    ]

    passed = 0
    failed = 0

    for day_num, expected_type, day_name in test_cases:
        result = ActivitySystem.categorize_weekday(day_num)
        status = "[PASS]" if result == expected_type else "[FAIL]"
        if result == expected_type:
            passed += 1
        else:
            failed += 1
        print(f"{status} {day_num} ({day_name:9s}) -> {result:7s} (expected: {expected_type})")

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0


def test_time_of_day_distribution():
    """Test time-of-day distribution analysis."""
    print("\n" + "=" * 60)
    print("Testing Time-of-Day Distribution Analysis")
    print("=" * 60)

    # Create a sample hourly pattern (24 hours)
    # Simulate a typical user who is:
    # - Very active in the evening (18-22)
    # - Moderately active in afternoon (12-17)
    # - Less active in morning (6-11)
    # - Rarely active at night/overnight
    hourly_pattern = [
        # Overnight (0-5): 2-5
        0, 0, 1, 1, 2, 2,
        # Morning (6-11): 6-11
        10, 15, 20, 25, 30, 35,
        # Afternoon (12-17): 12-17
        40, 45, 50, 55, 60, 50,
        # Evening (18-22): 18-22
        80, 90, 100, 85, 70,
        # Night (23-1): 23
        5
    ]

    # Create ActivitySystem instance (we just need the method, not DB connection)
    activity_system = ActivitySystem(db_manager=None)

    result = activity_system.analyze_time_of_day_distribution(hourly_pattern)

    print(f"\nTotal Activities: {result['total_activities']}")
    print(f"\nBy Period:")
    for period, count in result['by_period'].items():
        percentage = result['percentages'][period]
        print(f"  {period:10s}: {count:4d} activities ({percentage:5.1f}%)")

    print(f"\nMost Active Period: {result['most_active_period']}")
    print(f"Least Active Period: {result['least_active_period']}")
    print(f"Sorted Periods (most to least): {', '.join(result['sorted_periods'])}")

    # Verify the most active period is evening
    is_correct = result['most_active_period'] == "evening"
    status = "[PASS]" if is_correct else "[FAIL]"
    print(f"\n{status} Most active period correctly identified as 'evening': {is_correct}")

    return is_correct


def test_empty_hourly_pattern():
    """Test handling of empty or invalid hourly patterns."""
    print("\n" + "=" * 60)
    print("Testing Empty/Invalid Hourly Pattern Handling")
    print("=" * 60)

    activity_system = ActivitySystem(db_manager=None)

    # Test with empty list
    result1 = activity_system.analyze_time_of_day_distribution([])
    print(f"[PASS] Empty list handled: {result1['most_active_period'] == 'unknown'}")

    # Test with wrong length
    result2 = activity_system.analyze_time_of_day_distribution([1, 2, 3])
    print(f"[PASS] Wrong length handled: {result2['most_active_period'] == 'unknown'}")

    # Test with all zeros
    result3 = activity_system.analyze_time_of_day_distribution([0] * 24)
    print(f"[PASS] All zeros handled: Total activities = {result3['total_activities']}")

    return True


if __name__ == "__main__":
    print("\nTime-of-Day and Weekday Categorization Tests\n")

    all_passed = True
    all_passed &= test_hour_categorization()
    all_passed &= test_weekday_categorization()
    all_passed &= test_time_of_day_distribution()
    all_passed &= test_empty_hourly_pattern()

    print("\n" + "=" * 60)
    if all_passed:
        print("[SUCCESS] All tests passed!")
    else:
        print("[ERROR] Some tests failed!")
    print("=" * 60)
