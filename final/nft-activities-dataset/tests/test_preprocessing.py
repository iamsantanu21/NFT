import unittest
from src.data.preprocessing import clean_data, convert_timestamps, normalize_price_values

class TestPreprocessing(unittest.TestCase):

    def test_clean_data(self):
        # Add test cases for the clean_data function
        raw_data = [
            {'price': '0.5 ETH', 'timestamp': '2023-01-01T00:00:00Z'},
            {'price': None, 'timestamp': '2023-01-02T00:00:00Z'},
            {'price': '1.0 ETH', 'timestamp': 'invalid_timestamp'}
        ]
        cleaned_data = clean_data(raw_data)
        self.assertEqual(len(cleaned_data), 2)  # Expecting 2 valid entries

    def test_convert_timestamps(self):
        # Add test cases for the convert_timestamps function
        timestamps = ['2023-01-01T00:00:00Z', '2023-01-02T00:00:00Z']
        converted = convert_timestamps(timestamps)
        self.assertEqual(len(converted), 2)  # Expecting 2 converted timestamps

    def test_normalize_price_values(self):
        # Add test cases for the normalize_price_values function
        prices = ['0.5 ETH', '1.0 ETH', 'invalid_price']
        normalized = normalize_price_values(prices)
        self.assertEqual(len(normalized), 2)  # Expecting 2 valid normalized prices

if __name__ == '__main__':
    unittest.main()