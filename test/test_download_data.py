import unittest
from unittest import mock
from download_data import download_interval_data, DATA_URL
from constants import RAW_DATA_DAY_ZIP_PATH

# Mock the requests library
@mock.patch('download_data.requests.get')
class DownloadIntervalDataTestCase(unittest.TestCase):

    # Test case when download is successful
    def test_download_interval_data_success(self, mock_get):
        # Mock the response to return status code 200
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b'mock file content'
        mock_get.return_value = mock_response

        # Call the function under test
        download_interval_data("20220101")

        # Assert that the file was saved
        expected_file_name = 'interval_data_20220101.zip'
        expected_file_location = f'{RAW_DATA_DAY_ZIP_PATH}/{expected_file_name}'
        mock_get.assert_called_once_with(f"{DATA_URL}20220101.zip")
        with open(expected_file_location, 'rb') as file:
            self.assertEqual(file.read(), b'mock file content')
        print(f"Expected file: {expected_file_name} successfully downloaded.")

    # Test case when download fails
    def test_download_interval_data_failure(self, mock_get):
        # Mock the response to raise an exception
        mock_get.side_effect = Exception("Mocked HTTP error")

        # Call the function under test
        download_interval_data("20220101")

        # Assert that the retry logic was followed
        self.assertEqual(mock_get.call_count, 4)
        print("Expected HTTP error occurred. Retry attempts completed.")


if __name__ == '__main__':
    unittest.main()

