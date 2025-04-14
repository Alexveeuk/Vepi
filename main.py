import requests
import time
import sys
import pandas as pd
import io

class VenaETL:
    def __init__(self, hub, api_user, api_key, template_id, model_id=None):
        self.hub = hub
        self.api_user = api_user
        self.api_key = api_key
        self.template_id = template_id
        self.model_id = model_id
        
        # API URLs
        self.base_url = f'https://{hub}.vena.io/api/public/v1'
        self.start_with_data_url = f'{self.base_url}/etl/templates/{template_id}/startWithData'
        self.start_with_file_url = f'{self.base_url}/etl/templates/{template_id}/startWithFile'
        self.intersections_url = f'{self.base_url}/models/{model_id}/intersections' if model_id else None
        
        # Headers for requests
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        
        self.file_headers = {
            "accept": "application/json",
        }

    def _convert_dataframe_to_array(self, df):
        """Convert a pandas DataFrame to the required array format for Vena ETL."""
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        
        # Convert DataFrame to list of lists
        return df.values.tolist()

    def start_with_data(self, json_data):
        """Starts an ETL job with the provided JSON data and checks job status before completing."""
        try:
            body = {"input": {"data": json_data}}
            response = requests.post(
                self.start_with_data_url,
                json=body,
                auth=(self.api_user, self.api_key),
                headers=self.headers
            )
            response.raise_for_status()
            job_id = response.json()['id']
        except requests.exceptions.RequestException as e:
            print(f"Failed to start ETL job: {e}", file=sys.stderr)
            return

        check_status_url = f'{self.base_url}/etl/jobs/{job_id}/status'
        time.sleep(1)

        # Poll for job completion, error, or cancellation
        while True:
            try:
                status_response = requests.get(
                    url=check_status_url,
                    auth=(self.api_user, self.api_key),
                    headers=self.headers
                )
                status_response.raise_for_status()
                job_status = status_response.json()

                if job_status == "COMPLETED":
                    print(f"Job {job_id} completed.")
                    break
                elif job_status in ["ERROR", "CANCELLED"]:
                    print(f"Job {job_id} ended with status: {job_status}", file=sys.stderr)
                    break
                else:
                    print(f"Job {job_id} status: {job_status}")
            except requests.exceptions.RequestException as e:
                print(f"Error checking job status: {e}", file=sys.stderr)

            # Sleep between status checks (3 seconds, but can be adjusted based on the average runtime of the ETL Template)
            time.sleep(3)

    def start_with_file(self, df, filename):
        """
        Starts an ETL job with the provided DataFrame as a CSV file.
        
        Args:
            df (pd.DataFrame): The DataFrame to be converted to CSV and uploaded
            filename (str): The name of the file to be uploaded
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
            
        try:
            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Prepare the file for upload
            files = {
                'file': (filename, csv_data, 'text/csv')
            }
            
            # Make the request
            response = requests.post(
                self.start_with_file_url,
                files=files,
                auth=(self.api_user, self.api_key),
                headers=self.file_headers
            )
            response.raise_for_status()
            job_id = response.json()['id']
        except requests.exceptions.RequestException as e:
            print(f"Failed to start ETL job with file: {e}", file=sys.stderr)
            return

        check_status_url = f'{self.base_url}/etl/jobs/{job_id}/status'
        time.sleep(1)

        # Poll for job completion, error, or cancellation
        while True:
            try:
                status_response = requests.get(
                    url=check_status_url,
                    auth=(self.api_user, self.api_key),
                    headers=self.headers
                )
                status_response.raise_for_status()
                job_status = status_response.json()

                if job_status == "COMPLETED":
                    print(f"Job {job_id} completed.")
                    break
                elif job_status in ["ERROR", "CANCELLED"]:
                    print(f"Job {job_id} ended with status: {job_status}", file=sys.stderr)
                    break
                else:
                    print(f"Job {job_id} status: {job_status}")
            except requests.exceptions.RequestException as e:
                print(f"Error checking job status: {e}", file=sys.stderr)

            time.sleep(3)

    def import_dataframe(self, df):
        """
        Import data from a pandas DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the data to import
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
            
        # Convert DataFrame to array format
        data_array = self._convert_dataframe_to_array(df)
        
        # Process the entire array at once
        self.start_with_data(data_array)
            
        print("Data Import Script Finished")

    def export_data(self, page_size=50000):
        """
        Export intersections data from the Vena model with pagination support.
        
        Args:
            page_size (int): Number of records to fetch per page (default: 5000)
            
        Returns:
            pd.DataFrame: DataFrame containing all intersections data
        """
        if not self.model_id:
            raise ValueError("Model ID must be set to export data")
            
        try:
            all_data = []
            next_page_url = f"{self.intersections_url}?pageSize={page_size}"
            
            while next_page_url:
                # Make API request to get intersections data
                response = requests.get(
                    next_page_url,
                    auth=(self.api_user, self.api_key),
                    headers=self.headers
                )
                response.raise_for_status()
                
                # Load response into json
                data_response = response.json()
                
                # Skip the header row in data array and add the rest
                all_data.extend(data_response['data'][1:])  # Skip the first row which contains headers
                
                # Check if there's a next page
                next_page_url = data_response['metadata'].get('nextPage')
                
                # If there's a next page, use that URL directly
                if next_page_url:
                    print(f"Fetching next page... ({len(all_data)} records so far)")
            
            # Convert all data to DataFrame
            intersections_df = pd.DataFrame(all_data)
            
            # Set column names from metadata headers
            intersections_df.columns = data_response['metadata']['headers']
            
            print(f"Total records fetched: {len(intersections_df)}")
            return intersections_df
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to export data: {e}", file=sys.stderr)
            return None

# Example usage
if __name__ == "__main__":
    # Configuration
    HUB = 'HUB'  # Data center hub (e.g. us2, us3, ca3, etc.)
    API_USER = 'API_USER'  # API user from Vena authentication token
    API_KEY = 'API_KEY'  # API key from Vena authentication token
    TEMPLATE_ID = 'ETL_TEMPLATE_ID'  # ETL template ID
    MODEL_ID = 'MODEL_ID'  # Model ID for export

    # Create ETL instance
    vena_etl = VenaETL(HUB, API_USER, API_KEY, TEMPLATE_ID, MODEL_ID)

    # Example DataFrame for import
    df = pd.DataFrame({
        'Year': ['CurrentYear', 'CurrentYear'],
        'Version': ['V001', 'V002'],
        'Dimension': ['D10', 'D11'],
        'Member1': ['Placeholder 1 member', 'Placeholder 1 member'],
        'Member2': ['Placeholder 2 member', 'Placeholder 2 member'],
        'Member3': ['Placeholder 3 member', 'Placeholder 3 member'],
        'Member4': ['Placeholder 4 member', 'Placeholder 4 member'],
        'Period': ['2024', '2024'],
        'Month': ['1', '1'],
        'Scenario': ['Actual', 'Actual'],
        'Currency': ['Local', 'Local'],
        'ValueType': ['Value', 'Value'],
        'Amount': ['1000', '2000']
    })

    # Import data using file upload
    vena_etl.start_with_file(df, filename='my_data.csv')
    
    # Export data
    exported_data = vena_etl.export_data()
    if exported_data is not None:
        print('Intersections data:')
        print(exported_data)

    # Or specify a custom page size
    exported_data = vena_etl.export_data(page_size=10000)