import requests
import time
import sys
import pandas as pd
import io
from typing import Optional, Union, List, Dict, Any, TextIO
import os
from datetime import datetime
from io import StringIO
import json

class VenaETL:
    def __init__(self, hub: str, api_user: str, api_key: str, template_id: str, model_id: Optional[str] = None):
        """
        Initialize the Vena ETL client.
        
        Args:
            hub (str): Data center hub (e.g., us1, us2, ca3)
            api_user (str): API user from Vena authentication token
            api_key (str): API key from Vena authentication token
            template_id (str): ETL template ID
            model_id (str, optional): Model ID for export operations
        """
        if not all([hub, api_user, api_key, template_id]):
            raise ValueError("hub, api_user, api_key, and template_id are required")
            
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

    def _validate_dataframe(self, df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> None:
        """
        Validate the DataFrame structure.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            required_columns (List[str], optional): List of required column names
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
            
        if df.empty:
            raise ValueError("DataFrame cannot be empty")
            
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"DataFrame is missing required columns: {missing_columns}")

    def _convert_dataframe_to_array(self, df: pd.DataFrame) -> List[List[Any]]:
        """
        Convert a pandas DataFrame to the required array format for Vena ETL.
        
        Args:
            df (pd.DataFrame): DataFrame to convert
            
        Returns:
            List[List[Any]]: Array of arrays representing the data
        """
        self._validate_dataframe(df)
        return df.values.tolist()

    def start_with_data(self, json_data: Union[pd.DataFrame, List[List[Any]]]) -> None:
        """
        Starts an ETL job with the provided JSON data and checks job status before completing.
        
        Args:
            json_data (Union[pd.DataFrame, List[List[Any]]]): Data to import, either as a DataFrame or array of arrays
        """
        if isinstance(json_data, pd.DataFrame):
            json_data = self._convert_dataframe_to_array(json_data)
            
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

        self._monitor_job_status(job_id)

    def _dataframe_to_csv_string(self, df: pd.DataFrame) -> str:
        """
        Convert a DataFrame to a CSV string in the format required by Vena.
        
        Args:
            df (pd.DataFrame): The DataFrame to convert
            
        Returns:
            str: CSV string in the required format
        """
        # Ensure all columns are strings
        df = df.astype(str)
        
        # Create CSV string
        output = StringIO()
        df.to_csv(output, index=False, header=True)
        return output.getvalue()

    def start_with_file(self, file: Union[str, pd.DataFrame, TextIO], filename: str = None) -> str:
        """
        Start an ETL job using a file or DataFrame.
        
        Args:
            file: Can be one of:
                - str: Path to a CSV file
                - pd.DataFrame: DataFrame to convert to CSV
                - TextIO: File-like object containing CSV data
            filename (str, optional): Name for the file in Vena. If not provided,
                will use the input filename or generate a default name.
            
        Returns:
            str: Job ID for monitoring
            
        Raises:
            ValueError: If file is invalid or empty
            requests.exceptions.RequestException: If API request fails
        """
        try:
            # Handle different input types
            if isinstance(file, str):
                # File path
                if not os.path.exists(file):
                    raise ValueError(f"File not found: {file}")
                with open(file, 'r') as f:
                    file_content = f.read()
                if not filename:
                    filename = os.path.basename(file)
            
            elif isinstance(file, pd.DataFrame):
                # DataFrame
                if file.empty:
                    raise ValueError("DataFrame is empty")
                file_content = self._dataframe_to_csv_string(file)
                if not filename:
                    filename = f"data_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            elif hasattr(file, 'read'):
                # File-like object
                file_content = file.read()
                if not filename:
                    filename = f"data_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            else:
                raise ValueError("Invalid file type. Must be file path, DataFrame, or file-like object")

            # Validate file content
            if not file_content.strip():
                raise ValueError("File is empty")

            # Prepare the request
            url = self.start_with_file_url
            
            # Create metadata JSON with required fields
            metadata = {
                "input": {
                    "partName": "file",
                    "fileFormat": "CSV",
                    "fileEncoding": "UTF-8",
                    "fileName": filename
                }
            }
            
            # Create multipart form data with proper encoding
            files = {
                'file': (  # This key must match the partName in metadata
                    filename,
                    file_content.encode('utf-8'),
                    'text/csv; charset=utf-8'
                ),
                'metadata': (
                    'metadata.json',
                    json.dumps(metadata).encode('utf-8'),
                    'application/json'
                )
            }
            
            # Make the request with proper authentication and headers
            response = requests.post(
                url,
                files=files,
                auth=(self.api_user, self.api_key),
                headers={
                    "accept": "application/json"
                }
            )
            
            # Check for error response
            if response.status_code >= 400:
                try:
                    error_data = response.json()
                    error_msg = f"Error details: {error_data}"
                except:
                    error_msg = f"Error response: {response.text}"
                raise requests.exceptions.RequestException(error_msg)
            
            response.raise_for_status()
            
            # Extract and return job ID
            job_id = response.json().get('id')
            if not job_id:
                raise ValueError("No job ID received from Vena API")
            
            print(f"ETL job started with ID: {job_id}")
            
            # Monitor the job status
            self._monitor_job_status(job_id)
            
            return job_id
            
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    error_msg = f"{error_msg}\nDetails: {error_detail}"
                except:
                    pass
            print(f"Error starting ETL job: {error_msg}")
            raise

    def _monitor_job_status(self, job_id: str) -> None:
        """
        Monitor the status of an ETL job.
        
        Args:
            job_id (str): ID of the job to monitor
        """
        check_status_url = f'{self.base_url}/etl/jobs/{job_id}/status'
        time.sleep(1)

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
                    print(f"Job {job_id} completed successfully.")
                    break
                elif job_status in ["ERROR", "CANCELLED"]:
                    # Get error details if available
                    error_url = f'{self.base_url}/etl/jobs/{job_id}'
                    error_response = requests.get(
                        url=error_url,
                        auth=(self.api_user, self.api_key),
                        headers=self.headers
                    )
                    
                    error_details = ""
                    if error_response.status_code == 200:
                        try:
                            error_data = error_response.json()
                            if 'error' in error_data:
                                error_details = f"\nError details: {error_data['error']}"
                            elif 'message' in error_data:
                                error_details = f"\nError message: {error_data['message']}"
                            elif isinstance(error_data, dict):
                                error_details = f"\nError response: {error_data}"
                        except:
                            error_details = f"\nError response: {error_response.text}"
                    
                    print(f"Job {job_id} ended with status: {job_status}{error_details}", file=sys.stderr)
                    raise Exception(f"Job failed with status: {job_status}{error_details}")
                else:
                    print(f"Job {job_id} status: {job_status}")
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        error_data = e.response.json()
                        error_msg = f"Error details: {error_data}"
                    except:
                        error_msg = f"Error response: {e.response.text}"
                print(f"Error checking job status: {error_msg}", file=sys.stderr)
                raise Exception(f"Failed to check job status: {error_msg}")

            time.sleep(3)

    def import_dataframe(self, df: pd.DataFrame) -> None:
        """
        Import data from a pandas DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the data to import
        """
        self._validate_dataframe(df)
        self.start_with_data(df)
        print("Data Import Script Finished")

    def export_data(self, page_size: int = 50000) -> Optional[pd.DataFrame]:
        """
        Export intersections data from the Vena model with pagination support.
        
        Args:
            page_size (int): Number of records to fetch per page (default: 50000)
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing all intersections data, or None if there was an error
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

    def get_dimension_hierarchy(self) -> pd.DataFrame:
        """
        Get the dimension hierarchies from the Vena model.
        
        Returns:
            pd.DataFrame: DataFrame containing the dimension hierarchies with columns:
                - dimension: The dimension name
                - name: The hierarchy member name
                - alias: The alias for the member (if any)
                - parent: The parent member name
                - operator: The operator for the member (+ or -)
        """
        if not self.model_id:
            raise ValueError("Model ID must be set to get dimension hierarchies")
            
        try:
            # Construct the URL for the hierarchy endpoint
            hierarchy_url = f'{self.base_url}/models/{self.model_id}/hierarchy'
            
            # Make the API request
            response = requests.get(
                hierarchy_url,
                auth=(self.api_user, self.api_key),
                headers=self.headers
            )
            response.raise_for_status()
            
            # Parse the response
            data = response.json()
            
            # Convert to DataFrame
            df = pd.DataFrame(data['data'])
            
            # Print summary information
            print(f"Retrieved {len(df)} dimension hierarchy members")
            print("\nUnique dimensions:")
            print(df['dimension'].unique())
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to get dimension hierarchies: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    print(f"Error details: {error_data}", file=sys.stderr)
                except:
                    print(f"Error response: {e.response.text}", file=sys.stderr)
            return None 