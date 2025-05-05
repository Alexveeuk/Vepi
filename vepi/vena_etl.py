"""
Vena ETL Client for Python

This module provides a client for interacting with Vena's ETL API.
It supports data import, export, and job management operations.

Example usage:
    >>> from vepi import VenaETL
    >>> client = VenaETL(
    ...     hub='us1',
    ...     api_user='your_api_user',
    ...     api_key='your_api_key',
    ...     template_id='your_template_id',
    ...     model_id='your_model_id'
    ... )
    >>> 
    >>> # Import data
    >>> df = pd.DataFrame(...)
    >>> client.import_dataframe(df)
    >>> 
    >>> # Export data
    >>> exported_data = client.export_data()
    >>> 
    >>> # Run a job
    >>> result = client.run_job()
    >>> 
    >>> # Cancel a job
    >>> client.cancel_job(job_id)
"""

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
    """
    Client for interacting with Vena's ETL API.
    
    This class provides methods for:
    - Data import and export
    - Job creation and management
    - Job status monitoring
    - Job cancellation
    
    Attributes:
        hub (str): Data center hub (e.g., us1, us2, ca3)
        api_user (str): API user from Vena authentication token
        api_key (str): API key from Vena authentication token
        template_id (str): ETL template ID
        model_id (str, optional): Model ID for export operations
    """
    
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
        self.closed_url = f'https://{hub}.vena.io/api/'
        self.start_with_data_url = f'{self.base_url}/etl/templates/{template_id}/startWithData'
        self.start_with_file_url = f'{self.base_url}/etl/templates/{template_id}/startWithFile'
        self.create_job_url = f'{self.base_url}/etl/templates/{template_id}/jobs'
        self.job_status_url = f'{self.base_url}/etl/jobs'  # Base URL for job operations
        self.intersections_url = f'{self.base_url}/models/{model_id}/intersections' if model_id else None
        self.models_url = f'{self.closed_url}/models'
        self.processes_url = f'{self.closed_url}/processes'
        
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

    def upload_job_data(self, job_id: str, data: Union[pd.DataFrame, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Upload data to a job before submission.
        
        Args:
            job_id (str): The ID of the job to upload data to
            data (Union[pd.DataFrame, List[Dict[str, Any]]]): The data to upload
            
        Returns:
            Dict[str, Any]: The upload response
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.job_status_url}/{job_id}/data"
        
        # Convert DataFrame to list of dictionaries if needed
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
            
        body = {
            "data": data
        }
        
        try:
            response = requests.post(
                url,
                headers=self.headers,
                auth=(self.api_user, self.api_key),
                json=body
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"Error uploading data: {e.response.text}")
                try:
                    error_details = e.response.json()
                    print(f"Error details: {error_details}")
                except:
                    pass
            raise

    def create_job(self) -> str:
        """
        Create a new ETL job in EDITING stage.
        
        Returns:
            str: The ID of the created job
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.base_url}/etl/templates/{self.template_id}/jobs"
        response = requests.post(
            url,
            headers=self.headers,
            auth=(self.api_user, self.api_key)
        )
        if response.status_code == 422:
            print(f"Error response content: {response.text}")
        response.raise_for_status()
        return response.json().get('id')

    def submit_job(self, job_id: str) -> Dict[str, Any]:
        """
        Submit a job for processing.
        
        Args:
            job_id (str): The ID of the job to submit
            
        Returns:
            Dict[str, Any]: The submission response
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.job_status_url}/{job_id}/submit"
        response = requests.post(
            url, 
            headers=self.headers, 
            auth=(self.api_user, self.api_key)
        )
        if response.status_code == 422:
            print(f"Error response content: {response.text}")
        response.raise_for_status()
        return response.json()

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the current status of a job.
        
        Args:
            job_id (str): The ID of the job to check
            
        Returns:
            Dict[str, Any]: The job status information
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.job_status_url}/{job_id}"
        response = requests.get(url, headers=self.headers, auth=(self.api_user, self.api_key))
        response.raise_for_status()
        return response.json()

    def wait_for_job_completion(self, job_id: str, poll_interval: int = 5, timeout: int = 3600) -> Dict[str, Any]:
        """
        Wait for a job to complete, polling its status at regular intervals.
        
        Args:
            job_id (str): The ID of the job to monitor
            poll_interval (int): How often to check the job status (in seconds)
            timeout (int): Maximum time to wait for completion (in seconds)
            
        Returns:
            Dict[str, Any]: The final job status
            
        Raises:
            TimeoutError: If the job doesn't complete within the timeout period
            requests.exceptions.RequestException: If the API request fails
        """
        start_time = time.time()
        while True:
            status = self.get_job_status(job_id)
            if status.get('status') in ['COMPLETED', 'FAILED']:
                return status
                
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")
                
            time.sleep(poll_interval)

    def process_data(self, data: Union[pd.DataFrame, List[Dict[str, Any]]], poll_interval: int = 5, timeout: int = 3600) -> Dict[str, Any]:
        """
        Process data through the ETL pipeline: start job, submit it, and wait for completion.
        
        Args:
            data (Union[pd.DataFrame, List[Dict[str, Any]]]): The data to process
            poll_interval (int): How often to check the job status (in seconds)
            timeout (int): Maximum time to wait for completion (in seconds)
            
        Returns:
            Dict[str, Any]: The final job status
            
        Raises:
            ValueError: If the data is invalid
            TimeoutError: If the job doesn't complete within the timeout period
            requests.exceptions.RequestException: If any API request fails
        """
        # Start the job
        job_info = self.start_with_data(data)
        job_id = job_info['id']
        
        # Submit the job
        self.submit_job(job_id)
        
        # Wait for completion
        return self.wait_for_job_completion(job_id, poll_interval, timeout)

    def run_job(self, poll_interval: int = 5, timeout: int = 3600) -> Dict[str, Any]:
        """
        Run a complete job workflow: create job, submit it, and wait for completion.
        
        Args:
            poll_interval (int): How often to check the job status (in seconds)
            timeout (int): Maximum time to wait for completion (in seconds)
            
        Returns:
            Dict[str, Any]: The final job status containing:
                - id: The job ID
                - name: The job name
                - modelId: The model ID
                - modelName: The model name
                - createdDate: Creation timestamp
                - updatedDate: Last update timestamp
                - userName: Name of the user who created the job
                - status: Final job status
                - error: Any error message (if applicable)
                - warnings: Any warnings (if applicable)
            
        Raises:
            requests.exceptions.RequestException: If any API request fails
            TimeoutError: If the job doesn't complete within the timeout period
        """
        print("Creating job...")
        job_id = self.create_job()
        
        if not job_id:
            raise ValueError("Failed to create job")
            
        print(f"Job created successfully with ID: {job_id}")
        
        # Get initial job status
        initial_status = self.get_job_status(job_id)
        print(f"Initial job status: {initial_status.get('status') if initial_status else 'Unknown'}")
        
        # Submit the job
        print("Submitting job...")
        submit_result = self.submit_job(job_id)
        print(f"Job submitted successfully: {submit_result}")
        
        # Wait for completion
        print("Waiting for job completion...")
        final_status = self.wait_for_job_completion(job_id, poll_interval, timeout)
        print(f"Job completed with status: {final_status.get('status')}")
        
        if final_status.get('error'):
            print(f"Error: {final_status.get('error')}")
        if final_status.get('warnings'):
            print(f"Warnings: {final_status.get('warnings')}")
            
        return final_status

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel a currently running ETL job.
        
        Args:
            job_id (str): The ID of the job to cancel
            
        Returns:
            Dict[str, Any]: The cancellation response containing:
                - id: The job ID
                - name: The job name
                - modelId: The model ID
                - modelName: The model name
                - createdDate: Creation timestamp
                - updatedDate: Last update timestamp
                - userName: Name of the user who created the job
                - status: Current job status (should be "CANCELLED" on success)
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        url = f"{self.job_status_url}/{job_id}/cancel"
        response = requests.post(
            url,
            headers=self.headers,
            auth=(self.api_user, self.api_key)
        )
        response.raise_for_status()
        return response.json() 
    
    def models(self) -> Dict[str, Any]:
        url = f"{self.models_url}"
        response = requests.get(url, headers=self.headers, auth=(self.api_user, self.api_key))
        response.raise_for_status()
        return response.json()

    def get_models(self):
        """
        Get a DataFrame of models with id, name, and description.
        
        Returns:
            pd.DataFrame: DataFrame containing model information with columns:
                - id: Model ID
                - name: Model name
                - desc: Model description
        """
        try:
            # Get models data
            models_data = self.models()
            
            # Extract only the required fields
            models_list = [{
                'id': model['id'],
                'name': model['name'],
                'desc': model['desc']
            } for model in models_data]
            
            # Convert to DataFrame
            return pd.DataFrame(models_list)
            
        except Exception as e:
            print(f"Error creating models DataFrame: {e}", file=sys.stderr)
            return None

    def processes(self):
        url = f"{self.processes_url}"
        response = requests.get(url, headers=self.headers, auth=(self.api_user, self.api_key))
        response.raise_for_status()
        return response.json()
    
    def get_processes(self):
        """
        Get a DataFrame of processes with their details.
        
        Returns:
            pd.DataFrame: DataFrame containing process information with columns:
                - id: Process ID
                - name: Process name
                - status: Process status
                - processFolderId: Parent process folder ID
                - allModels: List of associated models
        """
        try:
            # Get processes data
            processes_data = self.processes()
            
            if not processes_data:
                print("No processes data received")
                return None
                
            # Extract only the required fields with safe access
            processes_list = []
            for process in processes_data:
                try:
                    process_info = {
                        'id': process.get('id', ''),
                        'name': process.get('name', ''),
                        'status': process.get('status', ''),
                        'processFolderId': process.get('processFolderId', ''),
                        'allModels': process.get('allModels', [])
                    }
                    processes_list.append(process_info)
                except Exception as e:
                    print(f"Error processing process: {e}")
                    continue
            
            if not processes_list:
                print("No valid processes found in the data")
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(processes_list)
            print(f"Successfully created DataFrame with {len(df)} processes")
            return df
            
        except Exception as e:
            print(f"Error creating processes DataFrame: {e}", file=sys.stderr)
            return None

    def job_history(self, offset: int = 0) -> Dict[str, Any]:
        """
        Get job history from the ETL v2 API.
        
        Args:
            offset (int): Number of records to skip (default: 0)
            
        Returns:
            Dict[str, Any]: Job history data containing:
                - jobs: List of job records
                - total: Total number of jobs
                - offset: Current offset
                - limit: Number of records per page
        """
        try:
            # Construct the URL with fixed parameters and configurable offset
            # Using the closed API URL for v2 endpoints
            url = f"{self.closed_url}/etl/v2/jobs?offset={offset}&requested=100&orderBy=id&orderDirection=desc"
            
            # Make the API request
            response = requests.get(
                url,
                headers=self.headers,
                auth=(self.api_user, self.api_key)
            )
            response.raise_for_status()
            
            # Parse and return the response
            data = response.json()
            print(f"Retrieved {len(data.get('jobs', []))} jobs from history")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving job history: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    print(f"Error details: {error_data}", file=sys.stderr)
                except:
                    print(f"Error response: {e.response.text}", file=sys.stderr)
            return None


