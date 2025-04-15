# Vepi - Vena ETL Python Interface

A Python package for interacting with Vena's ETL API, providing a simple interface for data import and export operations.

## Installation

```bash
pip install vepi
```

## Configuration

Create a configuration file (e.g., `config.py`) with your Vena API credentials:

```python
HUB = 'eu1'  # e.g., us1, us2, ca3
API_USER = 'your_api_user'
API_KEY = 'your_api_key'
TEMPLATE_ID = 'your_template_id'
MODEL_ID = 'your_model_id'  # Optional, needed for exports
```

## Usage

### Basic Setup

```python
from vepi import VenaETL

# Initialize the client
vena_etl = VenaETL(
    hub=HUB,
    api_user=API_USER,
    api_key=API_KEY,
    template_id=TEMPLATE_ID,
    model_id=MODEL_ID
)
```

### Importing Data

#### Using DataFrame (start_with_data)

```python
import pandas as pd

# Create a DataFrame with your data
data = {
    'Value': ['1000', '2000'],
    'Account': ['3910', '3910'],
    'Entity': ['V001', 'V001'],
    'Department': ['D10', 'D10'],
    'Year': ['2020', '2020'],
    'Period': ['1', '2'],
    'Scenario': ['Actual', 'Actual'],
    'Currency': ['Local', 'Local'],
    'Measure': ['Value', 'Value']
}
df = pd.DataFrame(data)

# Import the data
vena_etl.start_with_data(df)
```

#### Using File (start_with_file)

You can upload data in three ways:

1. From a CSV file:
```python
# Upload from a CSV file
vena_etl.start_with_file("path/to/your/data.csv")
```

2. From a DataFrame:
```python
# Upload from a DataFrame
df = pd.DataFrame(data)
vena_etl.start_with_file(df)
```

3. From a file-like object:
```python
# Upload from a file-like object
with open("data.csv", "r") as f:
    vena_etl.start_with_file(f)
```

### Exporting Data

```python
# Export data with custom page size
exported_data = vena_etl.export_data(page_size=10000)
print(f"Exported {len(exported_data)} records")
```

### Getting Dimension Hierarchy

```python
# Get dimension hierarchy
hierarchy = vena_etl.get_dimension_hierarchy()
print("Dimension hierarchy:")
print(hierarchy)
```

## Error Handling

The package includes comprehensive error handling for:
- Invalid credentials
- Missing required fields
- API communication errors
- Data validation errors

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 