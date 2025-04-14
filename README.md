# Vepi

A Python package for interacting with Vena's ETL and data export APIs.

note this is not an official Vena package this is a hobby project.

## Installation

```bash
pip install vepi
```

## Usage

### Importing Data

```python
from vepi import VenaETL
import pandas as pd

# Initialize the client
vena_etl = VenaETL(
    hub='your_hub',  # e.g., us1, us2, ca3
    api_user='your_api_user',
    api_key='your_api_key',
    template_id='your_template_id',
    model_id='your_model_id'  # Optional, needed for exports
)

# Import data using data upload
df = pd.DataFrame({
    'Value': ['1000'],
    'Account': ['3910'],
    'Entity': ['V001'],
    'Department': ['D10'],
    'Year': ['2020'],
    'Period': ['1'],
    'Scenario': ['Actual'],
    'Currency': ['Local'],
    'Measure': ['Value']
    # ... other required columns ...
})
vena_etl.import_dataframe(df)
```

### Exporting Data

```python
# Export intersections data
exported_data = vena_etl.export_data(page_size=50000)  # Adjust page size as needed
```

### Getting Dimension Hierarchies

```python
# Get dimension hierarchies
hierarchy_df = vena_etl.get_dimension_hierarchy()

# View hierarchy structure
for dim in hierarchy_df['dimension'].unique():
    print(f"\n{dim} hierarchy:")
    dim_df = hierarchy_df[hierarchy_df['dimension'] == dim]
    print(dim_df[['name', 'parent', 'operator']])
```

## Features

- Import data using direct data upload
- Export intersections data with pagination support
- Get dimension hierarchies and their structure
- Automatic job status monitoring
- Error handling and logging

## Requirements

- Python 3.7+
- requests>=2.31.0
- pandas>=2.0.0

## License

MIT 