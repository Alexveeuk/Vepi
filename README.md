# Vepi

A Python package for interacting with Vena's ETL and data export APIs.

## Installation

```bash
pip install vepi
```

## Usage

### Importing Data

```python
from vepi import VenaETL

# Initialize the client
vena_etl = VenaETL(
    hub='your_hub',  # e.g., us1, us2, ca3
    api_user='your_api_user',
    api_key='your_api_key',
    template_id='your_template_id',
    model_id='your_model_id'  # Optional, needed for exports
)

# Import data using file upload
df = pd.DataFrame({
    'Year': ['CurrentYear'],
    'Version': ['V001'],
    # ... other columns ...
})
vena_etl.start_with_file(df)

# Or import data using data upload
vena_etl.import_dataframe(df)
```

### Exporting Data

```python
# Export intersections data
exported_data = vena_etl.export_data(page_size=50000)  # Adjust page size as needed
```

## Features

- Import data using either file upload or direct data upload
- Export intersections data with pagination support
- Automatic job status monitoring
- Error handling and logging

## Requirements

- Python 3.7+
- requests
- pandas

## License

MIT 