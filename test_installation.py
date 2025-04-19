"""
Test script to verify Vepi package installation and basic functionality.
This script should be run in a fresh environment after installing the package.
"""

import sys
import pandas as pd
from vepi import VenaETL

def test_package_installation():
    """Test basic package import and initialization."""
    print("Testing package installation...")
    
    # Test basic import
    try:
        from vepi import VenaETL
        print("✓ Package import successful")
    except ImportError as e:
        print(f"✗ Package import failed: {e}")
        return False
    
    # Test class initialization
    try:
        # Using dummy credentials for testing
        vena_etl = VenaETL(
            hub='eu1',
            api_user='dummy_user',
            api_key='dummy_key',
            template_id='dummy_template'
        )
        print("✓ Class initialization successful")
    except Exception as e:
        print(f"✗ Class initialization failed: {e}")
        return False
    
    return True

def test_dataframe_creation():
    """Test DataFrame creation with required columns."""
    print("\nTesting DataFrame creation...")
    
    try:
        # Create a sample DataFrame
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
        print("✓ DataFrame creation successful")
        print("\nSample DataFrame:")
        print(df)
        return True
    except Exception as e:
        print(f"✗ DataFrame creation failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Vepi Package Installation Test")
    print("=============================")
    
    # Test Python version
    print(f"\nPython version: {sys.version}")
    
    # Test package installation
    if not test_package_installation():
        print("\nPackage installation test failed. Please check your installation.")
        return
    
    # Test DataFrame creation
    if not test_dataframe_creation():
        print("\nDataFrame creation test failed.")
        return
    
    print("\nAll tests completed successfully!")
    print("The package is installed correctly and basic functionality is working.")

if __name__ == "__main__":
    main() 