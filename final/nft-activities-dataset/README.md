# NFT Activities Dataset

## Overview
The NFT Activities Dataset project is designed to manage and analyze activities related to Non-Fungible Tokens (NFTs). This dataset includes raw, processed, and external data sources, along with tools for data preprocessing, analysis, and visualization.

## Project Structure
- **data/**: Contains directories for raw, processed, and external data.
  - **raw/**: Raw data files (tracked by Git with .gitkeep).
  - **processed/**: Processed data files (tracked by Git with .gitkeep).
  - **external/**: External data sources (tracked by Git with .gitkeep).
  
- **src/**: Source code for data management and analysis.
  - **data/**: Contains scripts for managing NFT collections and preprocessing data.
    - **collection.py**: Class for managing NFT collections.
    - **preprocessing.py**: Functions for cleaning and transforming data.
  - **analysis/**: Contains scripts for analyzing NFT activities.
    - **analytics.py**: Functions for calculating sales and generating reports.
  - **utils/**: Utility functions for various tasks.
    - **helpers.py**: Functions for formatting and handling data.

- **notebooks/**: Jupyter notebooks for exploratory data analysis.
  - **exploratory_analysis.ipynb**: Notebook for visualizations and statistical summaries.

- **tests/**: Unit tests for ensuring code quality.
  - **test_preprocessing.py**: Tests for preprocessing functions.

- **.gitignore**: Specifies files and directories to be ignored by Git.

- **requirements.txt**: Lists Python dependencies required for the project.

## Setup Instructions
1. Clone the repository:
   ```
   git clone <repository-url>
   ```
2. Navigate to the project directory:
   ```
   cd nft-activities-dataset
   ```
3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage
- Use the `src/data/collection.py` to manage NFT collections and activities.
- Preprocess the data using functions in `src/data/preprocessing.py`.
- Analyze the activities with the tools in `src/analysis/analytics.py`.
- Explore the dataset visually in the Jupyter notebook located in `notebooks/exploratory_analysis.ipynb`.

## Contribution
Contributions are welcome! Please submit a pull request or open an issue for any suggestions or improvements.