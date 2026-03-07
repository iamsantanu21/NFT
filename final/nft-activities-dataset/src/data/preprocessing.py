def clean_data(df):
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Fill missing values
    df = df.fillna(method='ffill')
    
    return df

def convert_timestamps(df, timestamp_column):
    # Convert timestamp column to datetime
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit='s')
    
    return df

def normalize_price(df, price_column):
    # Normalize price values to float
    df[price_column] = df[price_column].astype(float)
    
    return df