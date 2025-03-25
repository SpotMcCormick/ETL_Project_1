#transform py
import pandas as pd
from extract import extract_ms_pop_data, extract_dsci_data, extract_weather_data
from io import StringIO
from datetime import timedelta

def transform_ms_pop_data():
    """
    Transform the extracted HTML table into a pandas DataFrame

    :return: DataFrame containing city population data
    """
    try:
        city = extract_ms_pop_data()
        if city:
            df = pd.read_html(str(city), header=[0])[0].reset_index(drop=True)
            df = df.iloc[1:]  # Remove the first row if it's not needed
            # There are funky characters in the city of Jackson so I'm going to have to replace the whole thing
            df['Name'] = df['Name'].apply(lambda x: 'Jackson' if 'Jackson' in x else x)
            # Removing more funky characters
            df['Name'] = df['Name'].str.replace('†', '', regex=False).str.replace(r'\[.*?\]', '', regex=True).str.replace('â€¡', '', regex=False).str.strip()
            # In the dataset on wikipedia they combo counties example is Jackson touches rankin, madison, and hinds county so I have to split the County column and explode it to create duplicate records
            df['County[1]'] = df['County[1]'].str.split(', ')
            df = df.explode('County[1]')
            #adding string "county" to look it up with the drought data
            df['County[1]'] = df['County[1]'] + " County"
            df = df[['Name', 'County[1]', 'Population (2020)[1]']]
            df.rename(columns={'Population (2020)[1]': 'Population_2020', 'Name': 'City', 'County[1]': 'County'}, inplace=True)
            #Changing Data Type to int to get the max population. Some Cities in this state touch multiple counties so a few cities will be duplicated, mainly jackson and that shouldnt change anything
            df['Population_2020'] = df['Population_2020'].astype(int)
            #Aggregating it to the get the biggest cities by county
            df1 = df.groupby('County')['Population_2020'].max()
            #Merging the cities together to see what the biggest cities are the biggest in the county
            df = pd.merge(df1, df, on=['County', 'Population_2020'], how='left')
            # Drop the record with the name 'Total'
            df = df[df['City'] != 'Total']
            print('Data transformed successfully!')
            return df
        else:
            print("No data to transform!")
            return None
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")

def transform_dsci_data():
    """
    transform the extracted DSCI data into a pandas DataFrame from a text csv. From there going to change data types, adding days because the drought monitor goes from tuesday to tuesday
    after that i will explode the dates because i am going to aggregate all the percipitaion by coutny by the day.
    :return:
    """
    try:
        data = extract_dsci_data()
        if data:
            df = pd.read_csv(StringIO(data))
            #changing data type
            df['MapDate'] = pd.to_datetime(df['MapDate'], format='%Y%m%d')
            #getting the end of the week for when drought monitor. It updates every tues
            df['EOW'] = df['MapDate'] + timedelta(days=6)
            #making a date range between starting and stopping
            df['Date'] = df.apply(lambda x: pd.date_range(start=x['MapDate'], end=x['EOW']), axis=1)
            #exploding that date rande
            df = df.explode('Date').reset_index(drop=True)
            #SOW = Start of week EOW = End of Week
            df.rename(columns={'MapDate': 'SOW'}, inplace=True)
            print('Data transformed successfully!')
            return df
        else:
            print("No data to transform!")
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")


def transform_weather_data():
    try:
        weather_data = extract_weather_data()
        if weather_data:
            records = []
            for city_data in weather_data:
                city_name = city_data.get('resolvedAddress', 'Unknown')
                for day in city_data.get('days', []):
                    day_record = {
                        'City': city_name,
                        'Date': day.get('datetime', None),
                        'Precipitation': day.get('precip', None),
                        'SoilTemp10': day.get('soiltemp10', None),
                        'SoilTemp20': day.get('soiltemp20', None),
                        'SoilMoisture10': day.get('soilmoisture10', None),
                        'SoilMoisture20': day.get('soilmoisture20', None),
                        'SoilMoistureVol10': day.get('soilmoisturevol10', None),
                        'SoilMoistureVol20': day.get('soilmoisturevol20', None)
                    }
                    records.append(day_record)
            df = pd.DataFrame(records)
            df['City'] = df['City'].str.replace(', MS, United States', '').str.strip()
            df['Date']= pd.to_datetime(df['Date'], format='%Y-%m-%d')
            print('Data transformed successfully!')
            return df
        else:
            print("No data to transform!")
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")

def join_data():
    try:
        df1 = transform_ms_pop_data()
        df2 = transform_dsci_data()
        df3 = transform_weather_data()
        df = pd.merge(df1, df2, on=['County'], how='inner')
        df = pd.merge(df, df3, on=['City', 'Date'], how='inner')
        # Group by County, City, Start of week and aggregates of sum of percipitation
        aggregated_df = df.groupby(['County', 'City', 'SOW', 'DSCI']).agg({
            'Precipitation': 'sum',
            'SoilTemp10': 'mean',
            'SoilTemp20': 'mean',
            'SoilMoisture10': 'mean',
            'SoilMoisture20': 'mean',
            'SoilMoistureVol10': 'mean',
            'SoilMoistureVol20': 'mean'
        }).reset_index()
        aggregated_df = aggregated_df.round(2)
        print('Data joined successfully!')
        return aggregated_df
    except ValueError as e:
        print(f"Value error: {e}")
    except Exception as e:
        print(f"An error occurred during transformation: {e}")





