#extract py
import requests
from bs4 import BeautifulSoup
import datetime as dt
from config import api_key

def extract_ms_pop_data():
    """
    Gets population data for MS on wikipedia

    :return: city populattion by county to get the data
    """

    url = 'https://en.wikipedia.org/wiki/List_of_municipalities_in_Mississippi'
    try:
        response = requests.get(url, verify=True)

        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        city_pop_data = soup.find('table')
        if city_pop_data:
            print('Data extracted')
            return city_pop_data
        else:
            print("Could not retrieve HTML data!")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def extract_dsci_data():
    """
    Extracting the DSCI drought data for mississippi

    :return:
    """
    today = dt.date.today().strftime('%m/%d/%Y')
    url = f'https://usdmdataservices.unl.edu/api/CountyStatistics/GetDSCI?&aoi=ms&startdate=6/1/2023&enddate={today}&statisticsType=2'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.text
            print('Data extracted')
            return data
        else:
            print("Could not get Drought Data")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

city_list= [
'Natchez,MS', 'Corinth,MS', 'Centreville,MS', 'Kosciusko,MS', 'Ashland,MS', 'Cleveland,MS', 'Bruce,MS', 'Vaiden,MS', 'Houston,MS', 'Ackerman,MS', 'PortGibson,MS', 'Quitman,MS', 'WestPoint,MS', 'Clarksdale,MS',
'CrystalSprings,MS', 'Collins,MS', 'Southaven,MS', 'Hattiesburg,MS', 'Bude,MS', 'Lucedale,MS', 'Leakesville,MS', 'Grenada,MS', 'Diamondhead,MS', 'Gulfport,MS', 'Jackson,MS',
'Durant,MS', 'Belzoni,MS', 'Mayersville,MS', 'Fulton,MS', 'Pascagoula,MS', 'BaySprings,MS', 'Fayette,MS', 'Prentiss,MS', 'Laurel,MS', 'DeKalb,MS', 'Oxford,MS', 'Meridian,MS', 'Monticello,MS', 'Carthage,MS', 'Tupelo,MS', 'Greenwood,MS', 'Brookhaven,MS', 'Columbus,MS', 'Columbia,MS', 'HollySprings,MS', 'Amory,MS', 'Winona,MS', 'Philadelphia,MS', 'Newton,MS', 'Macon,MS', 'Starkville,MS', 'Batesville,MS', 'Picayune,MS', 'Richton,MS', 'McComb,MS', 'Pontotoc,MS', 'Booneville,MS', 'Marks,MS', 'Forest,MS', 'RollingFork,MS', 'Magee,MS', 'Taylorsville,MS', 'Wiggins,MS', 'Indianola,MS', 'Tutwiler,MS', 'Senatobia,MS', 'Ripley,MS', 'Iuka,MS', 'Tunica,MS', 'NewAlbany,MS', 'Tylertown,MS', 'Vicksburg,MS', 'Greenville,MS', 'Waynesboro,MS', 'Eupora,MS', 'Louisville,MS', 'WaterValley,MS', 'YazooCity,MS'
]

def extract_weather_data():
    """
    Extract weather data for unique cities from the city list

    :return: Weather data for each city
    """

    weather_data = []
    for city in city_list:
        today = dt.date.today().strftime('%Y-%m-%d')
        url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/2023-06-01/{today}?unitGroup=us&elements=datetime,latitude,longitude,precip,stations,source,soiltemp10,soiltemp20,soilmoisture10,soilmoisture20,soilmoisturevol10,soilmoisturevol20&key={api_key}&contentType=json'
        try:
            response = requests.get(url, verify=True)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json() # Attempt to parse JSON
            print('Weather Data Extracted')
            weather_data.append(data)
        except requests.exceptions.RequestException as e:
            print(f"Request error for city {city}: {e}")
        except ValueError as e:
            print(f"JSON decode error for city {city}: {e}")
        except Exception as e:
            print(f"An error occurred for city {city}: {e}")
    return weather_data