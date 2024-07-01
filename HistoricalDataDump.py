import requests
import pandas as pd
from sqlalchemy import create_engine
import time

API_KEY = 'e53790cf314e3a3d61d2cb30c8d465d0'
BASE_URL = 'http://api.weatherstack.com/historical'

cities = ["Delhi", "Mumbai", "Bangalore", "Kolkata", "Chennai", "Hyderabad", "Pune"]
start_date = '2010-01-01'
end_date = 'yesterday_date'  # Replace with the actual date logic

def fetch_weather_data(city, date):
    params = {
        'access_key': API_KEY,
        'query': city,
        'historical_date': date,
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    if 'historical' in data:
        return data['historical'][date]
    return None

def main():
    engine = create_engine('mysql+pymysql://username:password@hostname/dbname')
    
    all_data = []
    for city in cities:
        current_date = pd.to_datetime(start_date)
        while current_date <= pd.to_datetime(end_date):
            date_str = current_date.strftime('%Y-%m-%d')
            weather_data = fetch_weather_data(city, date_str)
            if weather_data:
                weather_data['city'] = city
                weather_data['date'] = date_str
                all_data.append(weather_data)
            current_date += pd.Timedelta(days=1)
            time.sleep(1)  # To avoid hitting API rate limits
    
    df = pd.DataFrame(all_data)
    df.to_sql('historical_weather', con=engine, if_exists='replace', index=False)

if __name__ == "__main__":
    main()
