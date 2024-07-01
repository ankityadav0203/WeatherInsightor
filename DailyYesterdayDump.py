from datetime import datetime, timedelta

def fetch_daily_weather_data():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    engine = create_engine('mysql+pymysql://username:password@hostname/dbname')
    
    all_data = []
    for city in cities:
        weather_data = fetch_weather_data(city, yesterday)
        if weather_data:
            weather_data['city'] = city
            weather_data['date'] = yesterday
            all_data.append(weather_data)
    
    df = pd.DataFrame(all_data)
    df.to_sql('daily_weather', con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    fetch_daily_weather_data()
