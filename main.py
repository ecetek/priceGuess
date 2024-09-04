from heapq import merge

import requests
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup


def fetch_secondCompany_data():
    myInput=input("URL:")
    url = myInput
    params = {'q': '', 'rows': 1000, 'start': 1, 'facet': 'datetime,area'}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        records = data.get('records', [])

        if records:
            df = pd.json_normalize(records)
            if 'fields.imbalanceprice' in df.columns and 'fields.datetime' in df.columns:
                df = df[['fields.imbalanceprice', 'fields.datetime']]
                df.columns = ['imbalance_price', 'datetime']
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.to_csv('x_company_imbalance_price.csv', index=False)
                print('Data saved to X company imbalance_price.csv')
                return df
            else:
                print('Required columns not found in fields.')
        else:
            print('No records found.')
    else:
        print('Failed to retrieve data:', response.status_code)


def fetch_firstCompany_data():
    myInput = input("URL:")
    url = myInput
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    imbalance_prices = []
    table = soup.find('table')

    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) > 1:
                timestamp = cols[0].get_text(strip=True)
                price = cols[1].get_text(strip=True)
                imbalance_prices.append({'Timestamp': timestamp, 'Imbalance Price': price})

        df = pd.DataFrame(imbalance_prices)
        df.to_csv('y_company_imbalance_prices.csv', index=False)
        print('Data saved to Y company imbalance_prices.csv')
        return df
    else:
        print('No table found in the HTML response.')


def fetch_weather_data():
    latitude = 52.52
    longitude = 13.41
    start_date = '2024-01-01'
    end_date = '2024-07-31'
    hourly = 'temperature_2m,relative_humidity_2m,wind_speed_10m'
    myInput = input("URL:")
    url = f'myInput={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={hourly}'
    response = requests.get(url)

    if response.status_code == 200:
        weather_data = response.json()
        weather_df = pd.DataFrame({
            'Timestamp': weather_data['hourly']['time'],
            'Temperature': weather_data['hourly']['temperature_2m'],
            'Humidity': weather_data['hourly']['relative_humidity_2m'],
            'Wind Speed': weather_data['hourly']['wind_speed_10m']
        })
        weather_df['Timestamp'] = pd.to_datetime(weather_df['Timestamp'])
        weather_df.set_index('Timestamp', inplace=True)
        weather_df.to_csv('weather_data.csv')
        print('Weather data saved to weather_data.csv')
        return weather_df
    else:
        print('Failed to retrieve weather data:', response.status_code)


def process_firstCompany_data(df):
    def convert_time_to_datetime(time_str, date_str='2024-01-01'):
        start_time, end_time = time_str.split(' - ')
        start_datetime = pd.to_datetime(date_str + ' ' + start_time, format='%Y-%m-%d %H:%M')
        end_datetime = pd.to_datetime(date_str + ' ' + end_time, format='%Y-%m-%d %H:%M')
        return start_datetime, end_datetime

    df[['DateTime Start', 'DateTime End']] = df['Timestamp'].apply(lambda x: convert_time_to_datetime(x)).apply(
        pd.Series)
    df['Imbalance Price'] = pd.to_numeric(df['Imbalance Price'].str.replace(',', '.'), errors='coerce')

    if df['Imbalance Price'].notna().any():
        df['Hour'] = df['DateTime Start'].dt.hour
        daily_avg = df.groupby('Hour')['Imbalance Price'].mean()

        plt.figure(figsize=(12, 6))
        plt.hist(df['Imbalance Price'].dropna(), bins=50, edgecolor='k', alpha=0.7)
        plt.title('Imbalance Price Distribution for Y company')
        plt.xlabel('Imbalance Price')
        plt.ylabel('Frequency')
        plt.grid(True)
        plt.show()

        plt.figure(figsize=(12, 6))
        plt.plot(daily_avg.index, daily_avg.values, marker='o')
        plt.title('Average Imbalance Price by Hour for Y company')
        plt.xlabel('Hour')
        plt.ylabel('Average Price')
        plt.xticks(range(24), [f'{i}:00' for i in range(24)], rotation=45)
        plt.grid(True)
        plt.show()

        low_price_freq = df[df['Imbalance Price'] < 0]['Imbalance Price'].count() / df['Imbalance Price'].count()
        print(f"Frequency of low or negative prices for Y company: {low_price_freq:.2%}")
    else:
        print("No valid 'Imbalance Price' data found.")


def merge_and_analyze_data(secondCompany_df, firstCompany_df, weather_df):
    if 'datetime' in secondCompany_df.columns:
        combined_df = secondCompany_df.join(weather_df, how='left')

        if 'DateTime Start' in firstCompany_df.columns:
            firstCompany_df.set_index('DateTime Start', inplace=True)
            combined_df = combined_df.join(firstCompany_df, how='left')

        print(combined_df.head())
        combined_df.to_csv('combined_data.csv')
        print('Data saved to combined_data.csv')

        plt.figure(figsize=(14, 7))
        plt.subplot(2, 1, 1)
        plt.hist(secondCompany_df['imbalance_price'].dropna(), bins=50, edgecolor='k', alpha=0.7)
        plt.title('Imbalance Price Distribution for X company')
        plt.xlabel('Imbalance Price')
        plt.ylabel('Frequency')
        plt.grid(True)

        plt.subplot(2, 1, 2)
        plt.plot(weather_df.index, weather_df['Temperature'], color='orange', label='Temperature')
        plt.title('Temperature Trend for X company')
        plt.xlabel('Time')
        plt.ylabel('Temperature (°C)')
        plt.grid(True)

        plt.tight_layout()
        plt.show()


    else:
        print("No 'datetime' column found in X company data for merging.")

