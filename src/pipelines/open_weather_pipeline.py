from src.utilities.weather_api_functions import get_current_weather_data, get_save_weather_data, get_combine_weather_data
import time
import datetime

print("Starting Open Weather data pipeline at ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("----------------------------------------------")

# Step 1: Extract Current Weather Data from Open Weather API ----
t0 = time.time()
weather_df = get_current_weather_data(verbose = False)
t1 = time.time()
print("\nStep 1: Done")
print("---> Weather data extracted in", str(t1-t0), "seconds", "\n")

# Step 2: Save Weather Data to Data File ----
t0 = time.time()
get_save_weather_data(data = weather_df)
t1 = time.time()
print("\nStep 2: Done")
print("---> Weather data save in", str(t1-t0), "seconds", "\n")

# Step 3: Combine Weather Data ----
t0 = time.time()
get_combine_weather_data()
t1 = time.time()
print("\nStep 3: Done")
print("---> Weather combined and saved in", str(t1-t0), "seconds", "\n")