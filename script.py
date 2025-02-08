import datetime
import json
from typing import Optional
from fastapi import FastAPI, HTTPException
import aiosqlite
import httpx
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio

DATABASE_PATH = "weather.db"# Путь к базе данных
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"# URL API для прогноза
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"# URL API для нахождения координат города
UPDATE_INTERVAL = 900  # 15 минут

# Класс города
class City(BaseModel):
    city_name: str # Название города
    latitude: float # Ширина
    longitude: float # Долгота

class WeatherData(BaseModel):
    temperature: float = None # Температура
    windspeed: float = None # Скорость ветра
    pressure: float = None # Атмосферное давление

# Создание таблицы базы данных
async def create_db():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cities (
                city_name TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS forecasts (
                city_name TEXT,
                time TEXT,
                temperature REAL,
                windspeed REAL,
                pressure REAL,
                FOREIGN KEY (city_name) REFERENCES cities (city_name)
            )
        ''')
        await db.commit()
# Метод, определяющие действия при запусе скрипта
@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_db()
    asyncio.create_task(update_forecasts_loop())
    yield
app = FastAPI(lifespan=lifespan)
# Обновление у городов из БД данных о погоде
async def update_forecasts():
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Проверка, пуста ли таблица cities
        cursor = await db.execute("SELECT COUNT(*) FROM cities")
        count = await cursor.fetchone()
        # Если в таблице нет городов
        if count[0] == 0:
            print("No cities to update forecasts for")
            return

        # Получаем список городов
        cursor = await db.execute("SELECT city_name, latitude, longitude FROM cities")
        cities = await cursor.fetchall()

        for city in cities:
            city_name, latitude, longitude = city

            # Удаляем старые прогнозы для этого города
            await db.execute("DELETE FROM forecasts WHERE city_name = ?", (city_name,))
            # Получаем погоду на текущий момет
            weather_data = await get_current_weather(latitude,longitude)
            await db.execute(
                "INSERT INTO forecasts (city_name, time,temperature, windspeed, pressure) VALUES (?, ?, ?, ?, ?)",
                    (city_name, datetime.datetime.now().strftime("%H:%M"), weather_data.temperature, weather_data.windspeed, weather_data.pressure))
            await db.commit()
            print(f"Data for {city_name} updated")
# Обновление данных о погоде каждые 15 минут
async def update_forecasts_loop():
    while True:
        await update_forecasts()
        await asyncio.sleep(UPDATE_INTERVAL)
# Получение информации о погоде по широте и долготе
@app.get("/current_weather", response_model=WeatherData)
async def get_current_weather(latitude: float, longitude: float):
    
    try:    
        if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid coordinates. Latitude must be between -90 and 90, longitude between -180 and 180"
                )
        # Параметры для GET запроса
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,wind_speed_10m,pressure_msl",
            "timezone": "auto"
        }
        # Отправка GET запроса
        async with httpx.AsyncClient() as client:
            response = await client.get(OPEN_METEO_URL, params=params)
            response.raise_for_status()
            data = response.json()
        current_weather = data["current"]
        return WeatherData(
            temperature=current_weather["temperature_2m"],
            windspeed=current_weather["wind_speed_10m"],
            pressure=current_weather.get("pressure_msl")
        )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to fetch weather data: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}"
        )
# Добавление города в список отслеживаемых
@app.post("/add_city")
async def add_city(city: City):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute("INSERT INTO cities (city_name, latitude, longitude) VALUES (?, ?, ?)",
                             (city.city_name, city.latitude, city.longitude))
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="City already exists")
    return {"message": "City added successfully"}
# Получение списка отслеживаемых городов
@app.get("/tracked_cities")
async def get_tracked_cities():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT city_name FROM cities")
        cities = await cursor.fetchall()
    # Если таблица пустая
    if len(cities) < 1:
        raise HTTPException(status_code=404, detail="Table 'cities' is empty")
    return [city[0] for city in cities]

# Метод для получения координат города
async def get_city_coords(city_name: str):
    # Параметры для GET запроса
    params={
        "name":city_name,
        "count": 1,
        "language": "en",
        "format": "json"
    }
    # Получение координат по названию города
    async with httpx.AsyncClient() as client:
        response = await client.get(GEOCODING_URL, params=params)
        response.raise_for_status()
        data = response.json()
    if "results" not in data:
        raise HTTPException(status_code=400,detail="Invalid city name")
    else:
        coords = {
            "latitude": data["results"][0]["latitude"],
            "longitude": data["results"][0]["longitude"]
        }
   
    return coords
# Метод принимает название города и время и возвращает для него погоду на текущий день в указанное время
@app.get("/weather_forecast")
async def get_weather_forecast(city_name: str, time: str, parameters: Optional[str] = None):
    try:
        request_time = datetime.datetime.strptime(time, "%H:%M")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid time format. Use HH:MM")

    if(len(time)!=5):
        raise HTTPException(status_code=400, detail="Invalid time format. Use HH:MM")
    # Получение координат города
    coords = await get_city_coords(city_name)
    if not coords:
        raise HTTPException(status_code=404, detail="City not found")

    # Определение доступных параметров
    available_params = ["temperature", "humidity", "windspeed", "precipitation"]
    selected_params = available_params if not parameters else [p.strip() for p in parameters.split(",")]

    # Проверка валидности запрошенных параметров
    invalid_params = [p for p in selected_params if p not in available_params]
    if invalid_params:
        raise HTTPException(status_code=400, detail=f"Invalid parameters: {', '.join(invalid_params)}")

    # Формирование текущей даты и времени для запроса
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    search_time = f"{current_date}T{time}:00"

    # Формирование запроса к Open-Meteo API
    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "hourly": ",".join(
            ["temperature_2m" if p == "temperature" else 
             "relativehumidity_2m" if p == "humidity" else
             "windspeed_10m" if p == "windspeed" else
             "precipitation" for p in selected_params]
        ),
        "timezone": "Europe/Moscow",
        "start_date": current_date,
        "end_date": current_date,
        "timezone": "auto"
    }
    #Отправка запроса
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(OPEN_METEO_URL, params=params)
            response.raise_for_status()
            data = response.json()
            # Проверяем, содержит ли ответ данные о погоде
            if "hourly" not in data:
                raise HTTPException(
                    status_code=404,
                    detail="Weather data not available for the requested location or time"
                )
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch weather data: {str(e)}"
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=500,
                detail=f"Network error occurred: {str(e)}"
            )

    hourly_data = data["hourly"]
    # Округление времени до часа ровно
    if (int(time[3:5]) >= 30 and int(time[0:2]) != 23):
        time_index = int(time[0:2]) + 1
    else:
        time_index = int(time[0:2])

    # Формируем ответ
    weather_data = {}
    for param in selected_params:
        if param == "temperature":
            weather_data["temperature"] = hourly_data["temperature_2m"][time_index]
        elif param == "humidity":
            weather_data["humidity"] = hourly_data["relativehumidity_2m"][time_index]
        elif param == "windspeed":
            weather_data["windspeed"] = hourly_data["windspeed_10m"][time_index]
        elif param == "precipitation":
            weather_data["precipitation"] = hourly_data["precipitation"][time_index]

    if not weather_data:
        raise HTTPException(status_code=400, detail="No valid parameters selected")

    return weather_data
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000,lifespan="on")