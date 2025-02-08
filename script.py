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
UPDATE_INTERVAL = 900  # 15 минут

# Класс города
class City(BaseModel):
    city_name: str # Название города
    latitude: float # Ширина
    longitude: float # Долгота

class WeatherData(BaseModel):
    temperature: float  # Температура
    windspeed: float  # Скорость ветра
    pressure: float  # Атмосферное давление
class User(BaseModel):
    username: str

# Создание таблицы базы данных
async def create_db():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Таблица пользователей
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL
            )
        ''')
        # Таблица городов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                city_name TEXT,
                latitude REAL,
                longitude REAL,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                UNIQUE(user_id, city_name)
            )
        ''')
        # Таблица прогнозов
        await db.execute('''
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                city_name TEXT,
                time TEXT,
                temperature REAL,
                windspeed REAL,
                pressure REAL,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (city_name) REFERENCES cities (city_name)
            )
        ''')
        await db.commit()
@asynccontextmanager
async def lifespan(app: FastAPI):
    # При запуске скрипта
    await create_db()
    asyncio.create_task(update_forecasts_loop())
    yield
    # После завершения работы
    # ....

app = FastAPI(lifespan=lifespan)

# Регистриация пользователя
@app.post("/register")
async def register_user(user: User):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            cursor = await db.execute(
                "INSERT INTO users (username) VALUES (?) RETURNING user_id",
                (user.username,)
            )
            user_id = await cursor.fetchone()
            await db.commit()
            return {"user_id": user_id[0], "username": user.username}
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="Username already exists")

# Обновление у городов из БД данных о погоде
async def update_forecasts():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Получаем всех пользователей
        cursor = await db.execute("SELECT user_id FROM users")
        users = await cursor.fetchall()
        
        if not users:
            print("No users found")
            return

        for user in users:
            user_id = user[0]
            
            # Получаем города пользователя
            cursor = await db.execute(
                "SELECT city_name, latitude, longitude FROM cities WHERE user_id = ?",
                (user_id,)
            )
            cities = await cursor.fetchall()

            if not cities:
                continue

            for city in cities:
                city_name, latitude, longitude = city

                try:
                    # Получаем новые данные о погоде
                    weather_data = await get_current_weather(latitude, longitude)
                    current_time = datetime.datetime.now().strftime("%H:%M")

                    # Проверяем, существует ли запись
                    cursor = await db.execute(
                        "SELECT COUNT(*) FROM forecasts WHERE user_id = ? AND city_name = ?",
                        (user_id, city_name)
                    )
                    count = await cursor.fetchone()

                    if count[0] > 0:
                        # Если запись существует, обновляем ее
                        await db.execute(
                            """
                            UPDATE forecasts 
                            SET time = ?, 
                                temperature = ?, 
                                windspeed = ?, 
                                pressure = ?
                            WHERE user_id = ? AND city_name = ?
                            """,
                            (current_time, 
                             weather_data.temperature, 
                             weather_data.windspeed, 
                             weather_data.pressure,
                             user_id, 
                             city_name)
                        )
                    else:
                        # Если записи нет, создаем новую
                        await db.execute(
                            """
                            INSERT INTO forecasts 
                            (user_id, city_name, time, temperature, windspeed, pressure) 
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (user_id, city_name, current_time,
                             weather_data.temperature, weather_data.windspeed, 
                             weather_data.pressure)
                        )

                    await db.commit()
                    print(f"Data for {city_name} (user {user_id}) updated")
                except Exception as e:
                    print(f"Failed to update weather for {city_name} (user {user_id}): {str(e)}")

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
async def add_city(city: City, user_id: int):
    # Проверка существования пользователя
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (int(user_id), ))
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail="User not found")

        try:
            await db.execute(
                "INSERT INTO cities (user_id, city_name, latitude, longitude) VALUES (?, ?, ?, ?)",
                (user_id, city.city_name, city.latitude, city.longitude)
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="City already exists for this user")
    return {"message": "City added successfully"}

# Получение списка отслеживаемых городов
@app.get("/tracked_cities")
async def get_tracked_cities(user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Проверка существования пользователя
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail="User not found")

        cursor = await db.execute(
            "SELECT city_name FROM cities WHERE user_id = ?",
            (user_id,)
        )
        cities = await cursor.fetchall()
    
    if len(cities) < 1:
        raise HTTPException(status_code=404, detail="No cities tracked for this user")
    return [city[0] for city in cities]


# Метод принимает название города и время и возвращает для него погоду на текущий день в указанное время
@app.get("/weather_forecast")
async def get_weather_forecast(user_id: int,city_name: str, time: str, parameters: Optional[str] = None):
    # Проверка существования пользователя
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail="User not found")

        # Проверка, отслеживает ли пользователь этот город
        cursor = await db.execute(
            "SELECT city_name FROM cities WHERE user_id = ? AND city_name = ?",
            (user_id, city_name)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail="City not tracked by this user")
        cursor = await db.execute(
            "SELECT latitude, longitude FROM cities WHERE user_id = ? AND city_name = ?",
            (user_id, city_name)
        )
        city_data = await cursor.fetchone()
    try:
        request_time = datetime.datetime.strptime(time, "%H:%M")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid time format. Use HH:MM")

    if(len(time)!=5):
        raise HTTPException(status_code=400, detail="Invalid time format. Use HH:MM")
    # Получение координат города
    coords = {
        "latitude": city_data[0],
        "longitude": city_data[1]
    }
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