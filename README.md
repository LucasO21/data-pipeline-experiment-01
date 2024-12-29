# 🌦️ Data Pipeline Example 01
## Minimalist Data Pipeline with OpenWeather API

Welcome to the **Minimalist Data Pipeline Project**, where simplicity meets functionality. This repository contains code for automating a data pipeline that fetches the current weather from the OpenWeather API and saves it to a local `data/` folder using Python and GitHub Actions. 🚀

__This project is part of a learning series where I am exploring how to automate ETL and ELT data pipelines.__

---

## 📖 Table of Contents

- [🌦️ Data Pipeline Example 01](#️-data-pipeline-example-01)
  - [Minimalist Data Pipeline with OpenWeather API](#minimalist-data-pipeline-with-openweather-api)
  - [📖 Table of Contents](#-table-of-contents)
  - [🌟 Project Overview](#-project-overview)
  - [🛠️ Features](#️-features)
  - [📁 Project Structure](#-project-structure)
  - [⚙️ How It Works](#️-how-it-works)
  - [🚀 Getting Started](#-getting-started)
  - [📹 Tutorial Reference](#-tutorial-reference)
  - [📜 License](#-license)

---

## 🌟 Project Overview

This project is a minimalist experiment to automate data pipelines using Python and GitHub Actions. The primary goal is to schedule a task that extracts current weather data from the [OpenWeather API](https://openweathermap.org/api) and saves it into a `data/` folder. The emphasis here is on scheduling automation rather than complex data processing.

---

## 🛠️ Features

- 🌐 **API Integration**: Fetch real-time weather data from the OpenWeather API.
- ⏰ **Automated Scheduling**: Uses GitHub Actions to schedule the pipeline.
- 📁 **Data Storage**: Saves weather data locally for later use.
- 🧪 **Minimalist Design**: A focused and lightweight approach to demonstrate scheduling with GitHub Actions.

---

## 📁 Project Structure

```
data-pipeline-example-01/
├── .github/workflows/
│   └── weather-data-pipeline.yml  # GitHub Actions workflow
├── data/                         # Folder to store weather data
├── src/
│   ├── pipelines/
│   │   └── open_weather_pipeline.py # OpenWeather API-specific pipeline
│   ├── utilities/
│       └── weather_api_functions.py # OpenWeather API helper functions
├── README.md                     # Project documentation
├── requirements.txt              # Python dependencies
├── setup.py                      # Package setup file
```

---

## ⚙️ How It Works

1. **Trigger**: GitHub Actions schedules the pipeline to run at predefined intervals.
2. **Fetch Data**: The `open_weather_pipeline.py` script queries the OpenWeather API for current weather data.
3. **Save Data**: The retrieved data is saved in the `data/` folder in JSON format.

---

## 🚀 Getting Started

Follow these steps to set up the project:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/LucasO21/data-pipeline-experiment-01
   cd minimalist-data-pipeline
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up OpenWeather API Key**:
   - Sign up at [OpenWeather](https://openweathermap.org/) and get an API key.
   - Create a `.env` file in the `src/` directory with the following content:
     ```env
     API_KEY=your_openweather_api_key
     ```

4. **Run the Script Locally**:
   ```bash
   python src/fetch_weather.py
   ```

5. **Check Data**:
   - The weather data will be saved in the `data/` folder.

---

## 📹 Tutorial Reference

This project was inspired by the tutorial: [Automate Python Scripts with GitHub Actions](https://www.youtube.com/watch?v=wJ794jLP2Tw).

---

## 📜 License

This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute as needed.

---

Happy automating! 🎉
