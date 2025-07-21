from flask import Flask
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import ftplib
from datetime import datetime
import logging

app = Flask(__name__)

# ========================
# Настройки
# ========================

url = "http://pingwinclub.ru/"

ftp_host = os.getenv("FTP_HOST")
ftp_user = os.getenv("FTP_USER")
ftp_pass = os.getenv("FTP_PASS")
ftp_path = os.getenv("FTP_PATH")

# ========================
# Парсинг сайта
# ========================

response = requests.get(url)
html = response.text
soup = BeautifulSoup(html, "html.parser")

rows = soup.select("tr.stat")
data = []

for row in rows:
    cols = row.find_all("td")
    if len(cols) < 5:
        continue

    # Имя — cols[2]
    name_tag = cols[2].find(class_="statname")
    name = name_tag.get_text(strip=True) if name_tag else cols[2].get_text(strip=True)

    # Рейтинг — cols[3]
    rating = cols[3].get_text(strip=True)

    # Δ Рейтинг — cols[4]
    delta = cols[4].get_text(strip=True)

    # Последнее участие:
    last_participation = "-"

    # Ищем в cols[1] (внутри div.podrstat)
    stats_div = cols[1].find("div", class_="podrstat")
    if stats_div:
        stats_text = stats_div.get_text(strip=True)
        match = re.search(r"Дата последнего участия - ([\d\.]+)", stats_text)
        if match:
            last_participation = match.group(1)

    # Если не нашли в div — ищем в cols[5] или cols[6]
    if last_participation == "-":
        if len(cols) > 5:
            possible_date = cols[5].get_text(strip=True)
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                last_participation = possible_date
        if len(cols) > 6 and last_participation == "-":
            possible_date = cols[6].get_text(strip=True)
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                last_participation = possible_date

    # Город — cols[7], если есть
    city = cols[7].get_text(strip=True) if len(cols) > 7 else "-"

    data.append([len(data) + 1, name, rating, delta, last_participation, city])

# ========================
# Создаём DataFrame
# ========================

df = pd.DataFrame(data, columns=["Место", "Имя", "Рейтинг", "Δ Рейтинг", "Последнее участие", "Город"])

# ========================
# Определяем самую позднюю дату
# ========================

# Преобразуем даты в формат datetime, но сохраняем оригинальный формат
df['Последнее участие'] = df['Последнее участие'].str.replace(r'\s\d{2}:\d{2}:\d{2}', '', regex=True)

# Теперь парсим даты и находим последнюю
df['Дата для фильтра'] = pd.to_datetime(df['Последнее участие'], format='%d.%m.%Y', errors='coerce')
latest_date = df['Дата для фильтра'].max()
latest_date_str = latest_date.strftime("%d.%m.%Y") if pd.notna(latest_date) else "-"

# ========================
# Генерируем HTML
# ========================

html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Рейтинг PingWinClub</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 40px;
            background-color: #f9f9f9;
            color: #333;
        }}
        h1 {{
            text-align: center;
            color: #2c3e50;
            margin-bottom: 10px;
        }}
        h3 {{
            text-align: center;
            color: #777;
            margin-bottom: 30px;
        }}
        h4 {{
            margin-top: 40px;
            margin-bottom: 10px;
        }}
        .filters {{
            margin-bottom: 20px;
        }}
        .filter-btn {{
            display: inline-block;
            padding: 6px 10px;
            margin-right: 10px;
            background: #2ecc71;
            color: white;
            cursor: pointer;
            border-radius: 4px;
            text-decoration: none;
        }}
        .filter-btn:hover {{
            background: #27ae60;
        }}
        .alffilter {{
            display: inline-block;
            padding: 6px 10px;
            margin-right: 5px;
            background: #3498db;
            color: white;
            cursor: pointer;
            border-radius: 4px;
            text-decoration: none;
        }}
        .alffilter:hover {{
            background: #2980b9;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #3498db;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        tr:hover {{
            background-color: #e0e0e0;
        }}
    </style>
</head>
<body>
    <h1>Рейтинг PingWinClub</h1>
    <h3>Клубный рейтинг и игровая статистика</h3>

    <div class="filters">
        <h4>Алфавитный фильтр:</h4>
        <span class="alffilter" data-letter="все">все</span>
        {"".join(f'<span class="alffilter" data-letter="{chr(c)}">{chr(c)}</span>' for c in range(ord("А"), ord("Я")+1))}
        
        <h4 style="margin-top: 20px;">Фильтр по последнему турниру:</h4>
        <span class="filter-btn" data-date="{latest_date_str}">Последний турнир: {latest_date_str}</span>
    </div>

    <table id="myTable" border="1">
        <thead>
            <tr>
                <th>Место</th>
                <th>Имя</th>
                <th>Рейтинг</th>
                <th>Δ Рейтинг</th>
                <th>Последнее участие</th>
                <th>Город</th>
            </tr>
        </thead>
        <tbody>
"""

# Добавляем строки таблицы
for _, row in df.iterrows():
    html_content += f"""
    <tr>
        <td>{row['Место']}</td>
        <td>{row['Имя']}</td>
        <td>{row['Рейтинг']}</td>
        <td>{row['Δ Рейтинг']}</td>
        <td class="last-activity">{row['Последнее участие']}</td>
        <td>{row['Город']}</td>
    </tr>
    """

# Закрываем таблицу и добавляем скрипт фильтрации
html_content += """
        </tbody>
    </table>

    <script>
        document.addEventListener("DOMContentLoaded", function () {
            const alffilters = document.querySelectorAll(".alffilter");
            const datefilters = document.querySelectorAll(".filter-btn");
            const rows = document.querySelectorAll("#myTable tbody tr");

            // Фильтр по фамилии
            alffilters.forEach(filter => {
                filter.addEventListener("click", function () {
                    const letter = this.getAttribute("data-letter");

                    rows.forEach(row => {
                        const nameCell = row.querySelector("td:nth-child(2)");
                        if (!nameCell) return;

                        const surname = nameCell.textContent.trim().split(" ")[0];
                        const firstLetter = surname.charAt(0).toUpperCase();

                        if (letter === "все" || firstLetter === letter) {
                            row.style.display = "";
                        } else {
                            row.style.display = "none";
                        }
                    });
                });
            });

            // Фильтр по дате последнего турнира
            datefilters.forEach(filter => {
                filter.addEventListener("click", function () {
                    const targetDate = this.getAttribute("data-date");

                    rows.forEach(row => {
                        const dateCell = row.querySelector("td.last-activity");
                        if (!dateCell) return;

                        const rowDate = dateCell.textContent.trim();

                        if (targetDate === "all" || rowDate === targetDate) {
                            row.style.display = "";
                        } else {
                            row.style.display = "none";
                        }
                    });
                });
            });
        });
    </script>
</body>
</html>
"""

# ========================
# Сохраняем HTML локально
# ========================

local_html_path = "rating_full.html"
with open(local_html_path, "w", encoding="utf-8") as f:
    f.write(html_content)
print(f"✅ HTML создан локально: {local_html_path}")

# ========================
# Загружаем файл на FTP
# ========================

try:
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login(user=ftp_user, passwd=ftp_pass)
        print("✅ Успешно подключились к FTP")

        # Переходим в нужную папку
        ftp.cwd(ftp_path)

        # Отправляем файл
        with open(local_html_path, "rb") as file:
            ftp.storbinary(f"STOR {local_html_path}", file)

    print(f"✅ Файл загружен на FTP: ftp://{ftp_host}{ftp_path}{local_html_path}")

except Exception as e:
    print(f"❌ Ошибка FTP: {e}")
# ========================
# Flask маршруты
# ========================
@app.route("/")
def index():
    return "✅ Парсинг запущен. Откройте /run для запуска парсера."

@app.route("/run")
def run():
    log("🔔 Запрос на запуск парсинга")
    run_parser()
    return "✅ Парсинг выполнен"

# ========================
# Точка входа
# ========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
