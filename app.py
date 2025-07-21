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
# Логирование
# ========================
def log(message):
    print(message)

# ========================
# Парсинг сайта
# ========================
def run_parser():
    log("🚀 Начинаем парсинг сайта")
    try:
        response = requests.get(url)
        html = response.text
        log("✅ HTML успешно загружен")
    except Exception as e:
        log(f"❌ Ошибка загрузки сайта: {e}")
        return

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tr.stat")
    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        name_tag = cols[2].find(class_="statname")
        name = name_tag.get_text(strip=True) if name_tag else cols[2].get_text(strip=True)
        rating = cols[3].get_text(strip=True)
        delta = cols[4].get_text(strip=True)
        last_participation = "-"

        stats_div = cols[1].find("div", class_="podrstat")
        if stats_div:
            stats_text = stats_div.get_text(strip=True)
            match = re.search(r"Дата последнего участия - ([\d\.]+)", stats_text)
            if match:
                last_participation = match.group(1)

        if last_participation == "-":
            if len(cols) > 5:
                possible_date = cols[5].get_text(strip=True)
                if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                    last_participation = possible_date
            if len(cols) > 6 and last_participation == "-":
                possible_date = cols[6].get_text(strip=True)
                if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                    last_participation = possible_date

        city = cols[7].get_text(strip=True) if len(cols) > 7 else "-"
        data.append([len(data) + 1, name, rating, delta, last_participation, city])

    df = pd.DataFrame(data, columns=["Место", "Имя", "Рейтинг", "Δ Рейтинг", "Последнее участие", "Город"])

    df['Последнее участие'] = df['Последнее участие'].str.replace(r'\s\d{2}:\d{2}:\d{4}', '', regex=True)
    latest_date = df[df['Последнее участие'] != "-"]['Последнее участие']
    if not latest_date.empty:
        latest_date = pd.to_datetime(latest_date, format='%d.%m.%Y', errors='coerce').max()
        latest_date_str = latest_date.strftime("%d.%m.%Y") if not pd.isna(latest_date) else "-"
    else:
        latest_date_str = "-"

    log(f"📅 Последняя дата турнира: {latest_date_str}")

    # Генерация HTML
    used_letters = set()
    for _, row in df.iterrows():
        name = row['Имя']
        if name:
            surname = name.split(" ")[0]
            if surname:
                first_letter = surname[0].upper()
                if first_letter.isalpha():
                    used_letters.add(first_letter)

    letters = [chr(c) for c in range(ord("А"), ord("Я") + 1)]
    filtered_letters = [l for l in letters if l in used_letters]
    half = len(filtered_letters) // 2
    first_row = filtered_letters[:half]
    second_row = filtered_letters[half:]

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>Рейтинг PingWinClub</title>
        <style>
            body {{ font-family: Arial; margin: 20px; background: #f9f9f9; color: #333; }}
            h1 {{ text-align: center; }}
            table {{ border-collapse: collapse; width: 100%; background: white; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #3498db; color: white; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            tr:hover {{ background-color: #e0e0e0; }}
        </style>
    </head>
    <body>
        <h1>Рейтинг PingWinClub</h1>
        <table>
            <thead>
                <tr>
                    <th>№</th><th>Имя</th><th>Рейтинг</th><th>Δ</th><th>Последнее участие</th><th>Город</th>
                </tr>
            </thead>
            <tbody>
    """

    for _, row in df.iterrows():
        rating = row['Рейтинг']
        delta = row['Δ Рейтинг']
        rating_style = 'style="color: darkgreen;"' if '+' in delta and delta not in ["+0", "+-0"] else ''
        if '-' in delta and delta not in ["-0", "+-0"]:
            rating_style = 'style="color: red;"'
        html_content += f"""
        <tr>
            <td>{row['Место']}</td>
            <td>{row['Имя']}</td>
            <td {rating_style}>{rating}</td>
            <td>{delta}</td>
            <td>{row['Последнее участие']}</td>
            <td>{row['Город']}</td>
        </tr>
        """

    html_content += """
            </tbody>
        </table>
    </body>
    </html>
    """

    local_html_path = "rating_full.html"
    try:
        with open(local_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        log(f"✅ HTML создан локально: {local_html_path}")
    except Exception as e:
        log(f"❌ Ошибка записи локального HTML: {e}")
        return

    # Загрузка на FTP
    log("📤 Начинаем загрузку на FTP")
    try:
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_pass)
            log("✅ Успешно подключились к FTP")
            ftp.cwd(ftp_path)
            log(f"📁 Перешли в папку: {ftp_path}")
            with open(local_html_path, "rb") as file:
                ftp.storbinary(f"STOR rating_full.html", file)
        log(f"✅ Файл загружен на FTP")
    except Exception as e:
        log(f"❌ Ошибка FTP: {e}")

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
