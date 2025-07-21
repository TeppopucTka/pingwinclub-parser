import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import ftplib
import time
from datetime import datetime, timezone, timedelta

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
# Основная функция парсинга (остаётся без изменений)
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

    # Генерация HTML (остаётся без изменений)
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
            /* стили остаются без изменений */
        </style>
    </head>
    <body>
        <h1>Рейтинг PingWinClub</h1>
        <!-- HTML-контент остаётся без изменений -->
    </body>
    </html>
    """

    # Сохранение HTML
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
        log(f"✅ Файл загружен на FTP: https://raketka66.ru/rating_full.html  ")
    except Exception as e:
        log(f"❌ Ошибка FTP: {e}")

# ========================
# Функция ожидания до нужного времени
# ========================
def wait_until_next_run():
    tz = timezone(timedelta(hours=5))  # UTC+5 — Москва
    now = datetime.now(tz)
    current_time = now.strftime("%H:%M")

    if current_time < "18:00":
        next_run = datetime.combine(now.date(), datetime.strptime("18:00", "%H:%M").time(), tzinfo=tz)
    elif current_time < "22:30":
        next_run = datetime.combine(now.date(), datetime.strptime("22:30", "%H:%M").time(), tzinfo=tz)
    else:
        next_run = datetime.combine(now.date() + timedelta(days=1), datetime.strptime("18:00", "%H:%M").time(), tzinfo=tz)

    wait_seconds = (next_run - now).total_seconds()
    log(f"💤 Ожидаем до следующего запуска: {next_run.strftime('%H:%M')}")
    return wait_seconds

# ========================
# Основной цикл
# ========================
def main():
    log("🟢 Скрипт запущен")
    while True:
        run_parser()
        wait_time = wait_until_next_run()
        log(f"💤 Следующий запуск через {int(wait_time // 3600)} ч. {int((wait_time % 3600) // 60)} мин.")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()
