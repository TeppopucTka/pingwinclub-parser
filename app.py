from flask import Flask, request
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import ftplib
import logging
from datetime import datetime, timedelta

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

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
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

# ========================
# Парсинг сайта
# ========================
def parse_date(date_str):
    """Быстрый парсинг даты в формате dd.mm.yyyy или dd.mm.yy"""
    try:
        if len(date_str) == 10 and date_str[2] == '.' and date_str[5] == '.':
            day, month, year = date_str.split('.')
            if len(year) == 2:
                year = "20" + year
            return datetime(int(year), int(month), int(day))
    except:
        pass
    return None

def run_parser():
    log("🚀 Начинаем парсинг сайта")
    
    # --- Шаг 1: Загрузка HTML ---
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        html = response.text
        log("✅ HTML успешно загружен")
    except Exception as e:
        log(f"❌ Ошибка загрузки сайта: {e}")
        return

    # --- Шаг 2: Парсинг HTML ---
    try:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr.stat")
        log(f"🔍 Найдено строк: {len(rows)}")
        
        if len(rows) > 1000:  # Защита от аномальных данных
            log("⚠️ Слишком много строк — ограничение до 1000")
            rows = rows[:1000]
        
        data = []
        today = datetime.now()
        six_months_ago = today - timedelta(days=90)  # 3 месяца

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            name_tag = cols[2].find(class_="statname")
            name = name_tag.get_text(strip=True) if name_tag else cols[2].get_text(strip=True)
            rating = cols[3].get_text(strip=True)
            delta = cols[4].get_text(strip=True)

            last_participation = "-"
            # Поиск даты в подсказке
            stats_div = cols[1].find("div", class_="podrstat")
            if stats_div:
                stats_text = stats_div.get_text(strip=True)
                match = re.search(r"Дата последнего участия - ([\d\.]+)", stats_text)
                if match:
                    last_participation = match.group(1)

            # Проверка дополнительных колонок
            if last_participation == "-":
                for i in [5, 6]:
                    if len(cols) > i:
                        possible_date = cols[i].get_text(strip=True)
                        if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                            last_participation = possible_date
                            break

            city = cols[7].get_text(strip=True) if len(cols) > 7 else "-"

            # Парсим дату
            parsed_date = parse_date(last_participation)
            if parsed_date and parsed_date >= six_months_ago:
                data.append([len(data) + 1, name, rating, delta, last_participation, city])

        log(f"📊 Отфильтровано записей (последние 3 месяца): {len(data)}")
    except Exception as e:
        log(f"❌ Ошибка парсинга: {e}")
        return

    # --- Шаг 3: Создание DataFrame ---
    if not data:
        log("⚠️ Нет данных для отображения")
        return

    df = pd.DataFrame(data, columns=["Место", "Имя", "Рейтинг", "Δ Рейтинг", "Последнее участие", "Город"])
    df["Место"] = range(1, len(df) + 1)

    # --- Шаг 4: Определение последней даты ---
    latest_date_str = "-"
    try:
        latest_date = max(parse_date(date) for date in df["Последнее участие"] if parse_date(date))
        latest_date_str = latest_date.strftime("%d.%m.%Y")
    except:
        pass
    log(f"📅 Последняя дата турнира: {latest_date_str}")

    # --- Шаг 5: Генерация HTML ---
    try:
        used_letters = set()
        for name in df["Имя"]:
            surname = name.strip().split(" ")[0]
            if surname and surname[0].isalpha():
                used_letters.add(surname[0].upper())

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
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Рейтинг PingWinClub</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f9f9f9;
                    color: #333;
                    line-height: 1.5;
                }}
                h1 {{
                    text-align: center;
                    color: #2c3e50;
                    margin-bottom: 10px;
                    font-size: 1.5em;
                }}
                h3 {{
                    text-align: center;
                    color: #777;
                    margin-bottom: 20px;
                    font-size: 1em;
                }}
                h4 {{
                    margin-top: 20px;
                    margin-bottom: 10px;
                    font-size: 1em;
                }}
                .filters {{
                    margin-bottom: 20px;
                }}
                .filter-row {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 5px;
                    margin-bottom: 10px;
                }}
                .filter-btn {{
                    padding: 6px 10px;
                    background: #2ecc71;
                    color: white;
                    cursor: pointer;
                    border-radius: 4px;
                    font-size: 0.9em;
                    flex: 1 1 auto;
                    text-align: center;
                    min-width: 40px;
                }}
                .filter-btn:hover {{
                    background: #27ae60;
                }}
                .alffilter {{
                    padding: 6px 10px;
                    background: #3498db;
                    color: white;
                    cursor: pointer;
                    border-radius: 4px;
                    font-size: 0.9em;
                    flex: 1 1 auto;
                    text-align: center;
                    min-width: 30px;
                }}
                .alffilter:hover {{
                    background: #2980b9;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    background-color: white;
                    box-shadow: 0 0 10px rgba(0,0,0,0.1);
                    font-size: 0.9em;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: #3498db;
                    color: white;
                }}
                .centered {{
                    text-align: center;
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
            <h1><a href="https://пингвинклуб.рф/reitingi.html" target="_parent">Рейтинг PingWinClub</a></h1>
            <h3>Клубный рейтинг за прошедшие 3 месяца</h3>
            <div class="filters">
                <h4>Алфавитный фильтр:</h4>
                <div class="filter-row">
                    <span class="alffilter" data-letter="все">ВСЕ</span>
                    {"".join(f'<span class="alffilter" data-letter="{l}">{l}</span>' for l in first_row)}
                </div>
                <div class="filter-row">
                    {"".join(f'<span class="alffilter" data-letter="{l}">{l}</span>' for l in second_row)}
                </div>
                <h4 style="margin-top: 20px;">Фильтр по последнему турниру:</h4>
                <div class="filter-row">
                    <span class="filter-btn" data-date="{latest_date_str}">Последний турнир: {latest_date_str}</span>
                </div>
            </div>
            <table id="myTable" border="1">
                <thead>
                    <tr>
                        <th class="centered">№</th>
                        <th>Имя</th>
                        <th class="centered">Рейтинг</th>
                        <th class="centered">Δ</th>
                        <th>Последнее участие</th>
                        <th>Город</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in df.iterrows():
            rating_style = 'style="color: darkgreen; font-weight: bold;"' if '+' in row['Δ Рейтинг'] and row['Δ Рейтинг'] not in ["+0", "+-0"] else ''
            if '-' in row['Δ Рейтинг'] and row['Δ Рейтинг'] not in ["-0", "+-0"]:
                rating_style = 'style="color: red; font-weight: bold;"'
            html_content += f"""
            <tr>
                <td class="centered">{row['Место']}</td>
                <td>{row['Имя']}</td>
                <td class="centered" {rating_style}>{row['Рейтинг']}</td>
                <td class="centered">{row['Δ Рейтинг']}</td>
                <td class="last-activity">{row['Последнее участие']}</td>
                <td>{row['Город']}</td>
            </tr>
            """
        html_content += """
                </tbody>
            </table>
            <script>
                document.addEventListener("DOMContentLoaded", function () {
                    const alffilters = document.querySelectorAll(".alffilter");
                    const datefilters = document.querySelectorAll(".filter-btn");
                    const rows = document.querySelectorAll("#myTable tbody tr");
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

        local_html_path = "rating_full.html"
        with open(local_html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        log(f"✅ HTML создан: {local_html_path}")
    except Exception as e:
        log(f"❌ Ошибка генерации HTML: {e}")
        return

    # --- Шаг 6: Загрузка на FTP ---
    try:
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_pass)
            log("✅ Подключились к FTP")
            ftp.cwd(ftp_path)
            with open(local_html_path, "rb") as file:
                ftp.storbinary("STOR rating_full.html", file)
        log("✅ Файл загружен на FTP")
    except Exception as e:
        log(f"❌ Ошибка FTP: {e}")

    # --- Шаг 7: Очистка памяти ---
    del soup, html, data, df, html_content
    log("🧹 Память очищена")

# ========================
# Flask маршруты
# ========================
@app.route("/")
def index():
    return "✅ Парсинг запущен. Откройте /run для запуска."

@app.route("/run")
def run():
    log("🔔 Запрос на запуск парсера")
    run_parser()
    return "✅ Парсинг выполнен"

# ========================
# Точка входа
# ========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
