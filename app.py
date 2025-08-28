# parser.py — Оптимизированная версия без Flask, с ручным запуском
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import ftplib
from datetime import datetime, timedelta
import logging

# ========================
# Настройки
# ========================
URL = "http://pingwinclub.ru/"
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_PATH = os.getenv("FTP_PATH")
LOCAL_HTML_PATH = "rating_full.html"

# ========================
# Логирование
# ========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.info

# ========================
# Парсинг сайта
# ========================
def run_parser():
    log("🚀 Начинаем парсинг сайта")
    
    # 1. Загружаем HTML
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        html = response.text
        log("✅ HTML успешно загружен")
    except Exception as e:
        log(f"❌ Ошибка загрузки сайта: {e}")
        return
    finally:
        del response  # Освобождаем память

    # 2. Парсим с помощью BeautifulSoup
    try:
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
            if last_participation == "-" and len(cols) > 5:
                possible_date = cols[5].get_text(strip=True)
                if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                    last_participation = possible_date
            if last_participation == "-" and len(cols) > 6:
                possible_date = cols[6].get_text(strip=True)
                if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", possible_date):
                    last_participation = possible_date
            city = cols[7].get_text(strip=True) if len(cols) > 7 else "-"
            data.append([len(data) + 1, name, rating, delta, last_participation, city])
        del soup, html  # Освобождаем память
    except Exception as e:
        log(f"❌ Ошибка парсинга: {e}")
        return

    # 3. Создаём DataFrame
    df = pd.DataFrame(data, columns=["Место", "Имя", "Рейтинг", "Δ Рейтинг", "Последнее участие", "Город"])
    del data  # Освобождаем список

    # 4. Фильтрация по дате (последние 3 месяца)
    try:
        df['Последнее участие'] = pd.to_datetime(
            df['Последнее участие'],
            format='%d.%m.%Y',
            errors='coerce',
            dayfirst=True
        )
        today = datetime.today()
        three_months_ago = today - timedelta(days=90)
        df_filtered = df[df['Последнее участие'].notna() & (df['Последнее участие'] >= three_months_ago)]
        df_filtered = df_filtered.sort_values(by='Последнее участие', ascending=False)
        df_filtered = df_filtered.reset_index(drop=True)
        df_filtered['Место'] = df_filtered.index + 1
        df_filtered['Последнее участие'] = df_filtered['Последнее участие'].dt.strftime('%d.%m.%Y')
        latest_date = df_filtered['Последнее участие'].max() if not df_filtered.empty else "-"
        log(f"📅 Последняя дата турнира (после фильтрации): {latest_date}")
    except Exception as e:
        log(f"❌ Ошибка обработки дат: {e}")
        return
    finally:
        del df  # Освобождаем исходный DataFrame

    # 5. Подготовка HTML
    try:
        used_letters = set()
        for _, row in df_filtered.iterrows():
            name = row['Имя']
            if name:
                surname = name.split(" ")[0]
                if surname and surname[0].isalpha():
                    used_letters.add(surname[0].upper())
        letters = [chr(c) for c in range(ord("А"), ord("Я") + 1)]
        filtered_letters = sorted([l for l in letters if l in used_letters])
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
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f9f9f9; color: #333; line-height: 1.5; }}
        h1 {{ text-align: center; color: #2c3e50; margin-bottom: 10px; font-size: 1.5em; }}
        h3 {{ text-align: center; color: #777; margin-bottom: 20px; font-size: 1em; }}
        h4 {{ margin-top: 20px; margin-bottom: 10px; font-size: 1em; }}
        .filters {{ margin-bottom: 20px; }}
        .filter-row {{ display: flex; flex-wrap: wrap; gap: 5px; margin-bottom: 10px; }}
        .filter-btn {{ padding: 6px 10px; background: #2ecc71; color: white; cursor: pointer; border-radius: 4px; font-size: 0.9em; flex: 1 1 auto; text-align: center; min-width: 40px; }}
        .filter-btn:hover {{ background: #27ae60; }}
        .alffilter {{ padding: 6px 10px; background: #3498db; color: white; cursor: pointer; border-radius: 4px; font-size: 0.9em; flex: 1 1 auto; text-align: center; min-width: 30px; }}
        .alffilter:hover {{ background: #2980b9; }}
        table {{ border-collapse: collapse; width: 100%; background-color: white; box-shadow: 0 0 10px rgba(0,0,0,0.1); font-size: 0.9em; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #3498db; color: white; }}
        .centered {{ text-align: center; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        tr:hover {{ background-color: #e0e0e0; }}
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
            <span class="filter-btn" data-date="{latest_date}">Последний турнир: {latest_date}</span>
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
        for _, row in df_filtered.iterrows():
            rating = row['Рейтинг']
            delta = row['Δ Рейтинг']
            rating_style = 'style="color: darkgreen; font-weight: bold;"' if '+' in delta and delta not in ["+0", "+-0"] else ''
            if '-' in delta and delta not in ["-0", "+-0"]:
                rating_style = 'style="color: red; font-weight: bold;"'
            html_content += f"""
            <tr>
                <td class="centered">{row['Место']}</td>
                <td>{row['Имя']}</td>
                <td class="centered" {rating_style}>{rating}</td>
                <td class="centered">{delta}</td>
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
                        const firstLetter = surname ? surname.charAt(0).toUpperCase() : '';
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
        with open(LOCAL_HTML_PATH, "w", encoding="utf-8") as f:
            f.write(html_content)
        log(f"✅ HTML успешно создан: {LOCAL_HTML_PATH}")
    except Exception as e:
        log(f"❌ Ошибка генерации HTML: {e}")
        return
    finally:
        del df_filtered  # Освобождаем DataFrame

    # 6. FTP загрузка
    log("📤 Начинаем загрузку на FTP")
    try:
        with ftplib.FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            log("✅ Подключились к FTP")
            ftp.cwd(FTP_PATH)
            log(f"📁 Перешли в папку: {FTP_PATH}")
            with open(LOCAL_HTML_PATH, "rb") as file:
                ftp.storbinary("STOR rating_full.html", file)
        log("✅ Файл успешно загружен на FTP")
    except Exception as e:
        log(f"❌ Ошибка FTP: {e}")
        return

# ========================
# Запуск
# ========================
if __name__ == "__main__":
    run_parser()
