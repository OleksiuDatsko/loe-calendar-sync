import re
import sys
import os
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from ics import Calendar, Event
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
from collections import defaultdict

from rich.console import Console
from rich.table import Table
from rich import box
from rich.text import Text
from rich.console import Group

load_dotenv()

# --- КОНФІГУРАЦІЯ ---
URL = os.getenv("LOE_URL", "https://poweron.loe.lviv.ua/")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "schedules")
STATE_FILE = os.path.join(OUTPUT_DIR, "schedule_state.json")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "history.json")
TZ = ZoneInfo(os.getenv("TIMEZONE", "UTC"))

env_groups = os.getenv("GROUPS", "1.1,1.2,2.1,2.2,3.1,3.2,4.1,4.2,5.1,5.2,6.1,6.2")
ALL_GROUPS = [g.strip() for g in env_groups.split(",") if g.strip()]

console = Console()
logging.basicConfig(
    level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()]
)


# --- РОБОТА З ФАЙЛАМИ ---
def load_json(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_json(filepath, data):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def update_history(date_str, group, seconds_off, intervals_list):
    history = load_json(HISTORY_FILE)

    if date_str not in history:
        history[date_str] = {}

    history[date_str][group] = {
        "total_seconds": seconds_off,
        "intervals": intervals_list,
    }

    save_json(HISTORY_FILE, history)


# --- ЛОГІКА ЧАСУ ---
def parse_time_aware(date_obj, time_str):
    if time_str == "24:00":
        dt_naive = datetime.combine(date_obj + timedelta(days=1), datetime.min.time())
    else:
        parsed_time = datetime.strptime(time_str, "%H:%M").time()
        dt_naive = datetime.combine(date_obj, parsed_time)
    return dt_naive.replace(tzinfo=TZ)


def format_timedelta_hours(td: timedelta):
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours}г {minutes:02d}хв"


def format_seconds_nice(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}г {minutes:02d}хв"


def get_intervals_signature(intervals):
    sigs = []
    for start, end in intervals:
        sigs.append(f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}")
    return "|".join(sigs)


# --- ВІЗУАЛІЗАЦІЯ ---
def create_visual_timeline_with_ruler(blackout_intervals):
    slots = [False] * 48
    for start, end in blackout_intervals:
        start_idx = start.hour * 2 + (1 if start.minute >= 30 else 0)
        # Обробка переходу через добу
        if end.date() > start.date() and (end.hour > 0 or end.minute > 0):
            end_idx = 48  # Заповнюємо до кінця поточної доби
        elif end.hour == 0 and end.minute == 0 and end.date() > start.date():
            end_idx = 48
        else:
            end_idx = end.hour * 2 + (1 if end.minute >= 30 else 0)

        for i in range(start_idx, min(end_idx, 48)):
            slots[i] = True

    ruler = Text()
    bar = Text()
    hours_labels = ["00", "04", "08", "12", "16", "20"]

    for i in range(0, 48, 8):
        if i > 0:
            ruler.append("│", style="dim white")
            bar.append("│", style="dim white")
        label_idx = i // 8
        label = hours_labels[label_idx] if label_idx < len(hours_labels) else "  "
        ruler.append(f"{label:<8}", style="dim white")

        chunk = slots[i : i + 8]
        for is_blackout in chunk:
            if is_blackout:
                bar.append("█", style="red")
            else:
                bar.append("█", style="green dim")
    ruler.append("│24", style="dim white")
    bar.append("│", style="dim white")
    return Group(ruler, bar)


# --- ОСНОВНА ЛОГІКА ---
def generate_events_for_group(group_id, target_date, text_content):
    """
    Повертає список подій (Event) та статистику для конкретної групи та дати.
    """
    events = []
    group_pattern = (
        rf"Група {re.escape(group_id)}\. Електроенергії немає (.*?)(?:\n|Група|$)"
    )
    group_match = re.search(group_pattern, text_content, re.DOTALL)

    blackout_intervals = []
    interval_strings = []
    total_blackout_duration = timedelta(0)

    if group_match:
        raw_intervals = group_match.group(1).strip()
        time_ranges = re.findall(r"з (\d{2}:\d{2}) до (\d{2}:\d{2})", raw_intervals)
        for start_str, end_str in time_ranges:
            interval_strings.append(f"{start_str}-{end_str}")
            start_dt = parse_time_aware(target_date, start_str)
            end_dt = parse_time_aware(target_date, end_str)
            if end_dt < start_dt:
                end_dt += timedelta(days=1)

            total_blackout_duration += end_dt - start_dt
            blackout_intervals.append((start_dt, end_dt))

            # Створення події ICS
            evt = Event(
                name="🌑 Нема світла",
                begin=start_dt,
                end=end_dt,
                description=f"Група {group_id}",
            )
            events.append(evt)

    blackout_intervals.sort(key=lambda x: x[0])

    # Додаємо події "Є світло" (локально, не для експорту в Google, якщо не треба)
    # Але для візуалізації і ics файлу ми зазвичай додаємо і позитивні події
    day_start = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=TZ)
    day_end = day_start + timedelta(days=1)
    current_time = day_start

    positive_events = []
    for b_start, b_end in blackout_intervals:
        if current_time < b_start:
            positive_events.append(
                Event(name="💡 Є світло", begin=current_time, end=b_start)
            )
        current_time = max(current_time, b_end)
    if current_time < day_end:
        positive_events.append(
            Event(name="💡 Є світло", begin=current_time, end=day_end)
        )

    events.extend(positive_events)

    visual_group = create_visual_timeline_with_ruler(blackout_intervals)
    current_signature = get_intervals_signature(blackout_intervals)
    percent_off = (total_blackout_duration.total_seconds() / 86400) * 100

    statistics = {
        "visual_group": visual_group,
        "intervals_display_str": (
            "\n".join(interval_strings) if interval_strings else "[green]Світло є[/]"
        ),
        "intervals_list": interval_strings,
        "intervals_signature": current_signature,
        "total_off": total_blackout_duration,
        "percent_off": percent_off,
    }
    return events, statistics


def print_historical_stats():
    history = load_json(HISTORY_FILE)
    if not history:
        return

    group_stats = defaultdict(list)
    total_days = len(history)

    for date_str, day_data in history.items():
        for group, data in day_data.items():
            if group not in ALL_GROUPS:
                continue

            seconds = 0
            if isinstance(data, (int, float)):
                seconds = data
            elif isinstance(data, dict):
                seconds = data.get("total_seconds", 0)

            group_stats[group].append(seconds)

    table = Table(
        title=f"📊 Зведена статистика (Днів в базі: {total_days})", box=box.DOUBLE_EDGE
    )
    table.add_column("Група", style="cyan bold", justify="center")
    table.add_column("Середній час", justify="right")
    table.add_column("Максимум", justify="right", style="red")
    table.add_column("Усього (сума)", justify="right", style="dim")

    for group in ALL_GROUPS:
        values = group_stats.get(group, [])
        if not values:
            continue

        avg_seconds = sum(values) / len(values)
        max_seconds = max(values)
        total_seconds = sum(values)

        table.add_row(
            group,
            format_seconds_nice(avg_seconds),
            format_seconds_nice(max_seconds),
            f"{total_seconds // 3600:.0f} год",
        )

    console.print("\n")
    console.print(table)


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    prev_state = load_json(STATE_FILE)
    # Підтримка старої структури для сумісності при першому запуску нової версії
    if "groups" in prev_state and not "dates" in prev_state:
        prev_state = {"dates": [], "groups": {}}

    with console.status("[bold green]Отримання даних...", spinner="dots"):
        page_text = ""
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(URL)
                page.wait_for_selector(
                    "text=Графік погодинних відключень", timeout=15000
                )
                page_text = page.inner_text("body")
                browser.close()
            except Exception as e:
                console.print(f"[bold red]Error:[/bold red] {e}")
                sys.exit(1)

    header_pattern = re.compile(
        r"Графік погодинних відключень на (\d{2}\.\d{2}\.\d{4})"
    )
    matches = list(header_pattern.finditer(page_text))
    if not matches:
        console.print("[bold red]Графіків не знайдено![/]")
        sys.exit(1)

    schedules = []
    for i, match in enumerate(matches):
        try:
            d = datetime.strptime(match.group(1), "%d.%m.%Y").date()
            end_idx = matches[i + 1].start() if i + 1 < len(matches) else len(page_text)
            schedules.append((d, page_text[match.end() : end_idx]))
        except ValueError:
            continue

    # Сортуємо від найстарішої до найновішої дати для послідовного виводу
    schedules.sort(key=lambda x: x[0])

    # Зберігаємо загальні календарі для кожної групи (акумулюємо всі дні)
    group_calendars = {g: Calendar() for g in ALL_GROUPS}

    # Новий стан: список дат і підписи для кожної групи на кожну дату
    new_state = {"dates": [], "groups": {g: {} for g in ALL_GROUPS}}

    # Знаходимо час оновлення (зазвичай він один на сторінці зверху, або біля кожного графіку)
    # Для простоти візьмемо з першого знайденого блоку або глобальний
    update_match = re.search(r"Інформація станом на\s+(.*?)(?:\n|$)", page_text)
    last_updated = update_match.group(1).strip() if update_match else "Невідомо"

    console.print(f"\n🕒 [dim]Оновлено на сайті: {last_updated}[/dim]\n")

    for target_date, target_text in schedules:
        target_date_str = target_date.strftime("%Y-%m-%d")
        new_state["dates"].append(target_date_str)

        console.print(
            f"📅 [bold cyan]Графік на: {target_date.strftime('%d.%m.%Y')}[/bold cyan]"
        )

        table = Table(
            title=f"Розклад ({target_date.strftime('%A')})",
            box=box.ROUNDED,
            pad_edge=False,
            show_lines=True,
        )

        table.add_column("Група", justify="center", style="cyan bold", no_wrap=True)
        table.add_column("Візуалізація", justify="left")
        table.add_column("Години", style="white")
        table.add_column("Вимкнення", justify="right")
        table.add_column("Статус", justify="center")

        for group in ALL_GROUPS:
            events, stats = generate_events_for_group(group, target_date, target_text)

            # Додаємо події до загального календаря групи
            for e in events:
                group_calendars[group].events.add(e)

            sig = stats["intervals_signature"]
            new_state["groups"][group][target_date_str] = sig

            update_history(
                target_date_str,
                group,
                stats["total_off"].total_seconds(),
                stats["intervals_list"],
            )

            # Порівняння змін з попереднім станом для цієї конкретної дати
            prev_sig = (
                prev_state.get("groups", {}).get(group, {}).get(target_date_str, "")
            )

            status_str = "[dim]Без змін[/dim]"
            if target_date_str not in prev_state.get("dates", []):
                status_str = "[bold blue]Новий день[/]"
            elif prev_sig != sig:
                status_str = "[bold red blink]⚠️ ЗМІНА![/]"

            pct = stats["percent_off"]
            color = "red" if pct > 50 else ("yellow" if pct > 30 else "green")
            stats_text = f"{format_timedelta_hours(stats['total_off'])}\n[{color}]{pct:.0f}% доби[/]"

            table.add_row(
                group,
                stats["visual_group"],
                stats["intervals_display_str"],
                stats_text,
                status_str,
            )

        console.print(table)
        console.print("")  # Відступ між таблицями

    # Збереження .ics файлів
    for group, cal in group_calendars.items():
        with open(
            os.path.join(OUTPUT_DIR, f"group_{group}.ics"), "w", encoding="utf-8"
        ) as f:
            f.writelines(cal.serialize_iter())

    save_json(STATE_FILE, new_state)

    print_historical_stats()
    console.print(f"\n[dim]Детальна історія зберігається в: {HISTORY_FILE}[/dim]")


if __name__ == "__main__":
    main()
