import os
import json
import datetime
import yaml
from zoneinfo import ZoneInfo
from ics import Calendar
from dotenv import load_dotenv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

from rich.console import Console
from rich.panel import Panel

# Імпортуємо main, щоб оновити дані перед синхронізацією
from main import main

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/calendar"]
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "schedules")
SCHEDULE_STATE_FILE = os.path.join(OUTPUT_DIR, "schedule_state.json")
SYNC_STATE_FILE = os.path.join(OUTPUT_DIR, "sync_state.json")
CONFIG_FILE = "config.yaml"

TZ_STR = os.getenv("TIMEZONE", "UTC")
TZ = ZoneInfo(TZ_STR)

console = Console()


def load_config():
    """Завантажує налаштування з YAML файлу."""
    if not os.path.exists(CONFIG_FILE):
        console.print(f"[bold red]Помилка:[/] Файл '{CONFIG_FILE}' не знайдено!")
        exit(1)

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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
    except Exception as e:
        console.print(f"[red]Не вдалося зберегти стан: {e}[/]")


def authenticate_google():
    """Авторизація через OAuth 2.0."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                console.print("[bold red]Помилка credentials.json![/]")
                exit(1)
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def get_local_events(group_id):
    ics_path = os.path.join(OUTPUT_DIR, f"group_{group_id}.ics")
    if not os.path.exists(ics_path):
        return []
    with open(ics_path, "r", encoding="utf-8") as f:
        c = Calendar(f.read())
    return [e for e in c.events if "Нема світла" in e.name]


def batch_callback(request_id, response, exception):
    """Callback для обробки результатів пакетного запиту."""
    if exception:
        # Ігноруємо 404 (вже видалено) і 410 (gone)
        if isinstance(exception, HttpError) and exception.resp.status in [404, 410]:
            pass
        else:
            console.print(
                f"[red]Помилка в batch request ({request_id}): {exception}[/]"
            )


def clear_existing_blackouts_batch(service, calendar_id, target_date, group_id):
    """Видаляє старі події через Batch API (швидко)."""
    start_dt = datetime.datetime.combine(target_date, datetime.time.min).replace(
        tzinfo=TZ
    )
    end_dt = start_dt + datetime.timedelta(days=1)

    try:
        events_result = (
            service.events()
            .list(
                calendarId=calendar_id,
                timeMin=start_dt.isoformat(),
                timeMax=end_dt.isoformat(),
                singleEvents=True,
                orderBy="startTime",
                q="Нема світла",
            )
            .execute()
        )
    except HttpError as e:
        console.print(f"[red]Помилка доступу: {e}[/]")
        return

    events = events_result.get("items", [])
    events_to_delete = [
        e for e in events if f"(Гр. {group_id})" in e.get("summary", "")
    ]

    if not events_to_delete:
        return

    batch = service.new_batch_http_request(callback=batch_callback)
    count = 0
    for event in events_to_delete:
        batch.add(service.events().delete(calendarId=calendar_id, eventId=event["id"]))
        count += 1

    if count > 0:
        console.print(f"  [yellow]Batch: видалення {count} подій...[/]", end=" ")
        try:
            batch.execute()
            console.print("[green]OK[/]")
        except Exception as e:
            console.print(f"[red]Fail: {e}[/]")


def insert_events_batch(service, calendar_id, events, group_id):
    """Створює нові події через Batch API."""
    if not events:
        return

    batch = service.new_batch_http_request(callback=batch_callback)
    count = 0

    for event in events:
        event_body = {
            "summary": f"{event.name} (Гр. {group_id})",
            "description": event.description,
            "start": {
                "dateTime": event.begin.datetime.isoformat(),
                "timeZone": str(TZ),
            },
            "end": {
                "dateTime": event.end.datetime.isoformat(),
                "timeZone": str(TZ),
            },
            "reminders": {
                "useDefault": False,
                "overrides": [{"method": "popup", "minutes": 15}],
            },
            "colorId": "11",  # Червоний колір (Tomato)
        }
        batch.add(service.events().insert(calendarId=calendar_id, body=event_body))
        count += 1

    if count > 0:
        console.print(f"  [green]Batch: створення {count} подій...[/]", end=" ")
        try:
            batch.execute()
            console.print("[green]OK[/]")
        except Exception as e:
            console.print(f"[red]Fail: {e}[/]")


def process_group_date(
    service, group_id, calendar_id, date_str, current_signature, last_synced_data
):
    """Обробляє одну дату для однієї групи."""
    synced_sig = last_synced_data.get(date_str)

    # Якщо сигнатура (хеш графіку) не змінилась — пропускаємо
    if synced_sig == current_signature:
        return False

    console.print(f"[bold cyan]>> Синхронізація: Група {group_id} | {date_str}[/]")

    target_date = datetime.date.fromisoformat(date_str)
    local_events = get_local_events(group_id)

    # Фільтруємо події з ICS тільки за цю дату
    day_events = [e for e in local_events if e.begin.date() == target_date]

    # 1. Швидке видалення старих
    clear_existing_blackouts_batch(service, calendar_id, target_date, group_id)

    # 2. Швидке додавання нових
    if day_events:
        insert_events_batch(service, calendar_id, day_events, group_id)
    else:
        console.print(f"  [dim]Світло є, нових подій немає.[/dim]")

    last_synced_data[date_str] = current_signature
    return True


def sync_all():
    config = load_config()

    global TZ
    if "timezone" in config:
        TZ = ZoneInfo(config["timezone"])

    calendar_mapping = config.get("calendars", {})
    if not calendar_mapping:
        console.print("[bold red]Увага:[/] У config.yaml не знайдено 'calendars'.")
        return

    # Запускаємо парсинг (main.py)
    main()

    schedule_state = load_json(SCHEDULE_STATE_FILE)
    available_dates = schedule_state.get("dates", [])

    if not available_dates:
        console.print("[yellow]Немає даних про графік для синхронізації.[/]")
        return

    sync_state = load_json(SYNC_STATE_FILE)
    service = authenticate_google()

    console.print(
        Panel(
            f"Календарів: {len(calendar_mapping)} | Днів: {len(available_dates)}",
            title="Google Sync",
            border_style="green",
        )
    )

    any_changes = False

    with console.status("[bold green]Перевірка змін...[/]", spinner="dots"):
        for group_id, calendar_id in calendar_mapping.items():
            group_id = str(group_id)
            if group_id not in sync_state:
                sync_state[group_id] = {}

            group_signatures = schedule_state.get("groups", {}).get(group_id, {})

            for date_str in available_dates:
                signature = group_signatures.get(date_str, "")
                if process_group_date(
                    service,
                    group_id,
                    calendar_id,
                    date_str,
                    signature,
                    sync_state[group_id],
                ):
                    any_changes = True

    if any_changes:
        save_json(SYNC_STATE_FILE, sync_state)
        console.print("\n[bold green]✨ Синхронізацію завершено![/]")
    else:
        console.print("\n[bold green]✨ Все актуально. Змін не виявлено.[/]")


if __name__ == "__main__":
    sync_all()
